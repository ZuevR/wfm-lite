import { Inject, Injectable } from '@nestjs/common';
import type { Pool } from 'pg';
import { PG_POOL } from './database.service';
import type { IntervalLoad } from '../forecast/forecast';
import type { EndedRow, StartedRow } from '../kafka/message-handler';

/**
 * Upsert'ы по interaction_id — ядро нечувствительности к порядку и к дублям:
 *  - started и ended пишут НЕПЕРЕСЕКАЮЩИЕСЯ колонки → результат одинаков при любом порядке прихода;
 *  - WHERE ... IS DISTINCT FROM по eventId → повтор того же события не меняет строку (rowCount = 0),
 *    и мы видим дубль в логах. Это и есть идемпотентный консьюмер при at-least-once.
 */
const UPSERT_STARTED = `
  INSERT INTO interactions (interaction_id, channel, queue, customer_segment, started_at, started_event_id)
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (interaction_id) DO UPDATE SET
    channel = EXCLUDED.channel,
    queue = EXCLUDED.queue,
    customer_segment = EXCLUDED.customer_segment,
    started_at = EXCLUDED.started_at,
    started_event_id = EXCLUDED.started_event_id,
    updated_at = now()
  WHERE interactions.started_event_id IS DISTINCT FROM EXCLUDED.started_event_id`;

const UPSERT_ENDED = `
  INSERT INTO interactions (interaction_id, ended_at, outcome, ended_event_id)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (interaction_id) DO UPDATE SET
    ended_at = EXCLUDED.ended_at,
    outcome = EXCLUDED.outcome,
    ended_event_id = EXCLUDED.ended_event_id,
    updated_at = now()
  WHERE interactions.ended_event_id IS DISTINCT FROM EXCLUDED.ended_event_id`;

/**
 * Нагрузка по 15-минутным интервалам. date_bin (PG 14+) с началом отсчёта от эпохи совпадает
 * с intervalStart() в forecast.ts. ended/avg_handle — по обращениям, НАЧАВШИМСЯ в интервале.
 */
const LOAD_BY_INTERVAL = `
  SELECT date_bin('15 minutes', started_at, TIMESTAMPTZ 'epoch') AS start,
         count(*)::int AS started,
         count(ended_at)::int AS ended,
         avg(extract(epoch FROM ended_at - started_at)) FILTER (WHERE ended_at IS NOT NULL) AS avg_handle_sec
  FROM interactions
  WHERE started_at >= $1 AND started_at < $2
    AND ($3::text IS NULL OR queue = $3)
    AND ($4::text IS NULL OR channel = $4)
  GROUP BY 1
  ORDER BY 1`;

export interface InteractionRecord {
  interaction_id: string;
  channel: string | null;
  queue: string | null;
  customer_segment: string | null;
  started_at: Date | null;
  ended_at: Date | null;
  outcome: string | null;
  started_event_id: string | null;
  ended_event_id: string | null;
}

@Injectable()
export class InteractionsRepository {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  /** applied=false — событие уже было применено (дубль при at-least-once). */
  async upsertStarted(r: StartedRow): Promise<{ applied: boolean }> {
    const res = await this.pool.query(UPSERT_STARTED, [
      r.interactionId,
      r.channel,
      r.queue,
      r.customerSegment,
      r.startedAt,
      r.eventId,
    ]);
    return { applied: res.rowCount === 1 };
  }

  async upsertEnded(r: EndedRow): Promise<{ applied: boolean }> {
    const res = await this.pool.query(UPSERT_ENDED, [
      r.interactionId,
      r.endedAt,
      r.outcome,
      r.eventId,
    ]);
    return { applied: res.rowCount === 1 };
  }

  async find(interactionId: string): Promise<InteractionRecord | null> {
    const res = await this.pool.query<InteractionRecord>(
      `SELECT interaction_id, channel, queue, customer_segment, started_at, ended_at, outcome,
              started_event_id, ended_event_id
       FROM interactions WHERE interaction_id = $1`,
      [interactionId],
    );
    return res.rows[0] ?? null;
  }

  async loadByInterval(filter: {
    from: Date;
    to: Date;
    queue?: string;
    channel?: string;
  }): Promise<IntervalLoad[]> {
    const res = await this.pool.query<{
      start: Date;
      started: number;
      ended: number;
      avg_handle_sec: string | null;
    }>(LOAD_BY_INTERVAL, [filter.from, filter.to, filter.queue ?? null, filter.channel ?? null]);
    return res.rows.map((r) => ({
      start: r.start,
      started: r.started,
      ended: r.ended,
      // numeric из pg приходит строкой (чтобы не терять точность) — округляем сами.
      avgHandleSec: r.avg_handle_sec === null ? null : Math.round(Number(r.avg_handle_sec)),
    }));
  }

  /**
   * Сводка целостности:
   *  - open      — идут сейчас (нет ended, начались меньше maturitySec назад) — норма;
   *  - unclosed  — нет ended дольше maturitySec: ended потерян. Строку НЕ закрываем выдуманным
   *    ended_at (ADR 2026-11-12) — это статус при чтении, а придёт поздний ended — upsert его примет;
   *  - endedWithoutStarted — «сироты» (ended пришёл раньше started или started потерян).
   */
  async summary(maturitySec: number): Promise<{
    total: number;
    open: number;
    unclosed: number;
    endedWithoutStarted: number;
  }> {
    const res = await this.pool.query<{
      total: number;
      open: number;
      unclosed: number;
      orphan: number;
    }>(
      `SELECT count(*)::int AS total,
              count(*) FILTER (WHERE started_at IS NOT NULL AND ended_at IS NULL
                                 AND started_at >= now() - make_interval(secs => $1))::int AS open,
              count(*) FILTER (WHERE started_at IS NOT NULL AND ended_at IS NULL
                                 AND started_at < now() - make_interval(secs => $1))::int AS unclosed,
              count(*) FILTER (WHERE started_at IS NULL)::int AS orphan
       FROM interactions`,
      [maturitySec],
    );
    const r = res.rows[0];
    return { total: r.total, open: r.open, unclosed: r.unclosed, endedWithoutStarted: r.orphan };
  }
}
