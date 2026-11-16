import { create } from 'zustand';
import type { Channel, WsEventMessage, WsServerMessage } from '@wfm/contracts';

export type LagSnapshot = Omit<Extract<WsServerMessage, { type: 'lag' }>, 'type'>;
import { RecentIds } from '../utils/dedupe';

export type ConnectionStatus = 'connecting' | 'open' | 'reconnecting';

export interface FeedItem {
  msg: WsEventMessage;
  /** occurredAt → получено браузером. Одна машина, поэтому разница часов не мешает. */
  latencyMs: number;
}

interface DashboardState {
  status: ConnectionStatus;
  retryInMs: number | null;
  consumerGroup: string | null;
  total: number;
  /** Отсечено повторов по eventId (at-least-once Kafka → gateway). */
  duplicates: number;
  byChannel: Record<Channel, number>;
  byPartition: Record<number, number>;
  byQueue: Record<string, number>;
  feed: FeedItem[];
  /** Lag consumer groups, измеренный gateway снаружи (admin API), раз в ~5 с. */
  lag: LagSnapshot | null;
  setStatus: (status: ConnectionStatus, retryInMs?: number | null) => void;
  setHello: (consumerGroup: string) => void;
  addEvent: (msg: WsEventMessage) => void;
  setLag: (lag: LagSnapshot) => void;
}

const FEED_SIZE = 20;
const DEDUPE_WINDOW = 10_000;
/** Вне стора: это служебное состояние, рендерить по нему нечего. */
const seen = new RecentIds(DEDUPE_WINDOW);
const inc = <K extends string | number>(m: Record<K, number>, k: K) => ({
  ...m,
  [k]: (m[k] ?? 0) + 1,
});

/**
 * Zustand: стор вне React, компоненты подписываются селекторами и перерисовываются,
 * только когда меняется выбранный кусок. Сокет пишет в стор через getState(), не через пропсы.
 * Дубли (at-least-once) отсекаем по eventId в окне последних DEDUPE_WINDOW событий.
 */
export const useDashboard = create<DashboardState>()((set) => ({
  status: 'connecting',
  retryInMs: null,
  consumerGroup: null,
  total: 0,
  duplicates: 0,
  byChannel: { chat: 0, voice: 0 },
  byPartition: {},
  byQueue: {},
  feed: [],
  lag: null,
  setStatus: (status, retryInMs = null) => set({ status, retryInMs }),
  setHello: (consumerGroup) => set({ consumerGroup }),
  setLag: (lag) => set({ lag }),
  addEvent: (msg) => {
    if (!seen.add(msg.event.eventId)) {
      set((s) => ({ duplicates: s.duplicates + 1 }));
      return;
    }
    set((s) => ({
      total: s.total + 1,
      byChannel: inc(s.byChannel, msg.event.payload.channel),
      byPartition: inc(s.byPartition, msg.meta.partition),
      byQueue: inc(s.byQueue, msg.event.payload.queue),
      feed: [{ msg, latencyMs: Date.now() - Date.parse(msg.event.occurredAt) }, ...s.feed].slice(
        0,
        FEED_SIZE,
      ),
    }));
  },
}));
