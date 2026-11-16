import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';
import { InteractionsRepository } from './interactions.repository';
import type { EndedRow, StartedRow } from '../kafka/message-handler';

/**
 * Интеграционный тест на НАСТОЯЩЕМ PostgreSQL: коммутативность и идемпотентность живут в SQL
 * (ON CONFLICT ... WHERE IS DISTINCT FROM), мок их не проверит.
 * Запуск: FORECAST_TEST_DATABASE_URL=postgres://wfm:wfm@localhost:5432/wfm yarn test
 * Каждый прогон — во временной схеме, которая удаляется в конце.
 */
const url = process.env.FORECAST_TEST_DATABASE_URL;
const schema = `test_${randomUUID().replaceAll('-', '').slice(0, 12)}`;

const migrationUp = () => {
  const sql = readFileSync(
    join(__dirname, '..', '..', 'migrations', '1762560000000_create-interactions.sql'),
    'utf8',
  );
  return sql.split('-- Down Migration')[0];
};

describe.skipIf(!url)('InteractionsRepository (PostgreSQL)', () => {
  let admin: Pool;
  let pool: Pool;
  let repo: InteractionsRepository;

  beforeAll(async () => {
    admin = new Pool({ connectionString: url });
    await admin.query(`CREATE SCHEMA ${schema}`);
    pool = new Pool({ connectionString: url, options: `-c search_path=${schema}` });
    await pool.query(migrationUp());
    repo = new InteractionsRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await admin?.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin?.end();
  });

  const started = (id: string): StartedRow => ({
    interactionId: id,
    channel: 'chat',
    queue: 'cards',
    customerSegment: 'retail',
    startedAt: new Date('2026-11-08T10:07:00Z'),
    eventId: randomUUID(),
  });
  const ended = (id: string): EndedRow => ({
    interactionId: id,
    endedAt: new Date('2026-11-08T10:09:30Z'),
    outcome: 'resolved',
    eventId: randomUUID(),
  });
  const businessFields = async (id: string) => {
    const { interaction_id: _id, ...rest } = (await repo.find(id))!;
    return rest;
  };

  it('коммутативность: started→ended и ended→started дают одинаковую строку', async () => {
    const a = randomUUID();
    const b = randomUUID();
    const sA = started(a),
      eA = ended(a);
    const sB = { ...started(b), eventId: sA.eventId },
      eB = { ...ended(b), eventId: eA.eventId };

    await repo.upsertStarted(sA);
    await repo.upsertEnded(eA);
    await repo.upsertEnded(eB); // обратный порядок
    expect(await repo.summary(900)).toMatchObject({ endedWithoutStarted: 1 }); // «сирота», пока нет started
    await repo.upsertStarted(sB);

    expect(await businessFields(b)).toEqual(await businessFields(a));
    expect(await repo.summary(900)).toMatchObject({ endedWithoutStarted: 0 });
  });

  it('идемпотентность: повтор того же события — applied=false, строка не меняется', async () => {
    const id = randomUUID();
    const s = started(id);
    expect(await repo.upsertStarted(s)).toEqual({ applied: true });
    const before = await businessFields(id);
    expect(await repo.upsertStarted(s)).toEqual({ applied: false });
    expect(await businessFields(id)).toEqual(before);
  });

  it('нагрузка по интервалам: date_bin режет по 15 минутам, handle time по завершённым', async () => {
    const id = randomUUID();
    await repo.upsertStarted({ ...started(id), queue: 'q-interval' });
    await repo.upsertEnded(ended(id));
    const rows = await repo.loadByInterval({
      from: new Date('2026-11-08T10:00:00Z'),
      to: new Date('2026-11-08T11:00:00Z'),
      queue: 'q-interval',
    });
    expect(rows).toEqual([
      { start: new Date('2026-11-08T10:00:00Z'), started: 1, ended: 1, avgHandleSec: 150 },
    ]);
  });

  it('open vs unclosed: без ended дольше окна — потерян (unclosed), моложе — ещё идёт (open)', async () => {
    const before = await repo.summary(900);
    await repo.upsertStarted({ ...started(randomUUID()), startedAt: new Date() }); // идёт сейчас
    await repo.upsertStarted({
      ...started(randomUUID()),
      startedAt: new Date(Date.now() - 3_600_000), // час без ended
    });
    const after = await repo.summary(900);
    expect(after.open - before.open).toBe(1);
    expect(after.unclosed - before.unclosed).toBe(1);
  });
});
