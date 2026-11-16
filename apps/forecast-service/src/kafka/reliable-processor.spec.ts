import { describe, expect, it, vi } from 'vitest';
import { backoffMs, classifyError, errorMessage } from './error-policy';
import {
  deadLetterInvalid,
  processReliably,
  ProcessingAbortedError,
  type ProcessorDeps,
  type RetryPolicy,
} from './reliable-processor';

const pgError = (code: string, message = 'pg error') =>
  Object.assign(new Error(message), { name: 'error', code });
const netError = (code: string) => Object.assign(new Error(`connect ${code}`), { code });

describe('classifyError: причина в сообщении или в окружении', () => {
  it.each([
    ['23502 not null', pgError('23502'), 'message'],
    ['23505 unique', pgError('23505'), 'message'],
    ['22007 invalid datetime', pgError('22007'), 'message'],
    ['57P01 admin shutdown', pgError('57P01'), 'environment'],
    ['40P01 deadlock', pgError('40P01'), 'environment'],
    ['53300 too many connections', pgError('53300'), 'environment'],
    ['42P01 нет таблицы (миграция не накатана)', pgError('42P01'), 'environment'],
    ['ECONNREFUSED', netError('ECONNREFUSED'), 'environment'],
    ['pg: Connection terminated', new Error('Connection terminated unexpectedly'), 'environment'],
    [
      'pg pool: connect timeout',
      new Error('timeout exceeded when trying to connect'),
      'environment',
    ],
    [
      'AggregateError (::1 и 127.0.0.1)',
      new AggregateError([netError('ECONNREFUSED'), netError('ECONNREFUSED')], ''),
      'environment',
    ],
    [
      'KafkaJS',
      Object.assign(new Error('x'), { name: 'KafkaJSNumberOfRetriesExceeded' }),
      'environment',
    ],
    ['TypeError в нашем коде', new TypeError('cannot read x of undefined'), 'unknown'],
    ['не Error', 'boom', 'unknown'],
  ])('%s → %s', (_, err, expected) => {
    expect(classifyError(err)).toBe(expected);
  });
});

describe('backoffMs', () => {
  const opts = { baseMs: 100, maxMs: 1_000 };
  it('растёт экспоненциально и упирается в потолок; jitter в [cap/2, cap]', () => {
    expect(backoffMs(0, opts, () => 1)).toBe(100);
    expect(backoffMs(0, opts, () => 0)).toBe(50);
    expect(backoffMs(3, opts, () => 1)).toBe(800);
    expect(backoffMs(10, opts, () => 1)).toBe(1_000);
    expect(backoffMs(10, opts, () => 0)).toBe(500);
  });
});

const policy: RetryPolicy = {
  baseMs: 100,
  maxMs: 1_000,
  unknownMaxAttempts: 3,
  heartbeatEveryMs: 300,
};

function deps<T>(apply: () => Promise<T>, over: Partial<ProcessorDeps<T>> = {}) {
  const d = {
    apply: vi.fn(apply),
    deadLetter: vi.fn(async () => {}),
    heartbeat: vi.fn(async () => {}),
    sleep: vi.fn(async () => {}),
    isStopping: () => false,
    onRetry: vi.fn(),
    random: () => 1,
    ...over,
  };
  return d;
}

/** apply, который падает заданными ошибками, а потом успешен. */
const failingThen = <T>(errors: unknown[], value: T) => {
  let i = 0;
  return async () => {
    if (i < errors.length) throw errors[i++];
    return value;
  };
};

describe('processReliably', () => {
  it('успех с первой попытки — без ожидания и DLQ', async () => {
    const d = deps(async () => 'ok');
    expect(await processReliably(policy, d)).toEqual({ kind: 'applied', value: 'ok', attempts: 1 });
    expect(d.sleep).not.toHaveBeenCalled();
    expect(d.deadLetter).not.toHaveBeenCalled();
  });

  it('БД лежит (окружение) → ждём и повторяем, сколько нужно; в DLQ не отправляем', async () => {
    const down = Array.from({ length: 20 }, () => netError('ECONNREFUSED'));
    const d = deps(failingThen(down, 'ok'));
    const res = await processReliably(policy, d);
    expect(res).toEqual({ kind: 'applied', value: 'ok', attempts: 21 });
    expect(d.deadLetter).not.toHaveBeenCalled();
    expect(d.onRetry).toHaveBeenCalledTimes(20);
  });

  it('длинное ожидание режется на куски, между ними heartbeat (иначе выкинут из группы)', async () => {
    const sleep = vi.fn(async (_ms: number) => {});
    const d = deps(failingThen([netError('ECONNREFUSED')], 'ok'), { random: () => 1, sleep });
    await processReliably({ ...policy, baseMs: 1_000 }, d);
    // 1000 мс кусками по 300 → 300, 300, 300, 100
    expect(sleep.mock.calls.map(([ms]) => ms)).toEqual([300, 300, 300, 100]);
    expect(d.heartbeat).toHaveBeenCalledTimes(4);
  });

  it('ошибка данных (23505) → сразу DLQ «rejected», без повторов', async () => {
    const d = deps(failingThen([pgError('23505', 'duplicate key')], 'ok'));
    const res = await processReliably(policy, d);
    expect(res).toMatchObject({ kind: 'dead-lettered', reason: 'rejected', attempts: 1 });
    expect(d.deadLetter).toHaveBeenCalledWith('rejected', 'error: duplicate key', 1);
    expect(d.apply).toHaveBeenCalledTimes(1);
  });

  it('неизвестная ошибка → unknownMaxAttempts попыток, потом DLQ «unknown»', async () => {
    const d = deps(async () => {
      throw new TypeError('boom');
    });
    const res = await processReliably(policy, d);
    expect(res).toMatchObject({ kind: 'dead-lettered', reason: 'unknown', attempts: 3 });
    expect(d.apply).toHaveBeenCalledTimes(3);
  });

  it('ошибки окружения не расходуют лимит неизвестных', async () => {
    const errs = [
      new TypeError('a'),
      netError('ECONNREFUSED'),
      netError('ECONNREFUSED'),
      new TypeError('b'),
    ];
    const d = deps(failingThen(errs, 'ok'));
    expect(await processReliably(policy, d)).toMatchObject({ kind: 'applied', attempts: 5 });
  });

  it('Kafka недоступна при отправке в DLQ → повторяем отправку; не «теряем» сообщение', async () => {
    let dlqCalls = 0;
    const d = deps(failingThen([pgError('23502')], 'ok'), {
      deadLetter: vi.fn(async () => {
        if (++dlqCalls < 3) throw Object.assign(new Error('x'), { name: 'KafkaJSConnectionError' });
      }),
    });
    expect(await processReliably(policy, d)).toMatchObject({ kind: 'dead-lettered' });
    expect(d.deadLetter).toHaveBeenCalledTimes(3);
    expect(d.onRetry).toHaveBeenCalledWith(expect.objectContaining({ step: 'dead-letter' }));
  });

  it('остановка сервиса прерывает ожидание исключением (offset не коммитится → повторная доставка)', async () => {
    let stopping = false;
    const d = deps(
      async () => {
        throw netError('ECONNREFUSED');
      },
      {
        isStopping: () => stopping,
        sleep: vi.fn(async () => {
          stopping = true;
        }),
      },
    );
    await expect(processReliably(policy, d)).rejects.toBeInstanceOf(ProcessingAbortedError);
  });

  it('ошибка heartbeat (ребаланс) не глотается — уходит в KafkaJS', async () => {
    const rebalance = Object.assign(new Error('rebalancing'), { type: 'REBALANCE_IN_PROGRESS' });
    const d = deps(failingThen([netError('ECONNREFUSED')], 'ok'), {
      heartbeat: vi.fn(async () => {
        throw rebalance;
      }),
    });
    await expect(processReliably(policy, d)).rejects.toBe(rebalance);
  });

  it('невалидное сообщение → DLQ «invalid»', async () => {
    const d = deps(async () => 'never');
    expect(await deadLetterInvalid('invalid JSON', policy, d)).toMatchObject({
      kind: 'dead-lettered',
      reason: 'invalid',
    });
    expect(d.apply).not.toHaveBeenCalled();
    expect(d.deadLetter).toHaveBeenCalledWith('invalid', 'invalid JSON', 1);
  });
});

describe('errorMessage', () => {
  it('раскрывает пустой AggregateError (pg на localhost: ::1 и 127.0.0.1)', () => {
    const agg = new AggregateError([netError('ECONNREFUSED'), netError('ECONNREFUSED')], '');
    expect(errorMessage(agg)).toBe(
      'AggregateError: Error: connect ECONNREFUSED; Error: connect ECONNREFUSED',
    );
  });
});
