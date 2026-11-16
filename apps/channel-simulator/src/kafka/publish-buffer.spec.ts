import { describe, expect, it, vi } from 'vitest';
import {
  BufferClosedError,
  BufferFullError,
  PublishBuffer,
  type BufferDeps,
} from './publish-buffer';

const opts = { capacity: 5, maxBatch: 2, retry: { baseMs: 100, maxMs: 1_000 } };
const down = () => Object.assign(new Error('Connection error'), { name: 'KafkaJSConnectionError' });
const tick = () => new Promise((r) => setImmediate(r));

function make(over: Partial<BufferDeps<string, string>> = {}) {
  const sent: string[][] = [];
  const deps: BufferDeps<string, string> = {
    sendBatch: vi.fn(async (items: string[]) => {
      sent.push(items);
      return items.map((i) => `ok:${i}`);
    }),
    isRetriable: () => true,
    sleep: vi.fn(async () => {}),
    random: () => 1,
    ...over,
  };
  return { buf: new PublishBuffer(opts, deps), deps, sent };
}

describe('PublishBuffer', () => {
  it('доставляет FIFO батчами не больше maxBatch и резолвит каждый промис своим результатом', async () => {
    const { buf, sent } = make();
    const ps = ['a', 'b', 'c'].map((x) => buf.enqueue(x));
    expect(await Promise.all(ps)).toEqual(['ok:a', 'ok:b', 'ok:c']);
    expect(sent.flat()).toEqual(['a', 'b', 'c']);
    expect(sent.every((b) => b.length <= 2)).toBe(true);
  });

  it('Kafka недоступна → повторяет ТОТ ЖЕ батч (не добирая новые), потом доставляет; ничего не теряет', async () => {
    let fails = 3;
    const { buf, sent, deps } = make({
      sendBatch: vi.fn(async (items: string[]) => {
        sent.push([...items]);
        if (fails-- > 0) throw down();
        return items.map((i) => `ok:${i}`);
      }),
    });
    const sent0 = sent;
    const pa = buf.enqueue('a');
    await tick();
    const pb = buf.enqueue('b'); // пришёл, пока батч [a] в повторах
    expect(await Promise.all([pa, pb])).toEqual(['ok:a', 'ok:b']);
    // 4 попытки [a] (3 ошибки + успех), затем [b] отдельно
    expect(sent0).toEqual([['a'], ['a'], ['a'], ['a'], ['b']]);
    expect(deps.sleep).toHaveBeenCalledTimes(3);
    expect(buf.stats()).toMatchObject({ delivered: 2, retries: 3, size: 0, blockedSinceMs: null });
  });

  it('полный буфер → синхронный BufferFullError, счётчик отказов; освободилось место — снова принимает', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { buf } = make({
      sendBatch: async (items) => {
        await gate;
        return items;
      },
    });
    const ps = ['1', '2', '3', '4', '5'].map((x) => buf.enqueue(x));
    expect(() => buf.enqueue('6')).toThrow(BufferFullError);
    expect(buf.freeSlots()).toBe(0);
    expect(buf.stats().rejectedFull).toBe(1);
    release();
    await Promise.all(ps);
    expect(buf.freeSlots()).toBe(5);
    await expect(buf.enqueue('7')).resolves.toBe('7');
  });

  it('неретраемая ошибка → батч отвергнут, следующие идут дальше (буфер не встаёт навсегда)', async () => {
    const tooLarge = Object.assign(new Error('Message too large'), { retriable: false });
    const { buf } = make({
      sendBatch: async (items) => {
        if (items.includes('big')) throw tooLarge;
        return items;
      },
      isRetriable: (e) => (e as { retriable?: boolean }).retriable !== false,
    });
    // Воркер стартует синхронно на первом enqueue (без linger) → big уходит отдельным батчем.
    const big = buf.enqueue('big');
    const pb = buf.enqueue('b');
    const pc = buf.enqueue('c');
    await expect(big).rejects.toBe(tooLarge);
    await expect(pb).resolves.toBe('b');
    await expect(pc).resolves.toBe('c');
    expect(buf.stats().failed).toBe(1);
  });

  it('close: дошлёт, что успеет; остаток отвергает с BufferClosedError и возвращает число потерянных', async () => {
    let now = 0;
    const { buf } = make({
      now: () => now,
      sendBatch: async () => {
        throw down();
      },
      sleep: async (ms) => {
        now += ms;
        await tick();
      },
    });
    const ps = ['a', 'b', 'c'].map((x) => buf.enqueue(x).catch((e: unknown) => e));
    const res = await buf.close(1_000);
    expect(res).toEqual({ lost: 3 });
    for (const r of await Promise.all(ps)) expect(r).toBeInstanceOf(BufferClosedError);
    expect(() => buf.enqueue('d')).toThrow(BufferClosedError);
  });

  it('close при живой Kafka досылает всё и ничего не теряет', async () => {
    const { buf } = make();
    const ps = ['a', 'b', 'c'].map((x) => buf.enqueue(x));
    expect(await buf.close(1_000)).toEqual({ lost: 0 });
    expect(await Promise.all(ps)).toEqual(['ok:a', 'ok:b', 'ok:c']);
  });
});
