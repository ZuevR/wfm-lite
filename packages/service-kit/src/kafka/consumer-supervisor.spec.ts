import { describe, expect, it, vi } from 'vitest';
import { ConsumerSupervisor, type SupervisorDeps } from './consumer-supervisor';

const flush = () => new Promise((r) => setImmediate(r));

function make(over: Partial<SupervisorDeps> = {}) {
  const timers: { fn: () => void; ms: number }[] = [];
  const deps = {
    connect: vi.fn(async () => {}),
    run: vi.fn(async () => {}),
    onRestartScheduled: vi.fn(),
    setTimer: (fn: () => void, ms: number) => {
      timers.push({ fn, ms });
      return timers.length;
    },
    clearTimer: vi.fn(),
    ...over,
  };
  const sup = new ConsumerSupervisor(deps, { baseMs: 1_000, maxMs: 4_000 });
  /** Выполнить последний запланированный таймер. */
  const fire = async () => {
    timers.pop()!.fn();
    await flush();
  };
  return { sup, deps, timers, fire };
}

describe('ConsumerSupervisor', () => {
  it('CRASH с restart=true — ничего не делаем (перезапустит KafkaJS)', () => {
    const { sup, timers } = make();
    sup.onCrash(true, new Error('x'));
    expect(timers).toHaveLength(0);
  });

  it('CRASH с restart=false — сами: connect + run через backoff', async () => {
    const { sup, deps, timers, fire } = make();
    sup.start();
    sup.onCrash(false, new Error('KafkaJSGroupCoordinatorNotFound'));
    expect(timers.map((t) => t.ms)).toEqual([1_000]);
    await fire();
    expect(deps.connect).toHaveBeenCalledTimes(1);
    expect(deps.run).toHaveBeenCalledTimes(2); // start + перезапуск
    expect(sup.selfRestarts).toBe(1);
  });

  it('connect не удался (Kafka ещё лежит) → backoff растёт до потолка', async () => {
    const { sup, timers, fire } = make({
      connect: vi.fn(async () => {
        throw new Error('ECONNREFUSED');
      }),
    });
    sup.onCrash(false, new Error('x'));
    const delays = [timers[0].ms];
    for (let i = 0; i < 3; i++) {
      await fire();
      delays.push(timers[timers.length - 1].ms);
    }
    expect(delays).toEqual([1_000, 2_000, 4_000, 4_000]);
  });

  it('вступление в группу сбрасывает backoff', async () => {
    const { sup, timers, fire } = make();
    sup.onCrash(false, new Error('x'));
    await fire();
    sup.onJoined();
    sup.onCrash(false, new Error('y'));
    expect(timers[timers.length - 1].ms).toBe(1_000);
  });

  it('run() отклонён → тоже перезапуск; повторный краш не плодит второй таймер', async () => {
    const { sup, timers } = make({
      run: vi.fn(async () => {
        throw new Error('join failed');
      }),
    });
    sup.start();
    await flush();
    sup.onCrash(false, new Error('again'));
    expect(timers).toHaveLength(1);
  });

  it('после stop() перезапусков нет', async () => {
    const { sup, deps, timers, fire } = make();
    sup.onCrash(false, new Error('x'));
    sup.stop();
    expect(deps.clearTimer).toHaveBeenCalled();
    await fire(); // даже если таймер всё же сработал
    expect(deps.connect).not.toHaveBeenCalled();
    sup.onCrash(false, new Error('y'));
    expect(timers).toHaveLength(0);
  });
});
