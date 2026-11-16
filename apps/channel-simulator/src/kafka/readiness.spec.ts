import { describe, expect, it } from 'vitest';
import { ReadinessTracker } from './readiness';

const STALE = 30_000;

describe('ReadinessTracker', () => {
  it('до первой проверки — not ready', () => {
    expect(new ReadinessTracker(STALE).isReady(0)).toBe(false);
  });

  it('успешная отправка/проба — ready', () => {
    const t = new ReadinessTracker(STALE);
    t.recordSuccess(1_000);
    expect(t.isReady(2_000)).toBe(true);
  });

  it('свежая ошибка после успеха — not ready (брокер упал, флаг CONNECT этого не видит)', () => {
    const t = new ReadinessTracker(STALE);
    t.recordSuccess(1_000);
    t.recordFailure(new Error('KafkaJSConnectionError'), 2_000);
    expect(t.isReady(3_000)).toBe(false);
    expect(t.snapshot(3_000).lastError).toBe('KafkaJSConnectionError');
  });

  it('успех после ошибки — снова ready (брокер вернулся)', () => {
    const t = new ReadinessTracker(STALE);
    t.recordFailure(new Error('down'), 1_000);
    t.recordSuccess(2_000);
    expect(t.isReady(2_500)).toBe(true);
  });

  it('устаревшая ошибка без новых проверок не держит not ready вечно', () => {
    const t = new ReadinessTracker(STALE);
    t.recordSuccess(1_000);
    t.recordFailure(new Error('blip'), 2_000);
    expect(t.isReady(2_000 + STALE + 1)).toBe(true);
  });
});
