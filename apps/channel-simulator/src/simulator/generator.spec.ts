import { describe, expect, it } from 'vitest';
import { interactionStartedPayloadV1 } from '@wfm/contracts';
import { planInteraction, randomInteraction } from './generator';

describe('randomInteraction', () => {
  it('всегда генерирует payload, валидный по схеме v1', () => {
    for (let i = 0; i < 1000; i++) {
      expect(interactionStartedPayloadV1.safeParse(randomInteraction()).success).toBe(true);
    }
  });

  it('interactionId уникален (это ключ партиционирования)', () => {
    const ids = new Set(Array.from({ length: 1000 }, () => randomInteraction().interactionId));
    expect(ids.size).toBe(1000);
  });

  it('детерминирован по rng (кроме id): rng < 0.6 → chat', () => {
    expect(randomInteraction(() => 0.1).channel).toBe('chat');
    expect(randomInteraction(() => 0.9).channel).toBe('voice');
  });
});

describe('planInteraction', () => {
  const cfg = { handleSecMin: 30, handleSecMax: 300, reorderRate: 0.05 };
  const seq = (...xs: number[]) => {
    let i = 0;
    return () => xs[i++ % xs.length];
  };

  it('обычное обращение: handle time в [min, max], не переставлено', () => {
    for (let i = 0; i < 1000; i++) {
      const p = planInteraction({ ...cfg, reorderRate: 0 });
      expect(p.reordered).toBe(false);
      expect(p.handleSec).toBeGreaterThanOrEqual(30);
      expect(p.handleSec).toBeLessThanOrEqual(300);
    }
  });

  it('переставленное: короткий handle time 5–60 с (обе отправки идут сразу)', () => {
    for (let i = 0; i < 200; i++) {
      const p = planInteraction({ ...cfg, reorderRate: 1 });
      expect(p.reordered).toBe(true);
      expect(p.handleSec).toBeGreaterThanOrEqual(5);
      expect(p.handleSec).toBeLessThanOrEqual(60);
    }
  });

  it('outcome по rng: < 0.15 → abandoned', () => {
    // порядок вызовов rng: reorder, payload (channel, queue, segment), handle, outcome
    expect(planInteraction(cfg, seq(0.9, 0.5, 0.5, 0.5, 0.5, 0.1)).outcome).toBe('abandoned');
    expect(planInteraction(cfg, seq(0.9, 0.5, 0.5, 0.5, 0.5, 0.9)).outcome).toBe('resolved');
  });
});
