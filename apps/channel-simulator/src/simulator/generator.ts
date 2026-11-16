import { randomUUID } from 'node:crypto';
import type { InteractionStartedPayloadV1, Outcome } from '@wfm/contracts';

export type Rng = () => number;

export const QUEUES = ['cards', 'credits', 'deposits', 'general'] as const;
const SEGMENTS = ['retail', 'premium', 'business'] as const;

const pick = <T>(items: readonly T[], rng: Rng): T => items[Math.floor(rng() * items.length)];

/** Случайное обращение. Допущение: 60% чатов / 40% звонков — упрощённое допущение. */
export function randomInteraction(rng: Rng = Math.random): InteractionStartedPayloadV1 {
  return {
    interactionId: randomUUID(),
    channel: rng() < 0.6 ? 'chat' : 'voice',
    queue: pick(QUEUES, rng),
    customerSegment: pick(SEGMENTS, rng),
  };
}

export interface LifecycleConfig {
  /** Длительность обращения (handle time), секунды: равномерно в [min, max]. */
  handleSecMin: number;
  handleSecMax: number;
  /** Доля обращений, у которых ended публикуется РАНЬШЕ started (проверка нечувствительности к порядку). */
  reorderRate: number;
}

export interface InteractionPlan {
  payload: InteractionStartedPayloadV1;
  handleSec: number;
  outcome: Outcome;
  /** true → сначала публикуем ended, потом started (occurredAt при этом честные: started < ended). */
  reordered: boolean;
}

/**
 * План жизни обращения. Допущения (упрощённые): 15% брошенных (клиент не дождался);
 * у «переставленных» handle time короткий (5–60 с), потому что обе отправки идут сразу,
 * а occurredAt started сдвигается в прошлое на handleSec.
 */
export function planInteraction(cfg: LifecycleConfig, rng: Rng = Math.random): InteractionPlan {
  const reordered = rng() < cfg.reorderRate;
  const [min, max] = reordered ? [5, 60] : [cfg.handleSecMin, cfg.handleSecMax];
  return {
    payload: randomInteraction(rng),
    handleSec: Math.round(min + rng() * (max - min)),
    outcome: rng() < 0.15 ? 'abandoned' : 'resolved',
    reordered,
  };
}
