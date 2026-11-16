import { describe, expect, it } from 'vitest';
import {
  createInteractionEndedV1,
  createInteractionStartedV1,
  parseInteractionEndedV1,
  parseInteractionStartedV1,
  partitionKeyOf,
} from '../index';

const interactionId = '3f2504e0-4f89-41d3-9a0c-0305e82c3301';
const ended = () =>
  createInteractionEndedV1({
    correlationId: 'corr-1',
    payload: { interactionId, outcome: 'resolved' },
  });

describe('interaction.ended v1', () => {
  it('создаёт валидный конверт и переживает round-trip через JSON', () => {
    const event = ended();
    expect(event).toMatchObject({ eventType: 'interaction.ended', version: 1 });
    expect(parseInteractionEndedV1(JSON.stringify(event))).toEqual({ ok: true, event });
  });

  it('тот же ключ, что у started того же обращения (co-partitioning)', () => {
    const started = createInteractionStartedV1({
      correlationId: 'corr-1',
      payload: { interactionId, channel: 'chat', queue: 'cards', customerSegment: 'retail' },
    });
    expect(partitionKeyOf(ended())).toBe(partitionKeyOf(started));
  });

  it('неизвестный outcome — невалидно, с путём к полю', () => {
    const broken = { ...ended(), payload: { interactionId, outcome: 'transferred' } };
    const res = parseInteractionEndedV1(JSON.stringify(broken));
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.error).toMatch(/payload\.outcome/);
  });

  it('события не взаимозаменяемы: ended не проходит схему started и наоборот', () => {
    expect(parseInteractionStartedV1(JSON.stringify(ended())).ok).toBe(false);
  });
});
