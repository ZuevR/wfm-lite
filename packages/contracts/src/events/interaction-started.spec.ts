import { describe, expect, it } from 'vitest';
import {
  createInteractionStartedV1,
  parseInteractionStartedV1,
  partitionKeyOf,
  type InteractionStartedPayloadV1,
} from './interaction-started';

const payload: InteractionStartedPayloadV1 = {
  interactionId: '3f2504e0-4f89-41d3-9a0c-0305e82c3301',
  channel: 'chat',
  queue: 'cards',
  customerSegment: 'retail',
};

const valid = () => createInteractionStartedV1({ correlationId: 'corr-1', payload });

describe('interaction.started v1', () => {
  it('создаёт валидный конверт и переживает round-trip через JSON', () => {
    const event = valid();
    expect(event).toMatchObject({
      eventType: 'interaction.started',
      version: 1,
      correlationId: 'corr-1',
    });

    const parsed = parseInteractionStartedV1(JSON.stringify(event));
    expect(parsed).toEqual({ ok: true, event });
  });

  it('ключ партиционирования = interactionId (порядок событий одного обращения)', () => {
    expect(partitionKeyOf(valid())).toBe(payload.interactionId);
  });

  it('битый JSON — не исключение, а результат с причиной', () => {
    const res = parseInteractionStartedV1('{not json');
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.error).toMatch(/invalid JSON/);
  });

  it('пустое value (tombstone) отклоняется', () => {
    expect(parseInteractionStartedV1(null).ok).toBe(false);
    expect(parseInteractionStartedV1(undefined).ok).toBe(false);
  });

  it('указывает путь к невалидному полю', () => {
    const broken = { ...valid(), payload: { ...payload, channel: 'email' } };
    const res = parseInteractionStartedV1(JSON.stringify(broken));
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.error).toMatch(/payload\.channel/);
  });

  it('другая версия схемы — не v1 (версию не меняем молча)', () => {
    const res = parseInteractionStartedV1(JSON.stringify({ ...valid(), version: 2 }));
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.error).toMatch(/version/);
  });

  it('createInteractionStartedV1 не пропускает мусор на стороне продюсера', () => {
    expect(() => createInteractionStartedV1({ correlationId: '', payload })).toThrow();
  });
});
