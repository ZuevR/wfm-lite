import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { envelopeSchema, parseEvent, type EnvelopeInput, type ParseResult } from './envelope';
import { TOPICS } from '../kafka/topics';

export const OUTCOMES = ['resolved', 'abandoned'] as const;
export type Outcome = (typeof OUTCOMES)[number];

export const interactionEndedPayloadV1 = z.object({
  /** id обращения из interaction.started. Ключ партиционирования — тот же. */
  interactionId: z.string().uuid(),
  /** resolved — обращение обработано оператором; abandoned — клиент ушёл, не дождавшись. */
  outcome: z.enum(OUTCOMES),
});
export type InteractionEndedPayloadV1 = z.infer<typeof interactionEndedPayloadV1>;

/**
 * interaction.ended v1 — обращение завершилось. Время завершения — occurredAt конверта.
 * Сознательно минимальный payload (без channel/queue): они уже есть в started. Цена — если ended
 * пришёл раньше started (разные топики → общего порядка нет), у потребителя временно «неполная»
 * запись, и он обязан это переживать (см. forecast-service, upsert по interactionId).
 */
export const interactionEndedV1 = envelopeSchema(
  TOPICS.INTERACTION_ENDED,
  1,
  interactionEndedPayloadV1,
);
export type InteractionEndedV1 = z.infer<typeof interactionEndedV1>;

/** Создаёт событие и сразу валидирует: невалидное событие не должно уйти в Kafka. */
export function createInteractionEndedV1(
  input: EnvelopeInput<InteractionEndedPayloadV1>,
): InteractionEndedV1 {
  return interactionEndedV1.parse({
    eventId: input.eventId ?? randomUUID(),
    eventType: TOPICS.INTERACTION_ENDED,
    version: 1,
    occurredAt: (input.occurredAt ?? new Date()).toISOString(),
    correlationId: input.correlationId,
    payload: input.payload,
  });
}

export function parseInteractionEndedV1(
  raw: string | null | undefined,
): ParseResult<InteractionEndedV1> {
  return parseEvent(interactionEndedV1, raw);
}
