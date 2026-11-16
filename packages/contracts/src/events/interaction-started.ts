import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { envelopeSchema, parseEvent, type EnvelopeInput, type ParseResult } from './envelope';
import { TOPICS } from '../kafka/topics';

export const CHANNELS = ['chat', 'voice'] as const;
export const channelSchema = z.enum(CHANNELS);
export type Channel = z.infer<typeof channelSchema>;

export const interactionStartedPayloadV1 = z.object({
  /** id обращения (чат-сессии / звонка). Он же ключ партиционирования. */
  interactionId: z.string().uuid(),
  channel: channelSchema,
  /** Очередь / тематика: cards, credits, ... — по ней позже считаем прогноз. */
  queue: z.string().min(1).max(64),
  customerSegment: z.enum(['retail', 'premium', 'business']),
});
export type InteractionStartedPayloadV1 = z.infer<typeof interactionStartedPayloadV1>;

/** interaction.started v1 — клиент начал обращение (чат или звонок). */
export const interactionStartedV1 = envelopeSchema(
  TOPICS.INTERACTION_STARTED,
  1,
  interactionStartedPayloadV1,
);
export type InteractionStartedV1 = z.infer<typeof interactionStartedV1>;

/** Создаёт событие и сразу валидирует: невалидное событие не должно уйти в Kafka. */
export function createInteractionStartedV1(
  input: EnvelopeInput<InteractionStartedPayloadV1>,
): InteractionStartedV1 {
  return interactionStartedV1.parse({
    eventId: input.eventId ?? randomUUID(),
    eventType: TOPICS.INTERACTION_STARTED,
    version: 1,
    occurredAt: (input.occurredAt ?? new Date()).toISOString(),
    correlationId: input.correlationId,
    payload: input.payload,
  });
}

/**
 * Ключ партиционирования. Kafka кладёт сообщения с одинаковым ключом в одну партицию
 * (hash(key) % partitions), а порядок гарантирован только внутри партиции.
 * Ключ = interactionId → все события одного обращения (started, позже ended) идут по порядку.
 */
export const partitionKeyOf = (event: { payload: { interactionId: string } }): string =>
  event.payload.interactionId;

/** Безопасный разбор value из Kafka: никогда не бросает, возвращает причину ошибки. */
export function parseInteractionStartedV1(
  raw: string | null | undefined,
): ParseResult<InteractionStartedV1> {
  return parseEvent(interactionStartedV1, raw);
}
