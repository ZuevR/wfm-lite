import type { IHeaders, KafkaMessage } from 'kafkajs';
import {
  HEADERS,
  TOPICS,
  parseInteractionEndedV1,
  parseInteractionStartedV1,
  type InteractionEndedV1,
  type InteractionStartedV1,
} from '@wfm/contracts';

/** Что записать в interactions по событию started. Поля ended не трогаем — операции коммутативны. */
export interface StartedRow {
  interactionId: string;
  channel: string;
  queue: string;
  customerSegment: string;
  startedAt: Date;
  eventId: string;
}

/** Что записать в interactions по событию ended. Поля started не трогаем. */
export interface EndedRow {
  interactionId: string;
  endedAt: Date;
  outcome: string;
  eventId: string;
}

export type HandleResult =
  | { kind: 'started'; correlationId: string; row: StartedRow }
  | { kind: 'ended'; correlationId: string; row: EndedRow }
  | { kind: 'invalid'; correlationId: string | undefined; reason: string };

export interface IncomingRecord {
  topic: string;
  message: Pick<KafkaMessage, 'value' | 'headers'>;
}

function readHeader(headers: IHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}

export const toStartedRow = (e: InteractionStartedV1): StartedRow => ({
  interactionId: e.payload.interactionId,
  channel: e.payload.channel,
  queue: e.payload.queue,
  customerSegment: e.payload.customerSegment,
  // occurredAt — время в домене. Не время записи в Kafka и не время обработки: при догоне
  // накопленного lag'а событие попадёт в СВОЙ интервал, а не в интервал «когда дочитали».
  startedAt: new Date(e.occurredAt),
  eventId: e.eventId,
});

export const toEndedRow = (e: InteractionEndedV1): EndedRow => ({
  interactionId: e.payload.interactionId,
  endedAt: new Date(e.occurredAt),
  outcome: e.payload.outcome,
  eventId: e.eventId,
});

/**
 * Чистая функция «Kafka-запись → что записать в БД», без I/O. Маршрут — по топику.
 * Невалидное сообщение — не исключение (иначе poison message остановит партицию), а результат.
 */
export function handleRecord({ topic, message }: IncomingRecord): HandleResult {
  const headerCorrelationId = readHeader(message.headers, HEADERS.CORRELATION_ID);
  const raw = message.value?.toString('utf8');

  if (topic === TOPICS.INTERACTION_STARTED) {
    const parsed = parseInteractionStartedV1(raw);
    return parsed.ok
      ? {
          kind: 'started',
          correlationId: parsed.event.correlationId,
          row: toStartedRow(parsed.event),
        }
      : { kind: 'invalid', correlationId: headerCorrelationId, reason: parsed.error };
  }
  if (topic === TOPICS.INTERACTION_ENDED) {
    const parsed = parseInteractionEndedV1(raw);
    return parsed.ok
      ? { kind: 'ended', correlationId: parsed.event.correlationId, row: toEndedRow(parsed.event) }
      : { kind: 'invalid', correlationId: headerCorrelationId, reason: parsed.error };
  }
  return {
    kind: 'invalid',
    correlationId: headerCorrelationId,
    reason: `unexpected topic ${topic}`,
  };
}
