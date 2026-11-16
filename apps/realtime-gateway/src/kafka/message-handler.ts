import type { IHeaders, KafkaMessage } from 'kafkajs';
import { HEADERS, parseInteractionStartedV1, type WsEventMessage } from '@wfm/contracts';

export function readHeader(headers: IHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}

export type HandleResult =
  | { kind: 'valid'; correlationId: string; wsMessage: WsEventMessage }
  | { kind: 'invalid'; correlationId: string | undefined; reason: string };

export interface IncomingRecord {
  topic: string;
  partition: number;
  message: Pick<KafkaMessage, 'value' | 'headers' | 'offset'>;
}

/**
 * Чистая функция «Kafka-запись → сообщение для WS» без I/O: её легко тестировать,
 * а ConsumerService остаётся тонким (подписка, логи, метрики).
 * Невалидное сообщение не бросает исключение: иначе KafkaJS будет ретраить его бесконечно
 * и вся партиция встанет (poison message). Здесь — лог + пропуск: для realtime-витрины
 * потеря одного события на экране допустима (DLQ — в forecast-service).
 */
export function handleInteractionStarted(
  record: IncomingRecord,
  now: Date = new Date(),
): HandleResult {
  const { topic, partition, message } = record;
  const headerCorrelationId = readHeader(message.headers, HEADERS.CORRELATION_ID);
  const parsed = parseInteractionStartedV1(message.value?.toString('utf8'));
  if (!parsed.ok) {
    return { kind: 'invalid', correlationId: headerCorrelationId, reason: parsed.error };
  }
  return {
    kind: 'valid',
    correlationId: parsed.event.correlationId,
    wsMessage: {
      type: 'event',
      event: parsed.event,
      meta: { topic, partition, offset: message.offset, gatewayReceivedAt: now.toISOString() },
    },
  };
}
