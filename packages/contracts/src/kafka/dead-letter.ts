import { CONSUMER_GROUPS } from './topics';

/** Совместим с IHeaders из kafkajs; свой тип — чтобы contracts не зависел от Kafka-клиента. */
export type KafkaHeaders = Record<string, Buffer | string | (Buffer | string)[] | undefined>;

/**
 * Dead Letter Queue — топик для сообщений, которые потребитель не может обработать НИКОГДА
 * (дело в самом сообщении, а не в окружении). Партицию они не держат: offset коммитится после
 * записи в DLQ, а сообщение остаётся доступным для разбора и повторной отправки (replay).
 *
 * DLQ — на consumer group, а не на исходный топик (ADR 2026-11-12): ошибка — свойство пары
 * «сообщение × потребитель». Одно и то же событие может быть ядом для forecast и нормой для gateway.
 * Имя `<группа>.dlq` — исключение из `<сущность>.<событие>`: это не доменное событие, а служебная очередь.
 */
export const DLQ_TOPICS = {
  FORECAST_SERVICE: `${CONSUMER_GROUPS.FORECAST_SERVICE}.dlq`,
} as const;

/**
 * Заголовки, которые потребитель добавляет к сообщению при отправке в DLQ.
 * value и key исходного сообщения НЕ меняем: replay = отправить их обратно в original-topic как есть.
 * Исходные заголовки (x-correlation-id и др.) сохраняются — по correlationId сообщение из DLQ
 * находится в логах всех сервисов.
 */
export const DLQ_HEADERS = {
  ORIGINAL_TOPIC: 'dlq-original-topic',
  ORIGINAL_PARTITION: 'dlq-original-partition',
  ORIGINAL_OFFSET: 'dlq-original-offset',
  ORIGINAL_TIMESTAMP: 'dlq-original-timestamp',
  CONSUMER_GROUP: 'dlq-consumer-group',
  /** invalid — не прошло схему; rejected — отвергнуто хранилищем; unknown — неизвестная ошибка после N попыток. */
  REASON: 'dlq-reason',
  ERROR: 'dlq-error',
  ATTEMPTS: 'dlq-attempts',
  FAILED_AT: 'dlq-failed-at',
} as const;

export const DLQ_REASONS = ['invalid', 'rejected', 'unknown'] as const;
export type DlqReason = (typeof DLQ_REASONS)[number];

/** Длинный текст ошибки (стек zod на 50 полей) не должен раздувать заголовки. */
const MAX_ERROR_LENGTH = 1_000;

export interface DeadLetterSource {
  topic: string;
  partition: number;
  message: {
    key: Buffer | null;
    value: Buffer | null;
    headers?: KafkaHeaders;
    offset: string;
    timestamp: string;
  };
}

export interface DeadLetterFailure {
  consumerGroup: string;
  reason: DlqReason;
  error: string;
  attempts: number;
  failedAt?: Date;
}

export interface DeadLetterMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers: KafkaHeaders;
}

/** Чистая функция: исходная Kafka-запись + причина → сообщение для DLQ. */
export function buildDeadLetter(
  src: DeadLetterSource,
  failure: DeadLetterFailure,
): DeadLetterMessage {
  const error =
    failure.error.length > MAX_ERROR_LENGTH
      ? `${failure.error.slice(0, MAX_ERROR_LENGTH)}…`
      : failure.error;
  return {
    key: src.message.key,
    value: src.message.value,
    headers: {
      ...src.message.headers,
      [DLQ_HEADERS.ORIGINAL_TOPIC]: src.topic,
      [DLQ_HEADERS.ORIGINAL_PARTITION]: String(src.partition),
      [DLQ_HEADERS.ORIGINAL_OFFSET]: src.message.offset,
      [DLQ_HEADERS.ORIGINAL_TIMESTAMP]: src.message.timestamp,
      [DLQ_HEADERS.CONSUMER_GROUP]: failure.consumerGroup,
      [DLQ_HEADERS.REASON]: failure.reason,
      [DLQ_HEADERS.ERROR]: error,
      [DLQ_HEADERS.ATTEMPTS]: String(failure.attempts),
      [DLQ_HEADERS.FAILED_AT]: (failure.failedAt ?? new Date()).toISOString(),
    },
  };
}
