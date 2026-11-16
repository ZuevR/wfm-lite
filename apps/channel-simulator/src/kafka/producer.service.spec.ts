import { describe, expect, it } from 'vitest';
import {
  KafkaJSConnectionError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
} from 'kafkajs';
import { isRetriableKafkaError } from './producer.service';

/**
 * Типы kafkajs объявляют конструкторы ошибок с одним аргументом, а в рантайме они принимают
 * { cause, retriable } (errors.js) — именно так их создаёт retrier. Воспроизводим рантайм-цепочку.
 */
type ErrorCtor = new (
  e: Error | string,
  opts?: { cause?: Error },
) => Error & { retriable: boolean };
const NonRetriable = KafkaJSNonRetriableError as unknown as ErrorCtor;
const RetriesExceeded = KafkaJSNumberOfRetriesExceeded as unknown as ErrorCtor;

describe('isRetriableKafkaError — по причине, а не по обёртке', () => {
  it('ретраи KafkaJS кончились на ошибке соединения → повторяем (обёртка NonRetriable, причина — retriable)', () => {
    // Цепочка как в живом логе (retry/index.js): внутренний retrier исчерпан → NumberOfRetriesExceeded
    // (тоже retriable=false) → внешний retrier заворачивает в NonRetriable с cause = исходная ошибка.
    const conn = new KafkaJSConnectionError('Connection error');
    const exceeded = new RetriesExceeded(conn, { cause: conn });
    const wrapped = new NonRetriable(exceeded, { cause: conn }); // e.cause || e
    expect(wrapped.retriable).toBe(false);
    expect(exceeded.retriable).toBe(false);
    expect(isRetriableKafkaError(wrapped)).toBe(true);
  });

  it('причина неретраемая (MESSAGE_TOO_LARGE) → не повторяем', () => {
    // ProtocolError берёт retriable из исходной ошибки брокера; MESSAGE_TOO_LARGE — неретраемая.
    const tooLarge = Object.assign(new KafkaJSProtocolError('Message too large'), {
      type: 'MESSAGE_TOO_LARGE',
      retriable: false,
    });
    expect(isRetriableKafkaError(tooLarge)).toBe(false);
    expect(isRetriableKafkaError(new NonRetriable(tooLarge, { cause: tooLarge }))).toBe(false);
  });

  it('прямая ошибка соединения → повторяем', () => {
    expect(isRetriableKafkaError(new KafkaJSConnectionError('x'))).toBe(true);
  });
});
