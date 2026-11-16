import type { DlqReason } from '@wfm/contracts';
import { backoffMs, classifyError, errorMessage, type ErrorClass } from './error-policy';

/**
 * Обработка одного сообщения с повторами и DLQ — без Kafka и БД внутри: всё I/O передаётся
 * зависимостями, поэтому логика тестируется на фейках.
 *
 * Почему повторы ВНУТРИ eachMessage, а не pause()/исключение (разбор runner.js KafkaJS 2.2.4):
 *  - pause() + return без исключения → KafkaJS считает сообщение обработанным и ДВИГАЕТ offset:
 *    сообщение потеряно (в Java-клиенте к pause добавили бы seek на текущий offset; в KafkaJS
 *    seek из eachMessage сбрасывает весь батч и усложняет код);
 *  - исключение → его ловит retrier раннера (5 попыток по умолчанию) и затем роняет консьюмер
 *    (CRASH → restart через ~10–30 с, ребаланс группы).
 * Блокирующий повтор с heartbeat держит партицию на месте (порядок сохранён, offset не двигается),
 * не роняет консьюмер и не вызывает ребаланс. При partitionsConsumedConcurrently=1 встаёт весь
 * консьюмер — для общей зависимости (одна БД на все партиции) это и нужно.
 */

export interface RetryPolicy {
  baseMs: number;
  maxMs: number;
  /** Сколько раз пробовать при НЕИЗВЕСТНОЙ ошибке, прежде чем отправить в DLQ. */
  unknownMaxAttempts: number;
  /** Не ждать дольше этого без heartbeat (меньше sessionTimeout группы, по умолчанию 30 с). */
  heartbeatEveryMs: number;
}

export interface RetryEvent {
  step: 'apply' | 'dead-letter';
  attempt: number;
  errorClass: ErrorClass;
  error: string;
  delayMs: number;
}

export interface ProcessorDeps<T> {
  /** Основная работа (запись в БД). */
  apply: () => Promise<T>;
  /** Отправка в DLQ. Должна быть надёжной: сюда же уходят и ошибки отправки — повторяем. */
  deadLetter: (reason: DlqReason, error: string, attempts: number) => Promise<void>;
  heartbeat: () => Promise<void>;
  sleep: (ms: number) => Promise<void>;
  isStopping: () => boolean;
  onRetry?: (e: RetryEvent) => void;
  random?: () => number;
}

export type ProcessOutcome<T> =
  | { kind: 'applied'; value: T; attempts: number }
  | { kind: 'dead-lettered'; reason: DlqReason; error: string; attempts: number };

/** Остановка сервиса прервала ожидание. offset не закоммичен → сообщение придёт повторно. */
export class ProcessingAbortedError extends Error {
  constructor() {
    super('processing aborted: consumer is stopping');
    this.name = 'ProcessingAbortedError';
  }
}

/** Ждём кусками, между ними heartbeat: консьюмер остаётся в группе, сколько бы ни длился простой БД. */
async function waitWithHeartbeat<T>(ms: number, policy: RetryPolicy, deps: ProcessorDeps<T>) {
  let left = ms;
  while (left > 0) {
    if (deps.isStopping()) throw new ProcessingAbortedError();
    const chunk = Math.min(left, policy.heartbeatEveryMs);
    await deps.sleep(chunk);
    left -= chunk;
    // Ошибку heartbeat (REBALANCE_IN_PROGRESS) не ловим: она уходит в KafkaJS, тот перевступит
    // в группу, а сообщение получит повторно тот, кому достанется партиция. Offset не сдвинут.
    await deps.heartbeat();
  }
  if (deps.isStopping()) throw new ProcessingAbortedError();
}

/** DLQ-отправка, которая не сдаётся: если Kafka недоступна — ждём, но offset без DLQ не двигаем. */
async function deadLetterReliably<T>(
  reason: DlqReason,
  error: string,
  attempts: number,
  policy: RetryPolicy,
  deps: ProcessorDeps<T>,
): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await deps.deadLetter(reason, error, attempts);
      return;
    } catch (err) {
      const delayMs = backoffMs(attempt - 1, policy, deps.random);
      deps.onRetry?.({
        step: 'dead-letter',
        attempt,
        errorClass: classifyError(err),
        error: errorMessage(err),
        delayMs,
      });
      await waitWithHeartbeat(delayMs, policy, deps);
    }
  }
}

export async function processReliably<T>(
  policy: RetryPolicy,
  deps: ProcessorDeps<T>,
): Promise<ProcessOutcome<T>> {
  let unknownFailures = 0;
  for (let attempt = 1; ; attempt++) {
    try {
      return { kind: 'applied', value: await deps.apply(), attempts: attempt };
    } catch (err) {
      const errorClass = classifyError(err);
      const error = errorMessage(err);
      if (errorClass === 'unknown') unknownFailures++;

      const reason: DlqReason | null =
        errorClass === 'message'
          ? 'rejected'
          : errorClass === 'unknown' && unknownFailures >= policy.unknownMaxAttempts
            ? 'unknown'
            : null;
      if (reason) {
        await deadLetterReliably(reason, error, attempt, policy, deps);
        return { kind: 'dead-lettered', reason, error, attempts: attempt };
      }

      const delayMs = backoffMs(attempt - 1, policy, deps.random);
      deps.onRetry?.({ step: 'apply', attempt, errorClass, error, delayMs });
      await waitWithHeartbeat(delayMs, policy, deps);
    }
  }
}

/** Невалидное сообщение: в обработку не пускаем, сразу в DLQ (надёжно). */
export async function deadLetterInvalid<T>(
  error: string,
  policy: RetryPolicy,
  deps: ProcessorDeps<T>,
): Promise<ProcessOutcome<T>> {
  await deadLetterReliably('invalid', error, 1, policy, deps);
  return { kind: 'dead-lettered', reason: 'invalid', error, attempts: 1 };
}
