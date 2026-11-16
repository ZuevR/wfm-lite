/**
 * Политика ошибок потребителя. Главный вопрос — не «можно ли повторить», а «В ЧЁМ причина»:
 *
 *  - environment — причина в окружении (БД/Kafka недоступны, перегрузка, дедлок, схема БД не та).
 *    Повтор поможет, когда окружение починят. Ломается ВСЁ подряд, поэтому DLQ здесь — вред:
 *    за минуту простоя БД весь поток уехал бы в DLQ. Повторяем бесконечно, партиция ждёт.
 *  - message — причина в самом сообщении (невалидные данные, нарушение ограничения).
 *    Повтор не поможет никогда. Сразу в DLQ, offset двигаем — партиция не встаёт.
 *  - unknown — не распознали (например, TypeError в нашем коде на необычных данных).
 *    Ограниченное число повторов, потом DLQ. Компромисс: баг в коде, который ломает КАЖДОЕ
 *    сообщение, тоже уведёт поток в DLQ — ловить алертом на рост DLQ (WfmDeadLetters).
 */
export type ErrorClass = 'environment' | 'message' | 'unknown';

/** Классы SQLSTATE (первые 2 символа), которые говорят о данных конкретного сообщения. */
const MESSAGE_SQLSTATE_CLASSES = new Set([
  '22', // data exception: значение не влезло, невалидная дата и т.п.
  '23', // integrity constraint violation: not null, unique, check, foreign key
]);

/** Классы SQLSTATE «дело в окружении». */
const ENVIRONMENT_SQLSTATE_CLASSES = new Set([
  '08', // connection exception
  '40', // transaction rollback: serialization failure, deadlock
  '53', // insufficient resources: диск, память, too many connections
  '55', // object not in prerequisite state: lock not available
  '57', // operator intervention: admin shutdown, cannot connect now, statement timeout
  '58', // system error (I/O)
  // 42 — syntax error / undefined table/column. Формально «не повторяемо», но причина не в сообщении,
  // а в рассинхроне кода и схемы (не накатили миграцию). DLQ'ить весь поток из-за деплоя — хуже,
  // чем подождать: починили схему → консьюмер продолжил с того же offset'а.
  '42',
]);

const NETWORK_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
]);

/** Сообщения node-postgres, у которых нет code. */
const PG_CONNECTION_MESSAGES = [
  'Connection terminated',
  'timeout exceeded when trying to connect',
  'Client has encountered a connection error',
  'Cannot use a pool after calling end',
];

const isSqlState = (code: unknown): code is string =>
  typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code);

export function classifyError(err: unknown): ErrorClass {
  if (!(err instanceof Error)) return 'unknown';
  // Ошибки KafkaJS (отправка в DLQ не удалась — брокер недоступен) — всегда окружение.
  if (err.name.startsWith('KafkaJS')) return 'environment';

  const code = (err as { code?: unknown }).code;
  if (typeof code === 'string' && NETWORK_CODES.has(code)) return 'environment';
  if (isSqlState(code)) {
    const cls = code.slice(0, 2);
    if (MESSAGE_SQLSTATE_CLASSES.has(cls)) return 'message';
    if (ENVIRONMENT_SQLSTATE_CLASSES.has(cls)) return 'environment';
    return 'unknown';
  }
  // AggregateError (pg на connect к нескольким адресам localhost: ::1 и 127.0.0.1)
  if (err instanceof AggregateError && err.errors.some((e) => classifyError(e) === 'environment')) {
    return 'environment';
  }
  if (PG_CONNECTION_MESSAGES.some((m) => err.message.includes(m))) return 'environment';
  return 'unknown';
}

/** Экспоненциальный backoff с «equal jitter»: случайно в [cap/2, cap], cap = min(max, base·2^attempt). Jitter — чтобы экземпляры не ретраили синхронно. */
export function backoffMs(
  attempt: number,
  opts: { baseMs: number; maxMs: number },
  random: () => number = Math.random,
): number {
  const cap = Math.min(opts.maxMs, opts.baseMs * 2 ** attempt);
  return Math.round(cap / 2 + (random() * cap) / 2);
}

/** Текст ошибки для лога. AggregateError (pg → ::1 и 127.0.0.1) приходит с пустым message — раскрываем. */
export function errorMessage(err: unknown): string {
  if (err instanceof AggregateError && !err.message) {
    return `AggregateError: ${err.errors.map(errorMessage).join('; ')}`;
  }
  return err instanceof Error ? `${err.name}: ${err.message}` : String(err);
}
