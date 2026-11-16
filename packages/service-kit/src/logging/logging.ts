// Вынесено из трёх копий (simulator, gateway, forecast): правило трёх сработало.
import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { logLevel, type LogEntry, type logCreator } from 'kafkajs';
import type { Params, PinoLogger } from 'nestjs-pino';
import { stdTimeFunctions } from 'pino';
import { HEADERS } from '@wfm/contracts';

/**
 * JSON-логи (pino). HTTP-запрос получает correlationId из заголовка x-correlation-id
 * или новый UUID; он же возвращается клиенту в ответе и попадает в каждую строку лога запроса.
 */
export function loggerParams(service: string, log: { level: string; pretty: boolean }): Params {
  return {
    pinoHttp: {
      level: log.level,
      base: { service },
      timestamp: stdTimeFunctions.isoTime,
      transport: log.pretty ? { target: 'pino-pretty', options: { singleLine: true } } : undefined,
      genReqId: (req, res) => {
        const header = req.headers[HEADERS.CORRELATION_ID];
        const id = (Array.isArray(header) ? header[0] : header) || randomUUID();
        res.setHeader(HEADERS.CORRELATION_ID, id);
        return id;
      },
      customAttributeKeys: { reqId: 'correlationId' },
      // Пробы и скрейп Prometheus (раз в 5–15 с) — не бизнес-запросы, в логе это шум.
      autoLogging: {
        ignore: (req) => req.url === '/health' || req.url === '/ready' || req.url === '/metrics',
      },
      redact: ['req.headers.authorization', 'req.headers.cookie'],
    },
  };
}

/** correlationId текущего HTTP-запроса (pino-http кладёт его в req.id). */
export function correlationIdOf(req: IncomingMessage): string {
  const id = (req as IncomingMessage & { id?: unknown }).id;
  return typeof id === 'string' && id.length > 0 ? id : randomUUID();
}

/** Мост: внутренние логи KafkaJS (connect, group join, rebalance, retry) → наш pino в JSON. */
export const kafkaLogCreator =
  (logger: PinoLogger): logCreator =>
  () =>
  ({ namespace, level, log }: LogEntry) => {
    const { message, timestamp, logger: _kafkaLogger, ...extra } = log;
    const ctx = { kafka: { namespace, ...extra } };
    switch (level) {
      case logLevel.ERROR:
      case logLevel.NOTHING:
        logger.error(ctx, message);
        break;
      case logLevel.WARN:
        logger.warn(ctx, message);
        break;
      case logLevel.INFO:
        logger.info(ctx, message);
        break;
      default:
        logger.debug(ctx, message);
    }
  };
