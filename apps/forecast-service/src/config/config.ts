import { z } from 'zod';
import { brokers, flag, logLevelSchema, parseEnv } from '@wfm/service-kit';
import { CONSUMER_GROUPS, DLQ_TOPICS } from '@wfm/contracts';

const schema = z.object({
  FORECAST_PORT: z.coerce.number().int().positive().default(3003),
  KAFKA_BROKERS: brokers,
  // Общая группа на все экземпляры: каждое событие учитывается в прогнозе один раз.
  FORECAST_GROUP_ID: z.string().min(1).default(CONSUMER_GROUPS.FORECAST_SERVICE),
  // Прогнозу нужна история → новая группа читает топики с начала. Влияет ТОЛЬКО на новую группу.
  FORECAST_FROM_BEGINNING: flag('true'),
  // Надёжность: DLQ и повторы. См. error-policy.ts и reliable-processor.ts.
  FORECAST_DLQ_TOPIC: z.string().min(1).default(DLQ_TOPICS.FORECAST_SERVICE),
  FORECAST_RETRY_BASE_MS: z.coerce.number().int().positive().default(200),
  // Потолок паузы между попытками. Компромисс: больше — меньше долбим лежащую БД; меньше — быстрее
  // подхватываем её возврат (после восстановления ждём до maxMs). Для одной группы 10 с — ок.
  FORECAST_RETRY_MAX_MS: z.coerce.number().int().positive().default(10_000),
  // Неизвестная ошибка (не окружение и не данные) — столько попыток, потом DLQ.
  FORECAST_UNKNOWN_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  // Окно дозревания AHT и срок, после которого незавершённое обращение считаем потерянным (unclosed).
  // Должно быть больше реального максимума handle time (симулятор: 300 с). В живом КЦ — час и больше.
  FORECAST_AHT_MATURITY_SEC: z.coerce.number().int().min(0).default(900),
  DATABASE_URL: z.string().url().default('postgres://wfm:wfm@localhost:5432/wfm'),
  // Dev-удобство: миграции при старте. node-pg-migrate берёт advisory lock, поэтому параллельный
  // старт нескольких экземпляров безопасен. На проде чаще отдельный шаг деплоя (job перед rollout).
  FORECAST_MIGRATE_ON_START: flag('true'),
  LOG_LEVEL: logLevelSchema,
  LOG_PRETTY: flag('false'),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const e = parseEnv(schema, env);
  return {
    serviceName: 'forecast-service',
    port: e.FORECAST_PORT,
    kafka: {
      brokers: e.KAFKA_BROKERS,
      clientId: 'forecast-service',
      groupId: e.FORECAST_GROUP_ID,
      fromBeginning: e.FORECAST_FROM_BEGINNING,
      dlqTopic: e.FORECAST_DLQ_TOPIC,
    },
    retry: {
      baseMs: e.FORECAST_RETRY_BASE_MS,
      maxMs: e.FORECAST_RETRY_MAX_MS,
      unknownMaxAttempts: e.FORECAST_UNKNOWN_MAX_ATTEMPTS,
      // heartbeatInterval KafkaJS по умолчанию 3 с, sessionTimeout 30 с: ждём кусками по 3 с.
      heartbeatEveryMs: 3_000,
    },
    forecast: { ahtMaturitySec: e.FORECAST_AHT_MATURITY_SEC },
    db: { url: e.DATABASE_URL, migrateOnStart: e.FORECAST_MIGRATE_ON_START },
    log: { level: e.LOG_LEVEL, pretty: e.LOG_PRETTY },
  };
}

export type ForecastConfig = ReturnType<typeof loadConfig>;
export const APP_CONFIG = Symbol('APP_CONFIG');
