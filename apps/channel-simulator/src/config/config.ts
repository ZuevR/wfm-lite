import { z } from 'zod';
import { brokers, flag, logLevelSchema, parseEnv } from '@wfm/service-kit';

const schema = z.object({
  SIMULATOR_PORT: z.coerce.number().int().positive().default(3001),
  KAFKA_BROKERS: brokers,
  SIMULATOR_RATE_PER_SEC: z.coerce.number().positive().max(500).default(2),
  SIMULATOR_AUTOSTART: flag('true'),
  // Сколько живёт обращение до interaction.ended (handle time), секунды.
  SIMULATOR_HANDLE_SEC_MIN: z.coerce.number().int().min(1).default(30),
  SIMULATOR_HANDLE_SEC_MAX: z.coerce.number().int().min(1).default(300),
  // Доля обращений, где ended отправляется раньше started — проверка нечувствительности к порядку.
  SIMULATOR_REORDER_RATE: z.coerce.number().min(0).max(1).default(0.05),
  // Буфер отправки: сколько событий держим в памяти, пока Kafka недоступна. При темпе 2/с
  // 10 000 — ~80 мин простоя; при 100/с — ~100 с. ~1 КБ на событие → ≤10 МБ памяти.
  SIMULATOR_BUFFER_CAPACITY: z.coerce.number().int().positive().default(10_000),
  SIMULATOR_BATCH_MAX: z.coerce.number().int().positive().max(10_000).default(500),
  LOG_LEVEL: logLevelSchema,
  LOG_PRETTY: flag('false'),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const e = parseEnv(schema, env);
  if (e.SIMULATOR_HANDLE_SEC_MIN > e.SIMULATOR_HANDLE_SEC_MAX) {
    throw new Error('Invalid configuration: SIMULATOR_HANDLE_SEC_MIN > SIMULATOR_HANDLE_SEC_MAX');
  }
  return {
    serviceName: 'channel-simulator',
    port: e.SIMULATOR_PORT,
    kafka: { brokers: e.KAFKA_BROKERS, clientId: 'channel-simulator' },
    buffer: { capacity: e.SIMULATOR_BUFFER_CAPACITY, maxBatch: e.SIMULATOR_BATCH_MAX },
    simulator: {
      ratePerSec: e.SIMULATOR_RATE_PER_SEC,
      autostart: e.SIMULATOR_AUTOSTART,
      lifecycle: {
        handleSecMin: e.SIMULATOR_HANDLE_SEC_MIN,
        handleSecMax: e.SIMULATOR_HANDLE_SEC_MAX,
        reorderRate: e.SIMULATOR_REORDER_RATE,
      },
    },
    log: { level: e.LOG_LEVEL, pretty: e.LOG_PRETTY },
  };
}

export type SimulatorConfig = ReturnType<typeof loadConfig>;
export const APP_CONFIG = Symbol('APP_CONFIG');
