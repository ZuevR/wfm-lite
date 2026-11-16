import { hostname } from 'node:os';
import { z } from 'zod';
import { brokers, flag, logLevelSchema, parseEnv } from '@wfm/service-kit';
import { CONSUMER_GROUPS, dashboardGatewayGroupId } from '@wfm/contracts';

const schema = z.object({
  GATEWAY_PORT: z.coerce.number().int().positive().default(3002),
  KAFKA_BROKERS: brokers,
  // По умолчанию — своя группа на экземпляр (хост + порт), см. CONSUMER_GROUPS.DASHBOARD_GATEWAY_PREFIX.
  GATEWAY_GROUP_ID: z.string().min(1).optional(),
  GATEWAY_FROM_BEGINNING: flag('false'),
  WS_HEARTBEAT_MS: z.coerce.number().int().min(1000).default(30_000),
  // Lag-монитор: как часто опрашивать брокер и какие группы наблюдать всегда
  // (их пустота — инцидент). Группы gateway подхватываются сами, пока живы.
  GATEWAY_LAG_POLL_MS: z.coerce.number().int().min(1000).default(5_000),
  GATEWAY_LAG_WATCHED_GROUPS: z
    .string()
    .default(CONSUMER_GROUPS.FORECAST_SERVICE)
    .transform((s) =>
      s
        .split(',')
        .map((g) => g.trim())
        .filter(Boolean),
    ),
  LOG_LEVEL: logLevelSchema,
  LOG_PRETTY: flag('false'),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const e = parseEnv(schema, env);
  return {
    serviceName: 'realtime-gateway',
    port: e.GATEWAY_PORT,
    kafka: {
      brokers: e.KAFKA_BROKERS,
      clientId: 'realtime-gateway',
      groupId: e.GATEWAY_GROUP_ID ?? dashboardGatewayGroupId(`${hostname()}-${e.GATEWAY_PORT}`),
      fromBeginning: e.GATEWAY_FROM_BEGINNING,
    },
    ws: { heartbeatMs: e.WS_HEARTBEAT_MS },
    lag: { pollMs: e.GATEWAY_LAG_POLL_MS, watchedGroups: e.GATEWAY_LAG_WATCHED_GROUPS },
    log: { level: e.LOG_LEVEL, pretty: e.LOG_PRETTY },
  };
}

export type GatewayConfig = ReturnType<typeof loadConfig>;
export const APP_CONFIG = Symbol('APP_CONFIG');
