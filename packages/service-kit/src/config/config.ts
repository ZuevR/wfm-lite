import { z } from 'zod';

/** 'true' | 'false' из env → boolean (z.coerce.boolean() считает 'false' истиной). */
export const flag = (def: 'true' | 'false') =>
  z
    .enum(['true', 'false'])
    .default(def)
    .transform((v) => v === 'true');

/** KAFKA_BROKERS='a:9092, b:9092' → ['a:9092', 'b:9092']. */
export const brokers = z
  .string()
  .default('localhost:9092')
  .transform((s) =>
    s
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean),
  );

export const logLevelSchema = z
  .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace'])
  .default('info');

/** Разбор env по схеме: кривой конфиг роняет сервис сразу и с понятным списком проблем (fail fast). */
export function parseEnv<S extends z.ZodTypeAny>(schema: S, env: NodeJS.ProcessEnv): z.infer<S> {
  const res = schema.safeParse(env);
  if (!res.success) {
    const issues = res.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
    throw new Error(`Invalid configuration:\n${issues}`);
  }
  return res.data;
}
