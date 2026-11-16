import { z } from 'zod';

/**
 * Конверт события — одинаковые служебные поля для всех событий системы.
 *   eventId       — уникален для события; понадобится для идемпотентности (дедуп по eventId)
 *   eventType     — что случилось; совпадает с именем топика
 *   version       — версия схемы payload; меняем схему → повышаем version
 *   occurredAt    — когда случилось в предметной области (не когда записали в Kafka)
 *   correlationId — сквозной id бизнес-цепочки, связывает логи всех сервисов
 */
export function envelopeSchema<E extends string, V extends number, P extends z.ZodTypeAny>(
  eventType: E,
  version: V,
  payload: P,
) {
  return z.object({
    eventId: z.string().uuid(),
    eventType: z.literal(eventType),
    version: z.literal(version),
    occurredAt: z.string().datetime({ offset: true }),
    correlationId: z.string().min(1).max(128),
    payload,
  });
}

export interface EnvelopeInput<P> {
  correlationId: string;
  payload: P;
  occurredAt?: Date;
  eventId?: string;
}

export type ParseResult<T> = { ok: true; event: T } | { ok: false; error: string };

export function formatZodError(error: z.ZodError): string {
  return error.issues.map((i) => `${i.path.join('.') || '<root>'}: ${i.message}`).join('; ');
}

/** Безопасный разбор value из Kafka по схеме: никогда не бросает, возвращает причину ошибки. */
export function parseEvent<S extends z.ZodTypeAny>(
  schema: S,
  raw: string | null | undefined,
): ParseResult<z.infer<S>> {
  if (raw == null || raw === '') return { ok: false, error: 'empty value (tombstone?)' };
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch (e) {
    return { ok: false, error: `invalid JSON: ${(e as Error).message}` };
  }
  const res = schema.safeParse(json);
  return res.success
    ? { ok: true, event: res.data }
    : { ok: false, error: formatZodError(res.error) };
}
