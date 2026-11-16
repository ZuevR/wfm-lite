/**
 * Готовность продюсера по фактам, а не по событиям KafkaJS.
 *
 * Почему не флаг CONNECT/DISCONNECT: KafkaJS шлёт DISCONNECT только при явном producer.disconnect().
 * Брокер упал — флаг остаётся true, /ready врёт 200. Поэтому смотрим на результаты реальных
 * обращений к брокеру: отправки событий и периодической пробы метаданных.
 *
 * Правило: not ready, если последний исход — ошибка и она свежая (моложе staleAfterMs).
 * Свежесть нужна, чтобы старая ошибка не держала сервис в not ready вечно, когда проб нет.
 */
export class ReadinessTracker {
  private lastSuccessAt: number | null = null;
  private lastFailureAt: number | null = null;
  private lastError: string | null = null;

  constructor(private readonly staleAfterMs: number) {}

  recordSuccess(now: number = Date.now()): void {
    this.lastSuccessAt = now;
  }

  recordFailure(error: unknown, now: number = Date.now()): void {
    this.lastFailureAt = now;
    this.lastError = error instanceof Error ? error.message : String(error);
  }

  isReady(now: number = Date.now()): boolean {
    if (this.lastSuccessAt === null && this.lastFailureAt === null) return false; // ещё ни разу не проверяли
    if (this.lastFailureAt === null) return true;
    const failureIsLatest = this.lastSuccessAt === null || this.lastFailureAt > this.lastSuccessAt;
    const failureIsFresh = now - this.lastFailureAt < this.staleAfterMs;
    return !(failureIsLatest && failureIsFresh);
  }

  snapshot(now: number = Date.now()) {
    return {
      ready: this.isReady(now),
      lastSuccessAt: this.lastSuccessAt && new Date(this.lastSuccessAt).toISOString(),
      lastFailureAt: this.lastFailureAt && new Date(this.lastFailureAt).toISOString(),
      lastError: this.lastError,
    };
  }
}
