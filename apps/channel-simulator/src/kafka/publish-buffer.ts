/**
 * Ограниченный буфер отправки в Kafka: FIFO + один воркер + бесконечные повторы + backpressure.
 * Без Kafka внутри — отправка передаётся зависимостью, поэтому логика тестируется на фейках.
 *
 * Зачем: раньше отправка жила только ретраями KafkaJS (~10–15 с), после чего событие
 * терялось молча (at-most-once). Теперь:
 *  - простой Kafka короче, чем «ёмкость / темп», не теряет НИЧЕГО — события ждут в буфере;
 *  - дольше — буфер заполняется и новые события ОТВЕРГАЮТСЯ на входе (BufferFullError): потеря
 *    явная, посчитанная и происходит у источника, который может отреагировать (HTTP 503, Retry-After);
 *  - память ограничена: без лимита за час простоя процесс съел бы всю память и упал — потеряв всё.
 * Чего НЕ даёт: переживания падения процесса (буфер в памяти). Это даёт только outbox (ADR 2026-11-12).
 *
 * Порядок: один воркер, батч уходит целиком, следующий — только после успеха → FIFO для всех событий,
 * в т.ч. для пары ended → started одного ключа (одна партиция).
 *
 * «Замороженный» батч: при ошибке повторяем РОВНО тот же набор сообщений, не добирая новые.
 * Идемпотентный продюсер KafkaJS при ошибке откатывает sequence партиции (sendMessages.js), и повтор
 * того же батча получает тот же firstSequence → если первая попытка на самом деле записалась
 * (таймаут ответа), брокер распознает дубль. Батч другого состава с тем же sequence — уже нет.
 */

export class BufferFullError extends Error {
  constructor(readonly capacity: number) {
    super(`publish buffer is full (capacity ${capacity})`);
    this.name = 'BufferFullError';
  }
}

export class BufferClosedError extends Error {
  constructor(message = 'publish buffer is closed') {
    super(message);
    this.name = 'BufferClosedError';
  }
}

export interface BufferOptions {
  capacity: number;
  maxBatch: number;
  retry: { baseMs: number; maxMs: number };
}

export interface BufferDeps<T, R> {
  /** Отправить батч; вернуть результат на каждый элемент в том же порядке. */
  sendBatch: (items: T[]) => Promise<R[]>;
  /** false — ошибка не пройдёт от повтора (сообщение слишком большое и т.п.): батч отвергается. */
  isRetriable: (err: unknown) => boolean;
  sleep: (ms: number) => Promise<void>;
  onRetry?: (e: { attempt: number; batchSize: number; error: unknown; delayMs: number }) => void;
  onRecovered?: (e: { attempts: number; batchSize: number; blockedMs: number }) => void;
  onFailed?: (e: { batchSize: number; error: unknown }) => void;
  now?: () => number;
  random?: () => number;
}

interface Entry<T, R> {
  item: T;
  enqueuedAt: number;
  resolve: (r: R) => void;
  reject: (e: unknown) => void;
}

export class PublishBuffer<T, R> {
  private readonly queue: Entry<T, R>[] = [];
  private inFlight: Entry<T, R>[] = [];
  private pumping = false;
  private closed = false;
  private wake: (() => void) | null = null;
  private readonly counters = { accepted: 0, delivered: 0, rejectedFull: 0, failed: 0, retries: 0 };
  private blockedSince: number | null = null;
  private lastError: string | null = null;

  constructor(
    private readonly opts: BufferOptions,
    private readonly deps: BufferDeps<T, R>,
  ) {}

  private get now() {
    return this.deps.now ?? Date.now;
  }

  /** Сколько элементов ещё влезет. Для пакетного приёма «всё или ничего» (HTTP burst). */
  freeSlots(): number {
    return this.opts.capacity - this.size();
  }

  size(): number {
    return this.queue.length + this.inFlight.length;
  }

  /**
   * Принять событие. Синхронно бросает BufferFullError/BufferClosedError — источник узнаёт об отказе
   * сразу, а не через минуту. Promise резолвится, когда Kafka подтвердила запись.
   */
  enqueue(item: T): Promise<R> {
    if (this.closed) throw new BufferClosedError();
    if (this.size() >= this.opts.capacity) {
      this.counters.rejectedFull++;
      throw new BufferFullError(this.opts.capacity);
    }
    this.counters.accepted++;
    const p = new Promise<R>((resolve, reject) => {
      this.queue.push({ item, enqueuedAt: this.now(), resolve, reject });
    });
    void this.pump();
    return p;
  }

  stats() {
    const oldest = this.inFlight[0] ?? this.queue[0];
    return {
      capacity: this.opts.capacity,
      size: this.size(),
      inFlight: this.inFlight.length,
      ...this.counters,
      blockedSinceMs: this.blockedSince === null ? null : this.now() - this.blockedSince,
      oldestAgeMs: oldest ? this.now() - oldest.enqueuedAt : null,
      lastError: this.lastError,
    };
  }

  /**
   * Graceful shutdown: перестать принимать, дать воркеру дослать за timeoutMs. Что не успело —
   * отвергаем BufferClosedError и возвращаем число: это потеря, её надо залогировать.
   */
  async close(timeoutMs: number): Promise<{ lost: number }> {
    this.closed = true;
    const deadline = this.now() + timeoutMs;
    while (this.size() > 0 && this.now() < deadline) {
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        void this.deps.sleep(Math.min(100, deadline - this.now())).then(resolve);
      });
    }
    const left = [...this.inFlight, ...this.queue];
    this.inFlight = [];
    this.queue.length = 0;
    const err = new BufferClosedError('publish buffer closed before delivery');
    for (const e of left) e.reject(err);
    this.counters.failed += left.length;
    return { lost: left.length };
  }

  private async pump(): Promise<void> {
    if (this.pumping) return;
    this.pumping = true;
    try {
      while (this.queue.length > 0) {
        this.inFlight = this.queue.splice(0, this.opts.maxBatch);
        await this.deliver(this.inFlight);
        this.inFlight = [];
        this.wake?.();
      }
    } finally {
      this.pumping = false;
    }
  }

  /** Отправить один «замороженный» батч: повторять тот же самый, пока не получится или не закроют. */
  private async deliver(batch: Entry<T, R>[]): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      // close() по таймауту уже отверг этот батч — бросаем повторы.
      if (this.inFlight !== batch) return;
      try {
        const results = await this.deps.sendBatch(batch.map((e) => e.item));
        if (this.inFlight !== batch) return; // ответ пришёл после close(): промисы уже отвергнуты
        batch.forEach((e, i) => e.resolve(results[i] as R));
        this.counters.delivered += batch.length;
        if (this.blockedSince !== null) {
          this.deps.onRecovered?.({
            attempts: attempt,
            batchSize: batch.length,
            blockedMs: this.now() - this.blockedSince,
          });
          this.blockedSince = null;
          this.lastError = null;
        }
        return;
      } catch (error) {
        if (this.inFlight !== batch) return;
        this.lastError = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
        if (!this.deps.isRetriable(error)) {
          for (const e of batch) e.reject(error);
          this.counters.failed += batch.length;
          this.deps.onFailed?.({ batchSize: batch.length, error });
          return;
        }
        this.blockedSince ??= this.now();
        this.counters.retries++;
        const cap = Math.min(this.opts.retry.maxMs, this.opts.retry.baseMs * 2 ** (attempt - 1));
        const delayMs = Math.round(cap / 2 + ((this.deps.random ?? Math.random)() * cap) / 2);
        this.deps.onRetry?.({ attempt, batchSize: batch.length, error, delayMs });
        await this.deps.sleep(delayMs);
      }
    }
  }
}
