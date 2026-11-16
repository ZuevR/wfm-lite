/**
 * Супервизор Kafka-консьюмера: перезапускает его, когда KafkaJS сам этого не сделает.
 *
 * KafkaJS перезапускает консьюмер после CRASH, только если ИСХОДНАЯ причина retriable
 * (consumer/index.js: getOriginalCause(e).retriable). KafkaJSGroupCoordinatorNotFound — наследник
 * NonRetriableError без cause: брокер только поднялся, координатор группы ещё не готов → CRASH с
 * restart=false, консьюмер мёртв навсегда, а процесс жив и /health = 200.
 * restartOnFailure в этом случае даже не вызывается.
 *
 * Без Kafka внутри: connect/run передаются зависимостями — логику проверяет unit-тест.
 * Было скопировано в forecast и gateway → вынесено в общий пакет.
 */
export interface SupervisorDeps {
  connect: () => Promise<void>;
  /** Запуск consumer.run(...). Промис не ждём до конца — он живёт, пока консьюмер работает. */
  run: () => Promise<void>;
  onRestartScheduled?: (e: { attempt: number; delayMs: number; error: unknown }) => void;
  onRunFailed?: (error: unknown) => void;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (t: unknown) => void;
}

export interface SupervisorOptions {
  baseMs: number;
  maxMs: number;
}

export class ConsumerSupervisor {
  private attempts = 0;
  private timer: unknown = null;
  private stopped = false;
  private restarts = 0;

  constructor(
    private readonly deps: SupervisorDeps,
    private readonly opts: SupervisorOptions = { baseMs: 1_000, maxMs: 30_000 },
  ) {}

  /** Первый запуск: run() без ожидания вступления в группу (порт сервиса открыт сразу). */
  start(): void {
    this.startRun();
  }

  /** Вызывать из обработчика CRASH: KafkaJS сам перезапустит, если restart=true. */
  onCrash(restart: boolean, error: unknown): void {
    if (!restart) this.scheduleRestart(error);
  }

  /** Вступили в группу — консьюмер жив, backoff сначала. */
  onJoined(): void {
    this.attempts = 0;
  }

  /** Остановка сервиса: никаких перезапусков после. */
  stop(): void {
    this.stopped = true;
    if (this.timer !== null) (this.deps.clearTimer ?? clearTimeout)(this.timer as NodeJS.Timeout);
    this.timer = null;
  }

  /** Сколько раз перезапускали сами (метрика wfm_consumer_self_restarts_total). */
  get selfRestarts(): number {
    return this.restarts;
  }

  private startRun(): void {
    this.deps.run().catch((err: unknown) => {
      this.deps.onRunFailed?.(err);
      this.scheduleRestart(err);
    });
  }

  private scheduleRestart(error: unknown): void {
    if (this.stopped || this.timer !== null) return; // уже запланирован — не плодим таймеры
    const delayMs = Math.min(this.opts.maxMs, this.opts.baseMs * 2 ** this.attempts++);
    this.deps.onRestartScheduled?.({ attempt: this.attempts, delayMs, error });
    const set = this.deps.setTimer ?? ((fn: () => void, ms: number) => setTimeout(fn, ms));
    this.timer = set(() => {
      this.timer = null;
      if (this.stopped) return;
      this.restarts++;
      this.deps.connect().then(
        () => this.startRun(),
        (e: unknown) => this.scheduleRestart(e),
      );
    }, delayMs);
  }
}
