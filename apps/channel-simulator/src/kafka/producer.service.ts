import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  Admin,
  Kafka,
  Partitioners,
  Producer,
  logLevel,
  type ICustomPartitioner,
  type Message,
  type PartitionMetadata,
} from 'kafkajs';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import {
  HEADERS,
  TOPICS,
  partitionKeyOf,
  type InteractionEndedV1,
  type InteractionStartedV1,
} from '@wfm/contracts';
import { APP_CONFIG, type SimulatorConfig } from '../config/config';
import { kafkaLogCreator, METRICS_REGISTRY, prom } from '@wfm/service-kit';
import { BufferClosedError, BufferFullError, PublishBuffer } from './publish-buffer';
import { ReadinessTracker } from './readiness';

/** События, которые публикует симулятор. Топик = eventType (конвенция контрактов). */
export type PublishableEvent = InteractionStartedV1 | InteractionEndedV1;

export interface PublishResult {
  partition: number;
  offset: string;
}

/** Элемент буфера: партиция вычислена ОДИН раз при приёме — повторы уходят туда же (см. publish-buffer). */
interface Outgoing {
  event: PublishableEvent;
  key: string;
  partition: number;
  message: Message;
}

const PUBLISHED_TOPICS = [TOPICS.INTERACTION_STARTED, TOPICS.INTERACTION_ENDED];

/**
 * Ошибку можно повторять? Смотрим на ПРИЧИНУ, а не на обёртку: когда ретраи клиента кончились,
 * retrier KafkaJS (retry/index.js) заворачивает исходную ошибку в KafkaJSNonRetriableError
 * (retriable=false), а настоящая причина — в `cause` (например, KafkaJSConnectionError, retriable=true).
 * Первая версия смотрела только на обёртку и выбрасывала батчи при любом простое Kafka.
 */
export function isRetriableKafkaError(err: unknown): boolean {
  if (!(err instanceof Error)) return true;
  const cause = (err as { cause?: unknown }).cause;
  if (cause instanceof Error) return isRetriableKafkaError(cause);
  return (err as { retriable?: boolean }).retriable !== false;
}

/** Как часто спрашиваем у брокера метаданные кластера (проба для /ready, когда поток событий стоит). */
const PROBE_INTERVAL_MS = 5_000;
/** Сколько ошибка считается «свежей» без новых проверок (см. ReadinessTracker). */
const FAILURE_STALE_MS = 3 * PROBE_INTERVAL_MS;

/**
 * Владелец Kafka-продюсера. Жизненный цикл привязан к Nest:
 *   onModuleInit         → connect (fail fast: нет Kafka — сервис не стартует)
 *   onApplicationShutdown → disconnect (досылает буфер, закрывает сокеты)
 *
 * Готовность (/ready) — по фактам: исход последней отправки или пробы метаданных.
 * Проба нужна, потому что при остановленном симуляторе отправок нет, а знать, жив ли брокер, хочется.
 *
 * Отправка: publish() кладёт событие в ограниченный буфер (PublishBuffer),
 * один воркер шлёт батчами с бесконечными повторами. Полный буфер → BufferFullError сразу.
 */
@Injectable()
export class ProducerService
  implements OnModuleInit, BeforeApplicationShutdown, OnApplicationShutdown
{
  private readonly producer: Producer;
  private readonly buffer: PublishBuffer<Outgoing, PublishResult>;
  private readonly partitioner: ReturnType<ICustomPartitioner>;
  /** Метаданные партиций по топику: нужны, чтобы посчитать партицию заранее. Обновляет проба. */
  private partitions = new Map<string, PartitionMetadata[]>();
  private readonly m: {
    published: prom.Counter<'topic'>;
    rejected: prom.Counter<'reason'>;
    failed: prom.Counter<'reason'>;
    retries: prom.Counter;
    batchSeconds: prom.Histogram;
  };
  /** Отдельный admin-клиент только для пробы: без ретраев, чтобы ошибка была видна сразу. */
  private readonly admin: Admin;
  private readonly readiness = new ReadinessTracker(FAILURE_STALE_MS);
  private probeTimer: NodeJS.Timeout | null = null;
  private probing = false;

  constructor(
    @Inject(APP_CONFIG) private readonly config: SimulatorConfig,
    @InjectPinoLogger(ProducerService.name) private readonly logger: PinoLogger,
    @Inject(METRICS_REGISTRY) registry: prom.Registry,
  ) {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.INFO,
      logCreator: kafkaLogCreator(logger),
    });
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
      // Идемпотентный продюсер: брокер отсекает дубль, если повтор отправки (таймаут ответа)
      // на самом деле уже записался. Требует acks=all. maxInFlightRequests=1 — порядок без сюрпризов.
      idempotent: true,
      maxInFlightRequests: 1,
      // По умолчанию у idempotent retries = ∞ — send висел бы вечно, без логов и метрик. Ограничиваем:
      // повторяет наш буфер ТЕМ ЖЕ батчем, а KafkaJS при ошибке откатывает sequence → дубль всё равно
      // распознаётся. KafkaJS предупредит «Limiting retries … may invalidate EoS» — для нашей схемы это
      // не так (разбор sendMessages.js, ADR 2026-11-12). Через рестарт процесса (новый producerId) —
      // дубли возможны, их гасит идемпотентный консьюмер.
      retry: { retries: 2 },
    });
    // murmur2, как у Java-клиента: одинаковый ключ → одинаковая партиция в любом клиенте.
    // Вызываем сами, чтобы знать партицию до отправки (и для лога, и для «замороженных» повторов).
    this.partitioner = Partitioners.DefaultPartitioner();
    this.admin = kafka.admin({ retry: { retries: 0 } });
    this.buffer = new PublishBuffer<Outgoing, PublishResult>(
      {
        capacity: config.buffer.capacity,
        maxBatch: config.buffer.maxBatch,
        retry: { baseMs: 200, maxMs: 5_000 },
      },
      {
        sendBatch: (items) => this.sendBatch(items),
        isRetriable: isRetriableKafkaError,
        sleep: (ms) => sleep(ms),
        onRetry: ({ attempt, batchSize, error, delayMs }) => {
          this.m.retries.inc();
          this.logger.warn(
            { attempt, batchSize, buffered: this.buffer.size(), err: error, retryInMs: delayMs },
            'kafka send failed — events kept in buffer, retrying',
          );
        },
        onRecovered: (e) =>
          this.logger.info({ ...e, buffered: this.buffer.size() }, 'kafka send recovered'),
        onFailed: ({ batchSize, error }) => {
          this.m.failed.inc({ reason: 'non_retriable' }, batchSize);
          this.logger.error({ batchSize, err: error }, 'non-retriable send error — batch dropped');
        },
      },
    );
    this.m = this.createMetrics(registry);
    // Нулевые ряды заранее — иначе increase() в алерте пропустит первый отказ (см. forecast DLQ).
    for (const reason of ['full', 'closed']) this.m.rejected.inc({ reason }, 0);
    for (const reason of ['non_retriable', 'shutdown']) this.m.failed.inc({ reason }, 0);
  }

  private createMetrics(registry: prom.Registry) {
    const registers = [registry];
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- collect() вызывается с this = Gauge
    const self = this;
    new prom.Gauge({
      name: 'wfm_publish_buffer_size',
      help: 'Событий в буфере отправки (ждут Kafka)',
      registers,
      collect() {
        this.set(self.buffer.size());
      },
    });
    new prom.Gauge({
      name: 'wfm_publish_buffer_capacity',
      help: 'Ёмкость буфера отправки; size/capacity → 1 — скоро начнутся отказы',
      registers,
      collect() {
        this.set(self.buffer.stats().capacity);
      },
    });
    new prom.Gauge({
      name: 'wfm_publish_blocked_seconds',
      help: 'Сколько секунд отправка в Kafka не удаётся (0 — идёт)',
      registers,
      collect() {
        this.set((self.buffer.stats().blockedSinceMs ?? 0) / 1000);
      },
    });
    new prom.Gauge({
      name: 'wfm_kafka_producer_ready',
      help: '1 — последняя отправка или проба метаданных брокера успешна (то же, что /ready)',
      registers,
      collect() {
        this.set(self.readiness.isReady() ? 1 : 0);
      },
    });
    return {
      published: new prom.Counter({
        name: 'wfm_events_published_total',
        help: 'События, подтверждённые Kafka (acks=all)',
        labelNames: ['topic'],
        registers,
      }),
      rejected: new prom.Counter({
        name: 'wfm_publish_rejected_total',
        help: 'События, отвергнутые на входе в буфер (reason: full | closed) — явная потеря у источника',
        labelNames: ['reason'],
        registers,
      }),
      failed: new prom.Counter({
        name: 'wfm_publish_failed_total',
        help: 'Принятые в буфер, но не доставленные (reason: non_retriable | shutdown)',
        labelNames: ['reason'],
        registers,
      }),
      retries: new prom.Counter({
        name: 'wfm_publish_retries_total',
        help: 'Повторы отправки батча (Kafka недоступна)',
        registers,
      }),
      batchSeconds: new prom.Histogram({
        name: 'wfm_publish_batch_seconds',
        help: 'Время одного sendBatch (acks=all), успешного или нет',
        buckets: [0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 5],
        registers,
      }),
    };
  }

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    await this.admin.connect();
    // Fail fast: нет топиков — нет смысла стартовать (автосоздание выключено).
    await this.refreshMetadata();
    this.readiness.recordSuccess();
    this.probeTimer = setInterval(() => void this.probe(), PROBE_INTERVAL_MS);
    this.logger.info({ brokers: this.config.kafka.brokers }, 'kafka producer connected');
  }

  /** Досылаем буфер до закрытия соединения. Что не успело за 10 с — потеря, считаем и логируем. */
  async beforeApplicationShutdown(): Promise<void> {
    const { lost } = await this.buffer.close(10_000);
    if (lost > 0) this.m.failed.inc({ reason: 'shutdown' }, lost);
    if (lost > 0) this.logger.error({ lost }, 'events lost on shutdown: buffer not drained');
    else this.logger.info('publish buffer drained');
  }

  async onApplicationShutdown(signal?: string): Promise<void> {
    if (this.probeTimer) clearInterval(this.probeTimer);
    await this.producer.disconnect();
    await this.admin.disconnect();
    this.logger.info({ signal }, 'kafka producer disconnected');
  }

  isReady(): boolean {
    return this.readiness.isReady();
  }

  readinessDetails() {
    return this.readiness.snapshot();
  }

  bufferStats() {
    return this.buffer.stats();
  }

  freeSlots(): number {
    return this.buffer.freeSlots();
  }

  private async refreshMetadata(): Promise<void> {
    const { topics } = await this.admin.fetchTopicMetadata({ topics: PUBLISHED_TOPICS });
    this.partitions = new Map(topics.map((t) => [t.name, t.partitions]));
  }

  private async probe(): Promise<void> {
    if (this.probing) return; // предыдущая проба ещё висит — не копим запросы
    this.probing = true;
    const wasReady = this.readiness.isReady();
    try {
      // Метаданные топиков: заодно и проба брокера, и свежие партиции для partitioner'а.
      await this.refreshMetadata();
      this.readiness.recordSuccess();
    } catch (err) {
      this.readiness.recordFailure(err);
    } finally {
      this.probing = false;
    }
    const isReady = this.readiness.isReady();
    if (wasReady !== isReady) {
      // Логируем только смену состояния, а не каждую пробу.
      const details = this.readiness.snapshot();
      if (isReady) this.logger.info(details, 'kafka reachable again — ready');
      else this.logger.warn(details, 'kafka unreachable — not ready');
    }
  }

  /**
   * Принять событие к отправке. Бросает СИНХРОННО BufferFullError / BufferClosedError — источник
   * узнаёт об отказе сразу. Promise резолвится, когда Kafka подтвердила запись (acks=all).
   */
  publish(event: PublishableEvent): Promise<PublishResult> {
    const key = partitionKeyOf(event);
    const message: Message = {
      key,
      value: JSON.stringify(event),
      headers: {
        [HEADERS.CORRELATION_ID]: event.correlationId,
        [HEADERS.EVENT_TYPE]: event.eventType,
        [HEADERS.EVENT_VERSION]: String(event.version),
      },
    };
    const partitionMetadata = this.partitions.get(event.eventType);
    if (!partitionMetadata?.length) throw new Error(`no metadata for topic ${event.eventType}`);
    const partition = this.partitioner({ topic: event.eventType, partitionMetadata, message });
    try {
      return this.buffer.enqueue({ event, key, partition, message: { ...message, partition } });
    } catch (err) {
      if (err instanceof BufferFullError) this.m.rejected.inc({ reason: 'full' });
      else if (err instanceof BufferClosedError) this.m.rejected.inc({ reason: 'closed' });
      throw err;
    }
  }

  /** Один запрос на батч (по топикам), результат — на каждое событие: offset = baseOffset + номер в партиции. */
  private async sendBatch(items: Outgoing[]): Promise<PublishResult[]> {
    const startedAt = performance.now();
    const byTopic = new Map<string, Message[]>();
    for (const it of items) {
      const list = byTopic.get(it.event.eventType) ?? [];
      list.push(it.message);
      byTopic.set(it.event.eventType, list);
    }
    let meta;
    const endTimer = this.m.batchSeconds.startTimer();
    try {
      meta = await this.producer.sendBatch({
        // acks=-1 (all): ждём подтверждения от всех in-sync реплик. Обязательно для idempotent.
        acks: -1,
        topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
      });
      this.readiness.recordSuccess();
    } catch (err) {
      this.readiness.recordFailure(err);
      throw err;
    } finally {
      endTimer();
    }
    for (const [topic, messages] of byTopic) this.m.published.inc({ topic }, messages.length);
    const base = new Map(
      meta.map((m) => [`${m.topicName}/${m.partition}`, BigInt(m.baseOffset ?? '0')]),
    );
    const seen = new Map<string, bigint>();
    const durationMs = Math.round(performance.now() - startedAt);
    return items.map((it) => {
      const tp = `${it.event.eventType}/${it.partition}`;
      const idx = seen.get(tp) ?? 0n;
      seen.set(tp, idx + 1n);
      const b = base.get(tp);
      const result = { partition: it.partition, offset: b === undefined ? '?' : String(b + idx) };
      this.logger.info(
        {
          correlationId: it.event.correlationId,
          eventId: it.event.eventId,
          topic: it.event.eventType,
          key: it.key,
          ...result,
          batchSize: items.length,
          durationMs,
        },
        'event published',
      );
      return result;
    });
  }
}
