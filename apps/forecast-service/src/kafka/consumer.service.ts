import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';
import { Consumer, EachMessagePayload, Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { buildDeadLetter, DLQ_REASONS, TOPICS, type DlqReason } from '@wfm/contracts';
import { APP_CONFIG, type ForecastConfig } from '../config/config';
import { InteractionsRepository } from '../database/interactions.repository';
import {
  ConsumerSupervisor,
  createConsumerMetrics,
  kafkaLogCreator,
  METRICS_REGISTRY,
  prom,
  type ConsumerMetrics,
} from '@wfm/service-kit';
import { handleRecord } from './message-handler';
import {
  deadLetterInvalid,
  processReliably,
  ProcessingAbortedError,
  type ProcessOutcome,
  type ProcessorDeps,
} from './reliable-processor';

const SUBSCRIBED_TOPICS = [TOPICS.INTERACTION_STARTED, TOPICS.INTERACTION_ENDED];

/**
 * Kafka consumer группы forecast-service (общая на все экземпляры: работу ДЕЛИМ, каждое событие —
 * один раз). Читает оба топика; они co-partitioned (3 партиции, ключ interactionId), поэтому
 * started и ended одного обращения приходят к одному экземпляру — но порядок МЕЖДУ топиками
 * не гарантирован, и upsert в репозитории от него не зависит.
 *
 * Доставка at-least-once (autoCommit после успешного eachMessage) + идемпотентный upsert по eventId
 * = эффект exactly-once для таблицы interactions.
 *
 * Ошибки (см. error-policy.ts / reliable-processor.ts, ADR 2026-11-12):
 *  - невалидное сообщение → DLQ `forecast-service.dlq` (reason=invalid), offset двигается;
 *  - БД отвергла данные (SQLSTATE 22/23) → DLQ (reason=rejected);
 *  - окружение (БД/Kafka недоступны) → повторы с backoff ВНУТРИ eachMessage + heartbeat:
 *    партиция ждёт, offset стоит, консьюмер не падает и не ребалансится;
 *  - неизвестная ошибка → N попыток, потом DLQ (reason=unknown).
 * Offset коммитится только после записи в БД ИЛИ в DLQ — сообщение не теряется ни в каком случае.
 */
@Injectable()
export class ConsumerService
  implements OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown
{
  private readonly consumer: Consumer;
  private readonly producer: Producer;
  private ready = false;
  /** Перезапуск консьюмера, когда KafkaJS сам не перезапустит. */
  private readonly supervisor: ConsumerSupervisor;
  private stopping = false;
  private assignment: Record<string, number[]> = {};
  private readonly counters = {
    started: 0,
    ended: 0,
    duplicates: 0,
    retries: 0,
    deadLettered: { invalid: 0, rejected: 0, unknown: 0 } satisfies Record<DlqReason, number>,
  };
  /** Сейчас висим на повторах: с какого момента и почему (для /stats и разбора инцидента). */
  private blocked: {
    since: string;
    topic: string;
    partition: number;
    offset: string;
    lastError: string;
    attempts: number;
  } | null = null;
  private readonly metrics: ConsumerMetrics & {
    dlq: prom.Counter<'reason'>;
    retries: prom.Counter<'step' | 'error_class'>;
  };

  constructor(
    @Inject(APP_CONFIG) private readonly config: ForecastConfig,
    private readonly repo: InteractionsRepository,
    @InjectPinoLogger(ConsumerService.name) private readonly logger: PinoLogger,
    @Inject(METRICS_REGISTRY) registry: prom.Registry,
  ) {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.INFO,
      logCreator: kafkaLogCreator(logger),
    });
    this.consumer = kafka.consumer({
      groupId: config.kafka.groupId,
      allowAutoTopicCreation: false,
    });
    // Продюсер только для DLQ. idempotent: повтор send после таймаута не создаст дубль в DLQ
    // (брокер отсекает по producerId + sequence). Требует acks=all — передаём в send.
    // DefaultPartitioner (murmur2, как в Java) — исходный ключ ляжет в DLQ в партицию с тем же номером.
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      createPartitioner: Partitioners.DefaultPartitioner,
      // У idempotent по умолчанию retries = ∞: send висел бы внутри eachMessage без heartbeat →
      // консьюмер выпал бы из группы. Ограничиваем — повторяет reliable-processor тем же сообщением
      // (KafkaJS откатывает sequence при ошибке, дубль всё равно распознаётся; ADR 2026-11-12).
      retry: { retries: 2 },
    });
    this.supervisor = new ConsumerSupervisor({
      connect: () => this.consumer.connect(),
      run: () => this.consumer.run({ eachMessage: (payload) => this.onMessage(payload) }),
      onRunFailed: (err) => {
        this.ready = false;
        this.logger.error({ err, groupId: config.kafka.groupId }, 'consumer run failed');
      },
      onRestartScheduled: ({ attempt, delayMs, error }) =>
        this.logger.error(
          { err: error, attempt, restartInMs: delayMs },
          'consumer stopped without KafkaJS restart — restarting it ourselves',
        ),
    });
    this.instrument();
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- collect() вызывается с this = Gauge
    const self = this;
    new prom.Gauge({
      name: 'wfm_consumer_blocked_seconds',
      help: 'Сколько секунд консьюмер висит на повторах одного сообщения (0 — не висит). Партиция ждёт',
      registers: [registry],
      collect() {
        this.set(self.blocked ? (Date.now() - Date.parse(self.blocked.since)) / 1000 : 0);
      },
    });
    this.metrics = {
      ...createConsumerMetrics(registry, {
        consumer: this.consumer,
        groupId: config.kafka.groupId,
        isInGroup: () => this.ready,
      }),
      dlq: new prom.Counter({
        name: 'wfm_dlq_messages_total',
        help: 'Сообщения, отправленные в DLQ (reason: invalid, rejected, unknown)',
        labelNames: ['reason'],
        registers: [registry],
      }),
      retries: new prom.Counter({
        name: 'wfm_processing_retries_total',
        help: 'Повторы обработки (step: apply | dead-letter; error_class: environment | message | unknown)',
        labelNames: ['step', 'error_class'],
        registers: [registry],
      }),
    };
    // Ряд счётчика с меткой появляется только после первого inc → increase() в алерте не увидит
    // ПЕРВОЕ сообщение в DLQ (не с чем сравнить). Заводим известные значения меток нулём заранее.
    for (const reason of DLQ_REASONS) this.metrics.dlq.inc({ reason }, 0);
  }

  async onApplicationBootstrap(): Promise<void> {
    const { groupId, fromBeginning } = this.config.kafka;
    // Продюсер DLQ — до старта чтения: первое же ядовитое сообщение должно куда-то уйти.
    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: SUBSCRIBED_TOPICS, fromBeginning });
    // run() не ждём: он резолвится только после вступления в группу (см. realtime-gateway).
    // Порт открывается сразу, вступление отражает /ready.
    this.supervisor.start();
    this.logger.info(
      { groupId, fromBeginning, topics: SUBSCRIBED_TOPICS },
      'consumer starting (joining group in background)',
    );
  }

  /**
   * До onApplicationShutdown остальных провайдеров (pool.end() в DatabaseService): сначала
   * перестаём читать, потом закрываем БД. stopping прерывает ожидание в повторах — offset
   * сообщения не закоммичен, после рестарта оно придёт снова.
   */
  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.stopping = true;
    this.supervisor.stop();
    await this.consumer.disconnect();
    this.logger.info({ signal }, 'consumer disconnected');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  isReady(): boolean {
    return this.ready;
  }

  stats() {
    return {
      groupId: this.config.kafka.groupId,
      ready: this.ready,
      assignment: this.assignment,
      counters: this.counters,
      dlqTopic: this.config.kafka.dlqTopic,
      blocked: this.blocked,
    };
  }

  private async onMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message, heartbeat } = payload;
    const startedAt = performance.now();
    const endTimer = this.metrics.processingSeconds.startTimer({ topic });
    const res = handleRecord({ topic, message });
    const kafkaCtx = { topic, partition, offset: message.offset };
    const logCtx = {
      correlationId: res.correlationId,
      ...(res.kind !== 'invalid' && { interactionId: res.row.interactionId }),
      ...kafkaCtx,
    };

    const deps: ProcessorDeps<{ applied: boolean }> = {
      apply: () =>
        res.kind === 'started'
          ? this.repo.upsertStarted(res.row)
          : res.kind === 'ended'
            ? this.repo.upsertEnded(res.row)
            : Promise.reject(new Error('unreachable: invalid message is not applied')),
      deadLetter: (reason, error, attempts) => this.sendToDlq(payload, reason, error, attempts),
      heartbeat,
      sleep: (ms) => sleep(ms),
      isStopping: () => this.stopping,
      onRetry: (e) => {
        this.counters.retries++;
        this.metrics.retries.inc({ step: e.step, error_class: e.errorClass });
        this.blocked = {
          since: this.blocked?.since ?? new Date().toISOString(),
          ...kafkaCtx,
          lastError: e.error,
          attempts: e.attempt,
        };
        this.logger.warn(
          {
            ...logCtx,
            step: e.step,
            attempt: e.attempt,
            errorClass: e.errorClass,
            error: e.error,
            retryInMs: e.delayMs,
          },
          e.step === 'apply'
            ? 'processing failed — retrying, partition waits'
            : 'dlq send failed — retrying',
        );
      },
    };

    let outcome: ProcessOutcome<{ applied: boolean }>;
    try {
      outcome =
        res.kind === 'invalid'
          ? await deadLetterInvalid(res.reason, this.config.retry, deps)
          : await processReliably(this.config.retry, deps);
    } catch (err) {
      if (err instanceof ProcessingAbortedError) {
        this.logger.warn(logCtx, 'processing aborted by shutdown — will be redelivered');
      }
      throw err;
    }

    if (this.blocked) {
      this.logger.info(
        { ...logCtx, blockedSince: this.blocked.since, attempts: this.blocked.attempts },
        'recovered after retries',
      );
      this.blocked = null;
    }
    const durationMs = Math.round(performance.now() - startedAt);
    endTimer();

    if (outcome.kind === 'dead-lettered') {
      this.counters.deadLettered[outcome.reason]++;
      this.metrics.dlq.inc({ reason: outcome.reason });
      this.metrics.consumed.inc({ topic, result: 'dead_lettered' });
      this.logger.warn(
        {
          ...logCtx,
          reason: outcome.reason,
          error: outcome.error,
          attempts: outcome.attempts,
          dlqTopic: this.config.kafka.dlqTopic,
          durationMs,
        },
        'message sent to DLQ',
      );
      return;
    }

    if (res.kind === 'invalid') return; // недостижимо: invalid всегда уходит в DLQ
    const { applied } = outcome.value;
    this.counters[res.kind]++;
    if (!applied) this.counters.duplicates++;
    this.metrics.consumed.inc({ topic, result: applied ? 'stored' : 'duplicate' });
    this.logger.info(
      { ...logCtx, eventId: res.row.eventId, applied, attempts: outcome.attempts, durationMs },
      applied ? `${res.kind} stored` : `${res.kind} duplicate ignored`,
    );
  }

  private async sendToDlq(
    { topic, partition, message }: EachMessagePayload,
    reason: DlqReason,
    error: string,
    attempts: number,
  ): Promise<void> {
    const dl = buildDeadLetter(
      { topic, partition, message },
      { consumerGroup: this.config.kafka.groupId, reason, error, attempts },
    );
    // acks=-1 (все ISR): offset исходного сообщения закоммитится сразу после этого send,
    // поэтому запись в DLQ должна быть подтверждена так же надёжно, как обычное событие.
    await this.producer.send({ topic: this.config.kafka.dlqTopic, acks: -1, messages: [dl] });
  }

  private instrument(): void {
    const { GROUP_JOIN, REBALANCING, CRASH, STOP, DISCONNECT } = this.consumer.events;
    this.consumer.on(GROUP_JOIN, ({ payload }) => {
      this.ready = true;
      this.supervisor.onJoined();
      this.assignment = payload.memberAssignment;
      this.logger.info(
        {
          groupId: payload.groupId,
          memberId: payload.memberId,
          assignment: payload.memberAssignment,
          durationMs: payload.duration,
        },
        'joined consumer group',
      );
    });
    this.consumer.on(REBALANCING, () => {
      this.ready = false;
      this.logger.warn('group rebalancing');
    });
    this.consumer.on(CRASH, ({ payload }) => {
      this.ready = false;
      this.logger.error({ err: payload.error, restart: payload.restart }, 'consumer crashed');
      this.supervisor.onCrash(payload.restart, payload.error);
    });
    this.consumer.on(STOP, () => (this.ready = false));
    this.consumer.on(DISCONNECT, () => (this.ready = false));
  }
}
