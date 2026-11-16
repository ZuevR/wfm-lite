import { Inject, Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { Consumer, EachMessagePayload, Kafka, logLevel } from 'kafkajs';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { TOPICS } from '@wfm/contracts';
import { APP_CONFIG, type GatewayConfig } from '../config/config';
import { EventsGateway } from '../ws/events.gateway';
import {
  ConsumerSupervisor,
  createConsumerMetrics,
  kafkaLogCreator,
  METRICS_REGISTRY,
  prom,
  type ConsumerMetrics,
} from '@wfm/service-kit';
import { handleInteractionStarted } from './message-handler';

/**
 * Kafka consumer gateway. Группа — своя на экземпляр (`dashboard-gateway-<host>-<port>`):
 *   - экземпляры с ОДНИМ groupId делили бы партиции между собой — для WS-рассылки это баг
 *     (клиенты экземпляра видят только его партиции), поэтому каждому экземпляру своя группа;
 *   - офсеты коммитятся автоматически (autoCommit) после успешного eachMessage → at-least-once:
 *     упали между обработкой и коммитом — после рестарта часть событий придёт повторно.
 *     Для realtime-дашборда это ок; для Forecast — нужна идемпотентность.
 */
@Injectable()
export class ConsumerService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly consumer: Consumer;
  private ready = false;
  /** Перезапуск консьюмера, когда KafkaJS сам не перезапустит. */
  private readonly supervisor: ConsumerSupervisor;
  private stopping = false;
  private assignment: Record<string, number[]> = {};
  private processed = 0;
  private invalid = 0;
  private readonly metrics: ConsumerMetrics;

  constructor(
    @Inject(APP_CONFIG) private readonly config: GatewayConfig,
    private readonly ws: EventsGateway,
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
    this.metrics = createConsumerMetrics(registry, {
      consumer: this.consumer,
      groupId: config.kafka.groupId,
      isInGroup: () => this.ready,
    });
  }

  async onApplicationBootstrap(): Promise<void> {
    const { groupId, fromBeginning } = this.config.kafka;
    await this.consumer.connect();
    // fromBeginning влияет ТОЛЬКО когда у группы нет закоммиченного офсета (новая группа).
    await this.consumer.subscribe({ topics: [TOPICS.INTERACTION_STARTED], fromBeginning });
    // run() НЕ ждём: его промис резолвится только после JoinGroup/SyncGroup. После kill -9 старый
    // участник висит в группе до sessionTimeout (~30 с), и всё это время app.listen() не вызывался бы —
    // /health и /ws молчат, liveness-проба в k8s убила бы под. Вступление в группу отражает /ready
    // (флаг ready ставится по событию GROUP_JOIN).
    this.supervisor.start();
    this.logger.info(
      { groupId, fromBeginning, topics: [TOPICS.INTERACTION_STARTED] },
      'consumer starting (joining group in background)',
    );
  }

  async onApplicationShutdown(signal?: string): Promise<void> {
    this.stopping = true;
    this.supervisor.stop();
    // disconnect: дождаться текущего eachMessage, закоммитить офсеты, выйти из группы
    // (LeaveGroup → брокер сразу ребалансит, не ждёт session.timeout).
    await this.consumer.disconnect();
    this.logger.info({ signal }, 'consumer disconnected');
  }

  isReady(): boolean {
    return this.ready;
  }

  stats() {
    return {
      groupId: this.config.kafka.groupId,
      ready: this.ready,
      assignment: this.assignment,
      processed: this.processed,
      invalid: this.invalid,
      ws: this.ws.stats(),
    };
  }

  private async onMessage({ topic, partition, message }: EachMessagePayload): Promise<void> {
    const endTimer = this.metrics.processingSeconds.startTimer({ topic });
    const res = handleInteractionStarted({ topic, partition, message });
    if (res.kind === 'invalid') {
      this.invalid++;
      this.metrics.consumed.inc({ topic, result: 'invalid' });
      endTimer();
      this.logger.warn(
        {
          correlationId: res.correlationId,
          topic,
          partition,
          offset: message.offset,
          reason: res.reason,
        },
        'invalid message skipped',
      );
      return;
    }
    this.processed++;
    const delivered = this.ws.broadcast(res.wsMessage);
    this.metrics.consumed.inc({ topic, result: 'forwarded' });
    endTimer();
    this.logger.info(
      {
        correlationId: res.correlationId,
        eventId: res.wsMessage.event.eventId,
        topic,
        partition,
        offset: message.offset,
        wsDelivered: delivered,
      },
      'event forwarded to ws',
    );
  }

  /** Instrumentation events KafkaJS → логи + состояние readiness. */
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
          isLeader: payload.isLeader,
          assignment: payload.memberAssignment,
          durationMs: payload.duration,
        },
        'joined consumer group',
      );
    });
    this.consumer.on(REBALANCING, ({ payload }) => {
      this.ready = false;
      this.logger.warn(
        { groupId: payload.groupId, memberId: payload.memberId },
        'group rebalancing',
      );
    });
    this.consumer.on(CRASH, ({ payload }) => {
      this.ready = false;
      this.logger.error({ err: payload.error, restart: payload.restart }, 'consumer crashed');
      this.supervisor.onCrash(payload.restart, payload.error);
    });
    this.consumer.on(STOP, () => {
      this.ready = false;
    });
    this.consumer.on(DISCONNECT, () => {
      this.ready = false;
    });
  }
}
