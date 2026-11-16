import { Inject, Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { Admin, Kafka, logLevel } from 'kafkajs';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { CONSUMER_GROUPS, DLQ_TOPICS, type GroupLagView } from '@wfm/contracts';
import { kafkaLogCreator, METRICS_REGISTRY, prom } from '@wfm/service-kit';
import { APP_CONFIG, type GatewayConfig } from '../config/config';
import { EventsGateway } from '../ws/events.gateway';
import { groupLagView, partitionLags, selectGroups, type TopicWatermarks } from './lag';

/**
 * Lag consumer groups, измеренный СНАРУЖИ — через admin API брокера, а не из самого консьюмера.
 *
 * Почему снаружи: lag из END_BATCH_PROCESS (как было в /stats) обновляется, только пока консьюмер
 * обрабатывает батчи. Встал (блокирован, упал, «умер навсегда») — цифра замерла
 * на последнем значении, хотя реальный lag растёт. Брокер же знает и high watermark, и offset
 * группы всегда. Так работают Burrow и kafka-exporter; имена метрик — как у kafka-exporter.
 *
 * Живёт в gateway, потому что gateway — BFF дашборда: тот же снимок уходит в WS (панель «Lag»).
 * Несколько экземпляров gateway → несколько одинаковых рядов с разной меткой instance; в алертах — max by (group).
 */
@Injectable()
export class LagMonitorService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly admin: Admin;
  private timer: NodeJS.Timeout | null = null;
  private polling = false;
  /** Последнее состояние up — логируем только смену (не каждые 5 с). */
  private upGaugeValue: number | null = null;
  private last: { measuredAt: string; groups: GroupLagView[] } | null = null;
  private readonly lagGauge: prom.Gauge<'group' | 'topic' | 'partition'>;
  private readonly lagSumGauge: prom.Gauge<'group'>;
  private readonly membersGauge: prom.Gauge<'group' | 'state'>;
  private readonly upGauge: prom.Gauge;
  private readonly topicOffsetGauge: prom.Gauge<'topic' | 'partition'>;

  constructor(
    @Inject(APP_CONFIG) private readonly config: GatewayConfig,
    @Inject(METRICS_REGISTRY) registry: prom.Registry,
    private readonly ws: EventsGateway,
    @InjectPinoLogger(LagMonitorService.name) private readonly logger: PinoLogger,
  ) {
    const kafka = new Kafka({
      clientId: `${config.kafka.clientId}-lag-monitor`,
      brokers: config.kafka.brokers,
      logLevel: logLevel.WARN,
      logCreator: kafkaLogCreator(logger),
    });
    // Без ретраев: следующий опрос через 5 с и есть ретрай; висящий запрос только копит опросы.
    this.admin = kafka.admin({ retry: { retries: 0 } });
    const registers = [registry];
    this.lagGauge = new prom.Gauge({
      name: 'kafka_consumergroup_lag',
      help: 'High watermark минус закоммиченный offset группы, по партиции (измерено через admin API)',
      labelNames: ['group', 'topic', 'partition'],
      registers,
    });
    this.lagSumGauge = new prom.Gauge({
      name: 'kafka_consumergroup_lag_sum',
      help: 'Суммарный lag группы по всем партициям',
      labelNames: ['group'],
      registers,
    });
    this.membersGauge = new prom.Gauge({
      name: 'kafka_consumergroup_members',
      help: 'Участников в группе; 0 у наблюдаемой группы — никто не читает',
      labelNames: ['group', 'state'],
      registers,
    });
    // Как kafka_topic_partition_current_offset у kafka-exporter. Нужен для DLQ: рост DLQ меряем
    // по самому топику, а не счётчиком в forecast — счётчик умирает вместе с процессом, и если тот
    // упал между записью в DLQ и скрейпом, Prometheus прироста не увидит.
    this.topicOffsetGauge = new prom.Gauge({
      name: 'kafka_topic_partition_current_offset',
      help: 'High watermark партиции топика (сколько сообщений записано за всё время)',
      labelNames: ['topic', 'partition'],
      registers,
    });
    this.upGauge = new prom.Gauge({
      name: 'wfm_lag_monitor_up',
      help: '1 — последний опрос брокера успешен; 0 — брокер недоступен, цифры lag устарели',
      registers,
    });
  }

  async onApplicationBootstrap(): Promise<void> {
    // Не ждём connect: брокер недоступен на старте — монитор поднимется со следующим опросом,
    // а gateway (его WS и /health) от этого не зависит.
    this.timer = setInterval(() => void this.poll(), this.config.lag.pollMs);
    void this.poll();
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.admin.disconnect();
  }

  snapshot() {
    return this.last;
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    try {
      await this.admin.connect(); // идемпотентно: уже подключён — no-op
      const { groups: all } = await this.admin.listGroups();
      const described = await this.admin.describeGroups(all.map((g) => g.groupId));
      const stateOf = new Map(described.groups.map((g) => [g.groupId, g]));
      const groupIds = selectGroups(
        all.map((g) => ({ groupId: g.groupId, state: stateOf.get(g.groupId)?.state ?? 'Unknown' })),
        {
          watched: this.config.lag.watchedGroups,
          prefixes: [`${CONSUMER_GROUPS.DASHBOARD_GATEWAY_PREFIX}-`],
        },
      );

      const views: GroupLagView[] = [];
      const watermarks = new Map<string, TopicWatermarks>();
      // DLQ мерим всегда, даже если его никто не читает (обычно так и есть).
      for (const topic of Object.values(DLQ_TOPICS)) {
        watermarks.set(topic, { topic, partitions: await this.admin.fetchTopicOffsets(topic) });
      }
      for (const groupId of groupIds) {
        const committed = await this.admin.fetchOffsets({ groupId });
        for (const { topic } of committed) {
          if (!watermarks.has(topic)) {
            const offs = await this.admin.fetchTopicOffsets(topic);
            watermarks.set(topic, { topic, partitions: offs });
          }
        }
        const d = stateOf.get(groupId);
        views.push(
          groupLagView(
            groupId,
            d?.state ?? 'Dead',
            d?.members.length ?? 0,
            partitionLags(committed, [...watermarks.values()]),
          ),
        );
      }
      this.publish(views, [...watermarks.values()]);
    } catch (err) {
      if (this.upGaugeValue !== 0) this.logger.warn({ err }, 'lag monitor: broker unreachable');
      this.upGauge.set(0);
      this.upGaugeValue = 0;
    } finally {
      this.polling = false;
    }
  }

  private publish(groups: GroupLagView[], topics: TopicWatermarks[]): void {
    this.topicOffsetGauge.reset();
    for (const t of topics) {
      for (const p of t.partitions) {
        this.topicOffsetGauge.set(
          { topic: t.topic, partition: String(p.partition) },
          Number(p.high),
        );
      }
    }
    // reset: группа исчезла (экземпляр gateway остановлен) — её ряды не должны висеть вечно.
    this.lagGauge.reset();
    this.lagSumGauge.reset();
    this.membersGauge.reset();
    for (const g of groups) {
      this.lagSumGauge.set({ group: g.group }, g.totalLag);
      this.membersGauge.set({ group: g.group, state: g.state }, g.members);
      for (const p of g.partitions) {
        this.lagGauge.set(
          { group: g.group, topic: p.topic, partition: String(p.partition) },
          p.lag,
        );
      }
    }
    if (this.upGaugeValue === 0) this.logger.info('lag monitor: broker reachable again');
    this.upGauge.set(1);
    this.upGaugeValue = 1;
    this.last = { measuredAt: new Date().toISOString(), groups };
    this.ws.broadcast({ type: 'lag', ...this.last });
  }
}
