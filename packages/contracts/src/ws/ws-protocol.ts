import type { InteractionStartedV1 } from '../events/interaction-started';

/**
 * Протокол WebSocket gateway → браузер. Отдельный от Kafka-конверта: фронту нужны ещё и
 * метаданные доставки (партиция, офсет), а Kafka-событие мы не меняем ради UI.
 */
export interface KafkaDeliveryMeta {
  topic: string;
  partition: number;
  offset: string;
  gatewayReceivedAt: string;
}

/** Lag одной партиции: high watermark − закоммиченный offset группы. */
export interface PartitionLagView {
  topic: string;
  partition: number;
  /** null — группа ещё ничего не коммитила в эту партицию (lag = вся удерживаемая история). */
  committed: string | null;
  high: string;
  lag: number;
}

/**
 * Состояние consumer group, измеренное СНАРУЖИ (admin API: offsets группы и high watermark топика).
 * Так видно и консьюмер, который умер и сам о себе ничего не сообщит.
 */
export interface GroupLagView {
  group: string;
  /** Stable / PreparingRebalance / CompletingRebalance / Empty / Dead — как отдаёт брокер. */
  state: string;
  members: number;
  totalLag: number;
  partitions: PartitionLagView[];
}

export type WsServerMessage =
  | { type: 'hello'; serverTime: string; consumerGroup: string }
  | { type: 'event'; event: InteractionStartedV1; meta: KafkaDeliveryMeta }
  | { type: 'lag'; measuredAt: string; groups: GroupLagView[] };

export type WsEventMessage = Extract<WsServerMessage, { type: 'event' }>;
