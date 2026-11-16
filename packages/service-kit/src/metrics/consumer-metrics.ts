import type { Consumer } from 'kafkajs';
import * as client from 'prom-client';

/**
 * Общие метрики Kafka-консьюмера (forecast, gateway). Видно ИЗНУТРИ процесса: что он обработал,
 * сколько падал и перевступал. Lag и «группа пуста» меряем СНАРУЖИ (lag-monitor в gateway):
 * мёртвый консьюмер про себя ничего не сообщит.
 */
export interface ConsumerMetrics {
  consumed: client.Counter<'topic' | 'result'>;
  processingSeconds: client.Histogram<'topic'>;
}

export function createConsumerMetrics(
  registry: client.Registry,
  opts: { consumer: Consumer; groupId: string; isInGroup: () => boolean },
): ConsumerMetrics {
  const { consumer, groupId } = opts;
  new client.Gauge({
    name: 'wfm_consumer_in_group',
    help: '1 — консьюмер в группе и получил партиции; 0 — вступает, ребалансится или упал',
    labelNames: ['group'],
    registers: [registry],
    collect() {
      this.set({ group: groupId }, opts.isInGroup() ? 1 : 0);
    },
  });
  const crashes = new client.Counter({
    name: 'wfm_consumer_crashes_total',
    help: 'Падения консьюмера KafkaJS (CRASH); restart=false — перезапускает наш супервизор',
    labelNames: ['group', 'restart'],
    registers: [registry],
  });
  const rebalances = new client.Counter({
    name: 'wfm_consumer_rebalances_total',
    help: 'Ребалансы группы, замеченные этим экземпляром',
    labelNames: ['group'],
    registers: [registry],
  });
  const consumed = new client.Counter({
    name: 'wfm_messages_consumed_total',
    help: 'Обработанные сообщения по исходу (result: stored, duplicate, forwarded, dead_lettered, invalid)',
    labelNames: ['topic', 'result'],
    registers: [registry],
  });
  const processingSeconds = new client.Histogram({
    name: 'wfm_message_processing_seconds',
    help: 'Время обработки одного сообщения в eachMessage, включая повторы',
    labelNames: ['topic'],
    // от 1 мс до ~1 мин: обычная запись — единицы мс, повторы при простое БД — секунды и больше
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60],
    registers: [registry],
  });

  const { CRASH, REBALANCING } = consumer.events;
  consumer.on(CRASH, ({ payload }) =>
    crashes.inc({ group: groupId, restart: String(payload.restart) }),
  );
  consumer.on(REBALANCING, () => rebalances.inc({ group: groupId }));

  return { consumed, processingSeconds };
}
