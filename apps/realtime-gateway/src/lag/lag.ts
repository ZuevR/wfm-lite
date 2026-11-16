import type { GroupLagView, PartitionLagView } from '@wfm/contracts';

/**
 * Lag consumer group — чистая функция, без Kafka. Входы — то, что отдаёт admin API:
 *   fetchTopicOffsets(topic)  → high (следующий offset, который получит новое сообщение) и low;
 *   fetchOffsets({ groupId }) → закоммиченный offset группы ('-1' — коммитов не было).
 * lag = high − committed: сколько сообщений группа ещё не подтвердила. Не «не прочитала»:
 * консьюмер мог прочитать, но не успеть закоммитить (autoCommit — раз в 5 с / после батча).
 */
export interface TopicWatermarks {
  topic: string;
  partitions: { partition: number; high: string; low: string }[];
}

export interface GroupCommitted {
  topic: string;
  partitions: { partition: number; offset: string }[];
}

export function partitionLags(
  committed: GroupCommitted[],
  watermarks: TopicWatermarks[],
): PartitionLagView[] {
  const byTopic = new Map(watermarks.map((w) => [w.topic, w.partitions]));
  const result: PartitionLagView[] = [];
  for (const { topic, partitions } of committed) {
    const marks = byTopic.get(topic);
    if (!marks) continue; // топик удалён — пропускаем, а не падаем
    for (const m of marks) {
      const c = partitions.find((p) => p.partition === m.partition)?.offset;
      const hasCommit = c !== undefined && c !== '-1';
      // Без коммита считаем от low: всё, что ещё хранится, группой не подтверждено.
      const from = BigInt(hasCommit ? c : m.low);
      const lag = BigInt(m.high) - from;
      result.push({
        topic,
        partition: m.partition,
        committed: hasCommit ? c : null,
        high: m.high,
        // Отрицательный lag бывает, если retention удалил сообщения раньше, чем группа их прочла
        // (committed < low) — тогда это не «отставание», а потеря; показываем 0, флаг — committed.
        lag: lag < 0n ? 0 : Number(lag),
      });
    }
  }
  return result.sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);
}

export function groupLagView(
  group: string,
  state: string,
  members: number,
  partitions: PartitionLagView[],
): GroupLagView {
  return {
    group,
    state,
    members,
    totalLag: partitions.reduce((sum, p) => sum + p.lag, 0),
    partitions,
  };
}

/**
 * Какие группы мерить. Всегда — «наблюдаемые» (forecast-service): их пустота — инцидент.
 * Группы gateway (по одной на экземпляр) — только живые: у остановленного экземпляра группа
 * остаётся в __consumer_offsets до offsets.retention (7 дней) с вечно растущим lag — это шум.
 */
export function selectGroups(
  all: { groupId: string; state: string }[],
  opts: { watched: string[]; prefixes: string[] },
): string[] {
  const live = all
    .filter((g) => opts.prefixes.some((p) => g.groupId.startsWith(p)))
    .filter((g) => g.state !== 'Empty' && g.state !== 'Dead')
    .map((g) => g.groupId);
  return [...new Set([...opts.watched, ...live])].sort();
}
