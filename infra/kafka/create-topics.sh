#!/bin/sh
# Идемпотентно создаёт топики. Запускается внутри контейнера kafka: `yarn infra:topics`.
# Формат строки: <имя> <партиции> [--config k=v ...]
set -eu
BOOTSTRAP=kafka:19092
BIN=/opt/kafka/bin

# 3 партиции: достаточно, чтобы увидеть распределение по ключам и ребаланс между 2–3 консьюмерами.
# Число партиций — потолок параллелизма consumer group: консьюмеров больше, чем партиций, — лишние простаивают.
# interaction.ended — столько же партиций и тот же ключ (interactionId), что у interaction.started:
# co-partitioning — события одного обращения из обоих топиков приходят в партицию с одним номером,
# а значит, к одному консьюмеру группы. Порядка МЕЖДУ топиками это не даёт (это разные логи).
#
# forecast-service.dlq — DLQ на consumer group (ADR 2026-11-12). Те же 3 партиции и исходный ключ:
# сообщения одного обращения в DLQ лежат по порядку. Retention 14 дней (по умолчанию 7): разбор
# DLQ — ручная работа, не должна сгорать за выходные + праздники.
TOPICS="
interaction.started 3
interaction.ended 3
forecast-service.dlq 3 --config retention.ms=1209600000
"

echo "$TOPICS" | while read -r name partitions extra; do
  [ -z "$name" ] && continue
  # shellcheck disable=SC2086 — extra намеренно без кавычек: это список аргументов
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 $extra
done

echo "--- topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
