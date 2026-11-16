# WFM-Lite — спецификация

MVP системы планирования нагрузки контакт-центра (WFM) на event-driven архитектуре. Цель — на правдоподобном домене отработать реальные компромиссы Kafka: доставку, обработку ошибок, наблюдаемость.

## Стек
TypeScript / Node.js + **NestJS**, KafkaJS, PostgreSQL, React + Zustand, WebSocket, Docker. В планах: react-hook-form (формы Scheduling), GitHub Actions (CI).

## Модули (минимум по одному сервису на модуль)

### Channel Simulator
Генерирует входящие события чатов/звонков, публикует в Kafka.
Топики: `interaction.started`, `interaction.ended`, `operator.status.changed`.

### Realtime Gateway
BFF для дашборда: консьюмер Kafka (своя группа на экземпляр `dashboard-gateway-<host>-<port>`, ADR 2026-11-08) → WebSocket `/ws`. Без состояния.

### Forecast Service
Консьюмер. Агрегирует нагрузку по 15-минутным интервалам, простой прогноз (moving average / линейная регрессия), сохраняет в PostgreSQL.
Реализация: NestJS :3003. Группа `forecast-service` (общая), читает `interaction.started` + `interaction.ended`, upsert в `interactions` (PostgreSQL 17, `pg` + `node-pg-migrate`). API: `GET /forecast?queue=&channel=&hours=&window=&horizon=` (факт по 15-мин интервалам + скользящее среднее), `GET /interactions/:id`, `/stats`.

### Scheduling Service _(в планах)_
Строит смены на основе прогноза. Упрощённо: жадный алгоритм с ограничениями квалификации/доступности.

### Dashboard
React + WebSocket: текущая загрузка, отклонения от плана, алерты по lag/просрочкам.

### Auth _(в планах)_
JWT между сервисами (mock service-to-service), минимальный authn/authz.

### Observability
Логи с correlation id, метрики (минимум Prometheus-формат), health/readiness.
Реализовано: `/metrics` в каждом сервисе (`@wfm/service-kit` → `MetricsModule`), lag групп и watermark топиков снаружи (lag-монитор в gateway),
Prometheus :9090 с правилами алертов `infra/prometheus/alerts.yml` и их тестами (`yarn infra:alerts:test`). Alertmanager и Grafana — не подняты.

## Принцип
MVP, но по-настоящему: реальные проблемы, реальные компромиссы, реальная наблюдаемость.

## Схемы событий
Источник истины — `packages/contracts` (zod). Здесь — человекочитаемое описание.

**Конверт (все события):**

| Поле | Тип | Смысл |
|---|---|---|
| `eventId` | uuid | уникален для события; ключ дедупликации (идемпотентность) |
| `eventType` | string | = имя топика |
| `version` | int | версия схемы payload |
| `occurredAt` | ISO-8601 | когда случилось в домене (не время записи в Kafka) |
| `correlationId` | string ≤128 | сквозной id бизнес-цепочки |
| `payload` | object | см. событие |

Заголовки Kafka: `x-correlation-id`, `event-type`, `event-version`.

### `interaction.started` v1
- **Продюсер:** channel-simulator. **Консьюмеры:** realtime-gateway (`dashboard-gateway-<instance>`), forecast-service (`forecast-service`).
- **Партиции:** 3. **Ключ:** `payload.interactionId` — порядок событий одного обращения (started → ended).
- **Payload:** `interactionId` (uuid), `channel` (`chat` | `voice`), `queue` (string ≤64), `customerSegment` (`retail` | `premium` | `business`).

### `interaction.ended` v1
- **Продюсер:** channel-simulator (через handle time 30–300 с после started; ~5% — ПЕРЕД started). **Консьюмеры:** forecast-service.
- **Партиции:** 3. **Ключ:** `payload.interactionId` — co-partitioning с `interaction.started` (тот же ключ и число партиций → одно обращение попадает к одному консьюмеру группы). Порядка между топиками нет — консьюмер обязан быть к нему нечувствителен.
- **Payload:** `interactionId` (uuid), `outcome` (`resolved` | `abandoned`). Время завершения — `occurredAt`.

### `forecast-service.dlq` (служебный)
- **Продюсер:** forecast-service (сообщения, которые нельзя обработать никогда). **Консьюмеры:** человек / будущий replay-скрипт.
- **Партиции:** 3, retention 14 дней. **Ключ:** исходный (`interactionId`) — сообщения одного обращения лежат по порядку.
- **Value:** исходное сообщение **байт в байт** (replay = отправить обратно как есть). Исходные заголовки сохраняются.
- **Заголовки DLQ** (`DLQ_HEADERS` в contracts): `dlq-original-topic/partition/offset/timestamp`, `dlq-consumer-group`,
  `dlq-reason` (`invalid` — не прошло схему; `rejected` — БД отвергла данные, SQLSTATE 22/23; `unknown` — неизвестная ошибка после N попыток),
  `dlq-error` (≤1000 символов), `dlq-attempts`, `dlq-failed-at`.

## Архитектурные решения (ADR-лайт)
_(формат: дата — решение — альтернативы — почему так)_

- **2026-11-05 — Kafka-клиент: KafkaJS.** Альтернатива: `@confluentinc/kafka-javascript` (поддерживается Confluent, librdkafka, KafkaJS-совместимый API). Почему: чистый JS без нативной сборки, больше материалов; механика групп/офсетов та же. Риск: KafkaJS не развивается с 2023 → путь миграции на confluent-клиент дешёвый (совместимый API).
- **2026-11-05 — KafkaJS напрямую, без `@nestjs/microservices` Kafka transport.** Альтернатива: транспорт Nest с `@EventPattern`. Почему: транспорт прячет группу, офсеты, коммиты и ребаланс — а это ровно то, что нужно понимать. Цена: lifecycle (connect/disconnect, shutdown hooks) пишем сами.
- **2026-11-05 — Отдельный Realtime Gateway (BFF), а не консьюмер внутри Forecast.** Почему: разная нагрузка и отказы (WS-клиенты vs расчёты), независимое масштабирование, наглядный fan-out (две группы на один топик).
- **2026-11-05 — WebSocket на `ws` (WsAdapter), не socket.io.** Почему: стандартный протокол, видна механика (ping/pong heartbeat, reconnect с backoff на клиенте). Цена: rooms/ack/fallback'и socket.io — пишем сами, если понадобятся.
- **2026-11-05 — Схемы: JSON + zod в общем пакете contracts.** Альтернатива: Avro/Protobuf + Schema Registry. Почему: быстрый старт, типы TS из схем; совместимость версий проверяем дисциплиной (`version`). К Schema Registry вернёмся на теме schema evolution.
- **2026-11-05 — Невалидные сообщения: лог + пропуск (временно).** Альтернатива: бросить исключение (KafkaJS будет ретраить → партиция встанет), DLQ. Почему: DLQ на тот момент не было; бесконечный ретрай poison message хуже пропуска. _2026-11-12: для forecast заменено на DLQ (см. ниже). Gateway по-прежнему пропускает: это realtime-витрина, потеря одного события на экране допустима, а DLQ без потребителя — мусор._
- **2026-11-05 — Пакетный менеджер: Yarn 4 (Berry), `nodeLinker: node-modules`.** Альтернативы: npm workspaces (было), pnpm, Yarn PnP. Почему: `workspace:*` протокол, `yarn workspace X <cmd>`, строгий lockfile, версия yarn закреплена в `packageManager` (corepack); безопасные дефолты 4.x — install-скрипты выключены, `npmMinimalAgeGate` (свежие версии не ставятся сутки). node-modules вместо PnP: Nest/Vite/vitest/eslint без патчей и IDE-SDK; цена — возможны «фантомные» зависимости.
- **2026-11-05 — Стенд: 1 брокер KRaft, RF=1, без томов, автосоздание топиков выключено.** Почему: минимум ресурсов; явное создание топиков защищает от опечаток. На проде: ≥3 брокера, RF=3, `min.insync.replicas=2`.
- **2026-11-08 — Consumer group gateway: общая группа `dashboard-gateway` — ошибка при >1 экземпляре. Принято (A).** Наблюдение: со 2-м экземпляром партиции делятся, и WS-клиенты каждого экземпляра видят только его партиции (`wsDelivered: 0` на p2 у экземпляра без клиентов). Правило: общая группа — когда работу нужно поделить (Forecast); группа на экземпляр — когда каждому нужен весь поток (WS-рассылка, кэши). Варианты: (A) `dashboard-gateway-<instanceId>` + чтение с конца — просто, но каждый экземпляр читает весь топик, а в `__consumer_offsets` копятся мёртвые группы (лечится `offsets.retention`); (B) одна группа + Redis pub/sub между экземплярами — один читатель на партицию, но ещё одна инфраструктурная зависимость. Склоняюсь к A для локального стенда. Проверено: два экземпляра → у обоих `[0,1,2]`.
- **2026-11-08 — `interaction.ended` — отдельный топик, консьюмер нечувствителен к порядку.** Альтернатива: один топик `interaction.events` (порядок по ключу из коробки). Почему: не ломаем работающий контракт и конвенцию имён (эволюция, а не переделка); upsert по `interactionId` с непересекающимися колонками коммутативен. Цена: временно «неполные» строки (ended без started), видны в `/stats` как `endedWithoutStarted`.
- **2026-11-08 — Forecast хранит факты, а не счётчики; идемпотентность — upsert + eventId.** Альтернатива: `count + 1` по интервалам (дешевле читать, но дубль at-least-once = +1). `ON CONFLICT ... DO UPDATE ... WHERE event_id IS DISTINCT FROM` → повтор = no-op, видно в логе. Агрегаты — `date_bin('15 minutes')` при чтении.
- **2026-11-08 — PostgreSQL через `pg` + `node-pg-migrate` (SQL-миграции), без ORM.** Альтернативы: TypeORM/Sequelize/Prisma/Drizzle. Почему: транзакции, `ON CONFLICT`, индексы видны как есть. Миграции при старте (advisory lock) — dev-удобство; на проде — отдельный шаг деплоя.
- **2026-11-08 — Порт сервиса открывается до вступления в группу; вступление — только в `/ready`.** `consumer.run()` не ждём (он резолвится после JoinGroup/SyncGroup). Проверено: после `kill -9` `/health` = 200 через 2 с, `/ready` = 503 ~30 с (sessionTimeout зомби), затем 200.
- **2026-11-08 — `/ready` продюсера — по фактам (исход send + проба `describeCluster` каждые 5 с).** Флаг CONNECT/DISCONNECT KafkaJS не видит падения брокера. Проверено: Kafka стоп → 503 через ~10 с, старт → 200 через ~4 с.
- **2026-11-12 — DLQ на consumer group (`forecast-service.dlq`), а не на исходный топик.** Альтернатива: `interaction.started.dlq` (как `<topic>.DLT` в Spring Kafka). Почему: «не могу обработать» — свойство пары сообщение × потребитель; одно событие может быть ядом для forecast и нормой для gateway, а владелец разбора DLQ — команда потребителя. Цена: при N потребителях одного топика — N DLQ; имя `<группа>.dlq` — исключение из конвенции `<сущность>.<событие>` (не доменное событие). Ключ и value исходные, контекст — в заголовках.
- **2026-11-12 — Ошибки делим по причине: окружение / сообщение / неизвестно.** Окружение (сеть, SQLSTATE 08/40/53/55/57/58 и 42, ошибки KafkaJS) → бесконечные повторы, партиция ждёт. Сообщение (невалидная схема, SQLSTATE 22/23) → сразу DLQ. Неизвестно (TypeError и т.п.) → 5 попыток, затем DLQ. Альтернатива: «N попыток для всего, потом DLQ». Почему нет: за минуту простоя БД весь поток уехал бы в DLQ. 42 (нет таблицы) — к окружению: причина — не накатанная миграция, а не сообщение. Цена: баг в коде, ломающий каждое сообщение, уведёт поток в DLQ через `unknown` → нужен алерт на рост DLQ (см. ADR 2026-11-14).
- **2026-11-12 — Повторы — блокирующие, внутри `eachMessage`, с `heartbeat()`; не `pause()` и не retry-топики.** Разбор KafkaJS 2.2.4 (`consumer/runner.js`): `pause()` + возврат без исключения → offset сдвигается, сообщение **потеряно**; исключение → retrier раннера (5 попыток) → CRASH → рестарт консьюмера и ребаланс. Блокирующий повтор: offset стоит, порядок по ключу сохранён, консьюмер в группе (heartbeat каждые ≤3 с при sessionTimeout 30 с). Backoff 200 мс → 10 с с jitter. Альтернатива: retry-топики (`.retry-1m`, `.retry-10m`) — не блокируют партицию, но ломают порядок по ключу и бессмысленны, когда лежит общая зависимость (упадёт всё). Цена: при `partitionsConsumedConcurrently=1` стоит весь консьюмер; остановка сервиса прерывает ожидание (`ProcessingAbortedError`) → сообщение доставится повторно. Отправка в DLQ — тем же циклом (Kafka недоступна → ждём): offset без подтверждённой записи в DLQ не двигается. DLQ-продюсер `idempotent: true`, `acks=-1`.
- **2026-11-12 — Продюсер симулятора: ограниченный буфер в памяти + бесконечные повторы + backpressure; outbox — не здесь.** Было: ретраи KafkaJS (~10–15 с), потом событие терялось молча (at-most-once). Стало: FIFO-буфер (10 000 событий), один воркер, `sendBatch` с повторами 200 мс → 5 с; полный буфер → событие отвергается на входе (`rejected`, HTTP 503 + `Retry-After`). Продюсер `idempotent: true` с `retries: 2` (по умолчанию у idempotent ∞ — send висел бы молча) и «замороженным» батчем: повтор — ровно тот же набор в те же партиции; KafkaJS при ошибке откатывает sequence (`sendMessages.js`) → дубль от повтора брокер отсекает. Партиция вычисляется заранее тем же DefaultPartitioner — в логе `partition/offset` на каждое событие. Альтернатива: Transactional Outbox — переживает и падение процесса, но у симулятора нет БД и бизнес-транзакции, которую надо атомарно связать с событием (dual write); буфер не переживает `kill -9` — для эмулятора источника это допустимо. Outbox — в Scheduling (в планах), где запись в БД и событие — одна бизнес-операция.
- **2026-11-12 — Консьюмер перезапускаем сами после CRASH с `restart=false`.** KafkaJS рестартит только если ИСХОДНАЯ причина retriable (`getOriginalCause(e).retriable`); `KafkaJSGroupCoordinatorNotFound` (координатор не готов после старта брокера) — NonRetriable без cause → консьюмер мёртв навсегда при живом процессе и `/health` 200. `restartOnFailure` для таких ошибок не вызывается. Делаем `connect()` + `run()` с backoff 1 → 30 с (forecast и gateway). Альтернатива: crash-only — `/health` 503 → оркестратор перезапустит под; оставили как следующую ступень (если консьюмер не в группе дольше N минут).
- **2026-11-12 — AHT с окном дозревания; вечно открытые обращения не закрываем выдуманным `ended_at`.** `FORECAST_AHT_MATURITY_SEC` (900 с > максимума handle time): `avgHandleSec` интервала — только когда его конец + окно ≤ now, иначе `null` и `ahtMature: false`. Обращение без ended дольше окна — `unclosed` (производный статус при чтении: в интервале `unclosed = started − ended`, в `/stats` — отдельно от `open`). Альтернатива: фоновое задание проставляет `ended_at = started_at + timeout` / `outcome = timed_out` — искажает факты и AHT, а поздний настоящий ended пришлось бы «перебивать». Цена: AHT по интервалу с `unclosed > 0` всё равно занижен (теряются чаще длинные) — это видно, а не спрятано.
- **2026-11-14 — Общий пакет `@wfm/service-kit`.** Логи (pino + мост KafkaJS), метрики (`MetricsModule`), `ConsumerSupervisor` (самоперезапуск консьюмера, теперь с unit-тестами), хелперы конфига. Правило трёх сработало на `logging.ts` (×3), затем добавилась копия супервизора (×2), метрики стали бы четвёртой. Цена: изменение пакета задевает все сервисы; доменные метрики остаются в сервисах — в kit только общее.
- **2026-11-14 — Метрики: prom-client, свой Registry на процесс, имена по конвенциям Prometheus.** `wfm_*` — наши, `kafka_consumergroup_*` / `kafka_topic_partition_current_offset` — как у kafka-exporter (переносимость алертов и дашбордов). Метки только с ограниченным набором значений (topic, reason, result, group) — никаких eventId/correlationId (кардинальность). Счётчики с известными метками заводим нулём при старте: ряд появляется с первым `inc`, и `increase()` не видит первое событие. Gauge «сколько висим» (`wfm_consumer_blocked_seconds`, `wfm_publish_blocked_seconds`) считается в `collect()` при скрейпе.
- **2026-11-14 — Lag и наличие участников меряем СНАРУЖИ групп: lag-монитор в gateway через admin API.** `lag = high watermark − committed`, `describeGroups` — состояние и число участников. Альтернатива: lag из `END_BATCH_PROCESS` внутри консьюмера (как было) — замирает ровно тогда, когда нужен: консьюмер встал или умер. Прод-аналоги — Burrow, kafka-exporter; свой монитор — чтобы понять механику и отдать тот же снимок в WS (панель дашборда). Группы gateway (по одной на экземпляр) — только живые, иначе мёртвые группы копят lag до `offsets.retention`. Цена: монитор в каждом экземпляре gateway → дубли рядов, в алертах `max by (group)`.
- **2026-11-14 — Алерт на DLQ — по high watermark DLQ-топика, а не по счётчику процесса.** Поймано вживую: forecast записал в DLQ и получил `kill -9` через 3 с — до следующего скрейпа (5 с); `wfm_dlq_messages_total` так и не показал 1, алерт молчал. Топик — источник правды, он прирост помнит. Счётчик оставлен для разбивки по `reason`.
- **2026-11-14 — Prometheus в compose, без Alertmanager и Grafana.** Состояние алертов — в UI Prometheus (`/alerts`). Правила покрыты `promtool test rules` (синтетические ряды → какие алерты горят в момент t). Алерты — про симптом для пользователя («группу никто не читает», «события теряются у источника»), а не про «странную метрику». Scrape 5 с — для стенда; на проде 15–30 с. Alertmanager (маршрутизация, дедуп, тишина) и Grafana — когда понадобится доставка алертов и графики.

