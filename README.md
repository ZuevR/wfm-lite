# WFM-Lite

Event-driven система планирования нагрузки контакт-центра (WFM, workforce management) на Kafka и Node.js.

Симулятор генерирует входящие обращения: чаты и звонки по очередям. События идут через Kafka в два независимых
потребителя. Первый — realtime-дашборд через WebSocket. Второй — сервис прогноза: он складывает обращения
в PostgreSQL, считает нагрузку по 15-минутным интервалам и строит простой прогноз.

Прогноз здесь намеренно простой (moving average). Основная часть работы — то, что обычно ломается в реальных
event-driven системах: семантика доставки, порядок и дубли, ошибки в консьюмере, потеря событий у продюсера,
наблюдаемость. Решения и компромиссы записаны в виде ADR в [docs/wfm-lite-spec.md](docs/wfm-lite-spec.md).

## Архитектура

```mermaid
flowchart LR
    SIM["channel-simulator<br/>:3001"]
    subgraph K["Kafka (KRaft)"]
        T1[["interaction.started"]]
        T2[["interaction.ended"]]
        DLQ[["forecast-service.dlq"]]
    end
    GW["realtime-gateway<br/>:3002"]
    FC["forecast-service<br/>:3003"]
    PG[("PostgreSQL")]
    UI["dashboard<br/>:5173"]
    PROM["Prometheus<br/>:9090"]

    SIM --> T1 & T2
    T1 -- "group: dashboard-gateway-{host}-{port}" --> GW
    T1 & T2 -- "group: forecast-service" --> FC
    FC -- "сообщение не обработать" --> DLQ
    FC --> PG
    GW -- "WebSocket /ws" --> UI
    GW -. "lag групп (admin API)" .-> K
    PROM -. "scrape /metrics" .-> SIM
    PROM -.-> GW
    PROM -.-> FC
```

| Сервис | Что делает |
|---|---|
| `channel-simulator` | Генерирует обращения (`interaction.started`, затем `interaction.ended`) с настраиваемым темпом; часть событий намеренно приходит не по порядку. HTTP: `GET /simulator`, `POST /simulator/start`, `/stop`, `/burst?count=N`. |
| `realtime-gateway` | BFF для дашборда: читает поток и рассылает его в WebSocket. У каждого экземпляра своя consumer group, потому что каждому нужен весь поток. Здесь же lag-монитор: через admin API брокера меряет lag и число участников всех групп. |
| `forecast-service` | Одна общая consumer group делит работу между экземплярами. Идемпотентно пишет обращения в PostgreSQL. `GET /forecast?queue=&channel=&hours=&window=&horizon=` возвращает факт по интервалам и прогноз, `GET /interactions/:id` — одно обращение. |
| `dashboard` | React + Zustand: KPI, распределение по партициям и очередям, consumer lag, лента последних событий. |
| `packages/contracts` | Единственный источник схем: имена топиков и групп, заголовки, конверт события, zod-схемы, протокол WebSocket. |
| `packages/service-kit` | Общая обвязка сервисов: JSON-логи, `/metrics`, супервизор консьюмера, хелперы конфига. |

У каждого сервиса есть `/health` (liveness), `/ready` (readiness: Kafka-клиент подключён или в группе, БД отвечает)
и `/metrics`.

## Стек

- **Backend:** TypeScript, Node.js 24, NestJS 11, KafkaJS (без `@nestjs/microservices`), zod, pino.
- **Хранилище:** PostgreSQL 17, `pg`, `node-pg-migrate`.
- **Frontend:** React 19, Zustand, Vite; WebSocket на библиотеке `ws`.
- **Инфраструктура:** Apache Kafka 3.9 (KRaft, один брокер), Kafka UI, Prometheus 3.5, Docker Compose.
- **Инструменты:** Yarn 4 workspaces (монорепа), vitest, ESLint, Prettier, `promtool` для тестов правил алертов.

## Запуск локально

Нужны Node.js 24 и Docker (стенд проверялся на Docker Desktop с 8 ГБ памяти).

```bash
corepack enable            # поставит yarn нужной версии из packageManager
cp .env.example .env
yarn install
yarn infra:up              # Kafka, Kafka UI, PostgreSQL, Prometheus; создаёт топики
yarn dev                   # все сервисы и дашборд в watch-режиме
```

В соседнем терминале:

```bash
yarn smoke                 # e2e: HTTP → Kafka → WebSocket и Kafka → PostgreSQL
curl -X POST localhost:3001/simulator/start
```

- Дашборд: http://localhost:5173
- Kafka UI: http://localhost:8080
- Prometheus: http://localhost:9090

Проверки:

```bash
yarn test                  # unit-тесты
FORECAST_TEST_DATABASE_URL=postgres://wfm:wfm@localhost:5432/wfm yarn test   # + интеграционные на PostgreSQL
yarn typecheck && yarn lint
yarn infra:alerts:test     # тесты правил алертов (promtool в Docker)
```

`yarn infra:down` останавливает стенд и стирает данные Kafka и PostgreSQL.
Эксперименты с партициями, consumer groups, ребалансом и lag описаны в [docs/runbook.md](docs/runbook.md).

## Что реализовано

**Контракты событий**
- Конверт `eventId, eventType, version, occurredAt, correlationId, payload`. Продюсер валидирует событие перед
  отправкой, консьюмер — после получения (`safeParse`, без исключений).
- Схема меняется только с повышением `version`: новая версия — новая схема рядом.
- Ключ сообщения — `interactionId`, поэтому все события одного обращения попадают в одну партицию по порядку.
- Метаданные дублируются в заголовках Kafka (`x-correlation-id`, `event-type`, `event-version`): по ним можно
  логировать, не разбирая value.

**Надёжность консьюмеров**
- Ошибки делятся по причине.
  - Виновато окружение (БД, сеть, Kafka) — блокирующий повтор с backoff и `heartbeat()` внутри `eachMessage`:
    партиция ждёт, offset не двигается, ребаланса нет.
  - Виновато сообщение (невалидная схема, SQLSTATE 22/23) — сразу в DLQ.
  - Неизвестная ошибка — ограниченное число повторов, затем DLQ.
- Самоперезапуск консьюмера. Если KafkaJS после CRASH сам не перезапускается (`restart=false`), сервис
  переподключается сам с backoff. Иначе процесс оставался бы живым, а консьюмер — мёртвым.
- Graceful shutdown: при SIGTERM консьюмер покидает группу и коммитит offset'ы до остановки процесса.

**DLQ**
- DLQ заводится на consumer group, а не на топик: `forecast-service.dlq`. Ошибка — свойство пары
  «сообщение × потребитель».
- В DLQ уходят исходные key и value без изменений. Добавляются заголовки: исходные топик, партиция и offset,
  причина (`invalid` / `rejected` / `unknown`), текст ошибки и число попыток. Сообщение можно разобрать
  и отправить повторно (replay).
- Рост DLQ меряется по high watermark самого топика, а не счётчиком в процессе: счётчик теряется, если процесс
  упал до scrape.

**Идемпотентность и порядок**
- Доставка at-least-once, консьюмер в forecast идемпотентный. Upsert по `interaction_id` с условием по
  `eventId`: повтор того же события не меняет строку.
- `started` и `ended` пишут непересекающиеся колонки, поэтому результат не зависит от порядка прихода.
- Дашборд отсекает дубли по `eventId`.
- Продюсер работает в режиме `idempotent: true` с `acks=all`. Повтор отправки идёт тем же батчем, и брокер
  отсекает дубль.

**Продюсер и backpressure**
- Отправка идёт через ограниченный буфер в памяти с бесконечными повторами. Простой Kafka короче,
  чем «ёмкость буфера / темп», не теряет событий.
- Полный буфер даёт явный отказ: `BufferFullError` или HTTP 503 с `Retry-After`. Каждый отказ посчитан
  в метриках, ничего не теряется молча.
- Ретраемость ошибки KafkaJS определяется по `cause`, а не по классу-обёртке.

**Наблюдаемость**
- JSON-логи (pino) с `correlationId` во всех сервисах, у Kafka-событий — ещё `topic/partition/offset`.
  correlationId проходит через HTTP-заголовок, заголовок Kafka-сообщения и WebSocket.
- Метрики Prometheus: исходы обработки, повторы, DLQ, время блокировки консьюмера, состояние буфера продюсера,
  WebSocket-клиенты. Kafka-метрики названы как у kafka-exporter.
- Consumer lag и число участников групп меряются снаружи групп, через admin API. Изнутри lag замирает ровно тогда,
  когда консьюмер встал. Те же данные показывает панель на дашборде.
- 11 правил алертов: сервис или БД недоступны, группа пуста, lag, блокировка, DLQ, crash loop, заполнение буфера
  и другие. У каждого правила есть тест `promtool test rules`.

**Тесты**
- vitest: unit-тесты на чистую логику без I/O (схемы, обработчики сообщений, политика ошибок, буфер, супервизор,
  расчёт lag и прогноза) плюс интеграционные тесты репозитория на реальном PostgreSQL.
- `yarn smoke` проверяет весь путь события на поднятом стенде.

## Структура репозитория

```
apps/
  channel-simulator/src/   config/ health/ kafka/ simulator/
  realtime-gateway/src/    config/ health/ kafka/ ws/ lag/
  forecast-service/src/    config/ health/ kafka/ database/ forecast/    + migrations/
  dashboard/src/           components/ hooks/ store/ utils/
packages/
  contracts/src/           events/ kafka/ ws/
  service-kit/src/         config/ logging/ metrics/ kafka/
infra/                     docker-compose.yml, kafka/create-topics.sh, prometheus/
scripts/smoke.mjs
docs/                      wfm-lite-spec.md (схемы событий, ADR), runbook.md
```

В NestJS-сервисах код разложен по зонам ответственности:
- `kafka/` владеет Kafka-клиентом и его lifecycle;
- доменные папки содержат API и расчёты;
- `database/` — пул, миграции и репозиторий.

Чистая логика без I/O вынесена в отдельные файлы, тесты лежат рядом с кодом.

## В планах

- **scheduling-service:** построение смен по прогнозу. Здесь появится Transactional Outbox: запись в БД
  и публикация события — одна бизнес-операция.
- **JWT между сервисами:** service-to-service аутентификация и guards в NestJS.
- **CI на GitHub Actions:** build → test → docker.

---

Проект сделан вместе с AI-агентом (Claude Code); архитектурные решения и структуру кода принимал и разбирал сам.
