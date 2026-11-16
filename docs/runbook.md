# Запуск и эксперименты

## Запуск (≈5 минут)
```bash
nvm use                 # Node 24 из .nvmrc
corepack enable         # один раз: yarn 4 из packageManager
cp .env.example .env
yarn install
yarn infra:up           # Kafka, Kafka UI, PostgreSQL, Prometheus; создаёт топики (идемпотентно)
yarn test               # unit-тесты (vitest по исходникам, build не нужен)
yarn dev                # simulator :3001, gateway :3002, forecast :3003, dashboard :5173
# в другом терминале:
yarn smoke              # ✓ … received via WS: partition=… offset=… e2e=… ms
```
Топики: `interaction.started` и `interaction.ended` (по 3 партиции), `forecast-service.dlq`.

Где смотреть: дашборд http://localhost:5173 · Kafka UI http://localhost:8080 ·
Prometheus http://localhost:9090 (вкладка Alerts) · `curl localhost:3002/stats` · `curl localhost:3003/stats`.

Интеграционные тесты на PostgreSQL (нужен поднятый `yarn infra:up`):
```bash
FORECAST_TEST_DATABASE_URL=postgres://wfm:wfm@localhost:5432/wfm yarn test
```

Частые проблемы при старте: `infra:up` не дожидается healthy (мало памяти у Docker, занят порт 9092 или 5432);
сервис падает с `Invalid configuration` (ошибка в `.env`) или `KafkaJSConnectionError` (Kafka ещё не готова).

`yarn infra:down` останавливает стенд и **стирает данные** Kafka и PostgreSQL (томов нет намеренно).

## Эксперименты
Перед каждым стоит сформулировать ожидание, потом проверить.

Группа gateway по умолчанию — своя на каждый экземпляр: `dashboard-gateway-<host>-<port>`
(точное имя — в логе `joined consumer group` и в `curl localhost:3002/stats`).

### 1. Kafka — лог, а не очередь
Kafka UI → Topics → `interaction.started` → Messages. Сообщения уже прочитаны gateway и forecast, но лежат на месте.
Consumers → `dashboard-gateway-…-3002` и `forecast-service`: у каждой группы свой офсет на каждую партицию.
**Вывод:** «прочитано» — свойство группы (её офсет), а не сообщения. Удаление — только по retention.

### 2. Развязка во времени и lag
Остановить forecast: `yarn dev` → Ctrl+C, затем в разных терминалах `yarn dev:sim` и `yarn dev:gw`.
Simulator продолжает писать. Kafka UI → Consumers → `forecast-service` → **lag растёт**; на дашборде — панель
«Consumer lag» (lag меряет gateway снаружи групп); через минуту в Prometheus загорается `WfmConsumerGroupEmpty`.
Поднять forecast (`yarn dev:fc`): в логах `joined consumer group`, затем догоняет с закоммиченного офсета, lag → 0.
**Вывод:** продюсер не знает о консьюмерах; простой консьюмера = lag, а не потеря данных (пока не истёк retention).

### 3. `fromBeginning` — только для новой группы
```bash
GATEWAY_FROM_BEGINNING=true yarn dev:gw             # ничего не меняется: у группы уже есть офсеты
GATEWAY_GROUP_ID=replay-1 GATEWAY_FROM_BEGINNING=true GATEWAY_PORT=3012 yarn workspace @wfm/realtime-gateway dev
```
Вторая команда — новая группа → прочитает всю историю топика.

### 4. Ключ → партиция
В логах simulator: `event published … key, partition, offset`. На дашборде — «По партициям» и колонка p/offset.
```bash
curl -X POST -H 'x-correlation-id: exp-4' 'localhost:3001/simulator/burst?count=30'
```
Ключ у каждого события свой (`interactionId`) → разлетаются по 3 партициям. Офсеты растут **независимо в каждой партиции**.
**Вывод:** порядок есть только внутри партиции; одинаковый ключ → одна партиция → порядок для одного обращения.

### 5. Fan-out: две группы — две независимые копии потока
```bash
GATEWAY_PORT=3012 yarn workspace @wfm/realtime-gateway dev
```
Второй экземпляр получает свою группу (`…-3012`) и **все** события — так же, как gateway и forecast независимо
читают один топик. Simulator при этом не меняется.

### 6. Ребаланс внутри одной группы
Два экземпляра с **одной** группой:
```bash
GATEWAY_GROUP_ID=gw-shared yarn dev:gw
GATEWAY_GROUP_ID=gw-shared GATEWAY_PORT=3012 yarn workspace @wfm/realtime-gateway dev
```
В логах обоих: `group rebalancing` → `joined consumer group` с `assignment` — 3 партиции поделились 2+1
(`curl localhost:3002/stats` и `:3012/stats`). Остановить один — второй забирает все 3.
Четыре экземпляра — один останется без партиций. **Число партиций — потолок параллелизма группы.**
Побочный эффект: клиенты дашборда каждого экземпляра видят только его партиции — поэтому по умолчанию группа
на экземпляр (ADR 2026-11-08).

### 7. correlationId сквозь систему
```bash
curl -X POST -H 'x-correlation-id: trace-me' 'localhost:3001/simulator/burst?count=1'
```
Найти `trace-me` в логах simulator (HTTP-запрос + `event published`), gateway (`event forwarded to ws`) и forecast.
correlationId идёт через HTTP-заголовок, заголовок Kafka-сообщения `x-correlation-id` и поле каждого лога.
