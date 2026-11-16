#!/usr/bin/env node
// Сквозная проверка вертикального среза:
//   HTTP → channel-simulator → Kafka (interaction.started) → realtime-gateway → WebSocket
//                                                         → forecast-service → PostgreSQL
// Шлём 1 событие с уникальным correlationId и ждём ровно его в WS. Без зависимостей (Node ≥ 22:
// встроенные fetch и WebSocket).
const SIM = process.env.SIMULATOR_URL ?? 'http://localhost:3001';
const GW = process.env.GATEWAY_URL ?? 'http://localhost:3002';
const FC = process.env.FORECAST_URL ?? 'http://localhost:3003';
const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS ?? 15_000);
const correlationId = `smoke-${crypto.randomUUID()}`;

const fail = (msg) => {
  console.error(`✗ ${msg}`);
  console.error('  Подсказка: yarn infra:up && yarn dev (в соседнем терминале)');
  process.exit(1);
};

async function checkReady(name, base) {
  try {
    const res = await fetch(`${base}/ready`);
    const body = await res.json();
    if (!res.ok) fail(`${name} not ready: ${JSON.stringify(body)}`);
    console.log(`✓ ${name} ready ${JSON.stringify(body.checks)}`);
  } catch (e) {
    fail(`${name} недоступен на ${base}: ${e.message}`);
  }
}

await checkReady('channel-simulator', SIM);
await checkReady('realtime-gateway', GW);
await checkReady('forecast-service', FC);

const ws = new WebSocket(`${GW.replace(/^http/, 'ws')}/ws`);
const timer = setTimeout(
  () => fail(`событие ${correlationId} не пришло в WS за ${TIMEOUT_MS} мс`),
  TIMEOUT_MS,
);

const received = new Promise((resolve) => {
  ws.addEventListener('message', (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'hello') console.log(`✓ ws hello, consumer group = ${msg.consumerGroup}`);
    if (msg.type === 'event' && msg.event.correlationId === correlationId) resolve(msg);
  });
});
await new Promise((resolve, reject) => {
  ws.addEventListener('open', resolve, { once: true });
  ws.addEventListener('error', () => reject(new Error('ws error')), { once: true });
}).catch((e) => fail(`ws: ${e.message}`));

const res = await fetch(`${SIM}/simulator/burst?count=1`, {
  method: 'POST',
  headers: { 'x-correlation-id': correlationId },
});
const body = await res.json();
if (!res.ok || body.published !== 1) fail(`publish failed: ${res.status} ${JSON.stringify(body)}`);
console.log(`✓ published, correlationId = ${correlationId}`);

const msg = await received;
clearTimeout(timer);
const e2e = Date.now() - Date.parse(msg.event.occurredAt);
console.log(
  `✓ received via WS: ${msg.event.payload.channel}/${msg.event.payload.queue}, ` +
    `partition=${msg.meta.partition} offset=${msg.meta.offset}, e2e=${e2e} ms`,
);
ws.close();

// Тот же топик читает вторая, независимая группа forecast-service → строка в PostgreSQL.
const interactionId = msg.event.payload.interactionId;
const deadline = Date.now() + TIMEOUT_MS;
let row = null;
while (Date.now() < deadline) {
  const r = await fetch(`${FC}/interactions/${interactionId}`);
  if (r.ok && (row = await r.json()).started_at) break;
  await new Promise((resolve) => setTimeout(resolve, 200));
}
if (!row?.started_at)
  fail(`обращение ${interactionId} не появилось в forecast-service за ${TIMEOUT_MS} мс`);
console.log(
  `✓ stored by forecast-service: interaction ${interactionId}, started_at=${row.started_at}`,
);
console.log(`  Найди цепочку в логах всех сервисов по: ${correlationId}`);
process.exit(0);
