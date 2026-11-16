/**
 * Имена топиков: `<сущность>.<событие>` в прошедшем времени.
 * Событие — факт, который уже случился («обращение началось»), а не команда («начни обращение»).
 */
export const TOPICS = {
  INTERACTION_STARTED: 'interaction.started',
  INTERACTION_ENDED: 'interaction.ended',
} as const;
export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

/**
 * Consumer group = «логический потребитель»: кто читает и зачем.
 * Офсеты (позиция чтения) хранятся на группу. Две разные группы читают один топик независимо.
 */
export const CONSUMER_GROUPS = {
  /**
   * ПРЕФИКС, а не группа: у каждого экземпляра gateway своя группа `dashboard-gateway-<instance>`.
   * Gateway рассылает события своим WS-клиентам, поэтому каждому экземпляру нужен ВЕСЬ поток (fan-out).
   * В общей группе партиции поделились бы, и клиенты экземпляра видели бы только его партиции
   * (ADR 2026-11-08). Цена: каждый экземпляр читает весь топик; группы остановленных экземпляров
   * остаются в __consumer_offsets, пока не истечёт offsets.retention (7 дней по умолчанию).
   */
  DASHBOARD_GATEWAY_PREFIX: 'dashboard-gateway',
  /** Общая группа: каждое событие должно быть посчитано в прогнозе ровно одним экземпляром. */
  FORECAST_SERVICE: 'forecast-service',
} as const;

/** Группа экземпляра gateway: стабильна между рестартами (тот же хост и порт → те же offset'ы). */
export const dashboardGatewayGroupId = (instanceId: string): string =>
  `${CONSUMER_GROUPS.DASHBOARD_GATEWAY_PREFIX}-${instanceId}`;

/**
 * Заголовки Kafka-сообщения. Дублируют часть конверта, чтобы логировать и маршрутизировать,
 * не парся value (например, у битого сообщения correlationId всё равно будет в логе).
 */
export const HEADERS = {
  CORRELATION_ID: 'x-correlation-id',
  EVENT_TYPE: 'event-type',
  EVENT_VERSION: 'event-version',
} as const;
