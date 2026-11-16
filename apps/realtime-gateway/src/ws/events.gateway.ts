import { Inject, OnModuleDestroy } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import type { IncomingMessage } from 'node:http';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { WebSocket, WebSocketServer as WsServer } from 'ws';
import type { WsServerMessage } from '@wfm/contracts';
import { METRICS_REGISTRY, prom } from '@wfm/service-kit';
import { APP_CONFIG, type GatewayConfig } from '../config/config';

/** Сколько байт может скопиться в буфере сокета медленного клиента, прежде чем мы начнём пропускать ему события. */
const MAX_BUFFERED_BYTES = 1_000_000;

/**
 * «Голый» WebSocket (ws) на том же порту, что HTTP: ws://host:3002/ws.
 * Heartbeat: раз в WS_HEARTBEAT_MS шлём ping; кто не ответил pong к следующему тику — terminate.
 * Без этого «мёртвые» соединения (уснул ноутбук, оборвался NAT) висят вечно.
 */
@WebSocketGateway({ path: '/ws' })
export class EventsGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect, OnModuleDestroy
{
  @WebSocketServer() private server?: WsServer;
  private readonly alive = new WeakMap<WebSocket, boolean>();
  private heartbeat: NodeJS.Timeout | null = null;
  private droppedForSlowClient = 0;
  private readonly sent: prom.Counter<'type'>;
  private readonly dropped: prom.Counter;

  constructor(
    @Inject(APP_CONFIG) private readonly config: GatewayConfig,
    @InjectPinoLogger(EventsGateway.name) private readonly logger: PinoLogger,
    @Inject(METRICS_REGISTRY) registry: prom.Registry,
  ) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- collect() вызывается с this = Gauge
    const self = this;
    new prom.Gauge({
      name: 'wfm_ws_clients',
      help: 'Подключённых WebSocket-клиентов',
      registers: [registry],
      collect() {
        this.set(self.clientCount());
      },
    });
    this.sent = new prom.Counter({
      name: 'wfm_ws_messages_sent_total',
      help: 'Отправлено WS-сообщений клиентам (по типу: event, lag)',
      labelNames: ['type'],
      registers: [registry],
    });
    this.dropped = new prom.Counter({
      name: 'wfm_ws_dropped_slow_client_total',
      help: 'Сообщений НЕ отправлено медленным клиентам (буфер сокета > 1 МБ) — at-most-once до браузера',
      registers: [registry],
    });
  }

  afterInit(server: WsServer): void {
    this.heartbeat = setInterval(() => {
      for (const client of server.clients) {
        if (this.alive.get(client) === false) {
          this.logger.warn('ws client missed heartbeat — terminating');
          client.terminate();
          continue;
        }
        this.alive.set(client, false);
        client.ping();
      }
    }, this.config.ws.heartbeatMs);
  }

  handleConnection(client: WebSocket, req?: IncomingMessage): void {
    this.alive.set(client, true);
    client.on('pong', () => this.alive.set(client, true));
    this.send(client, {
      type: 'hello',
      serverTime: new Date().toISOString(),
      consumerGroup: this.config.kafka.groupId,
    });
    this.logger.info(
      { remote: req?.socket.remoteAddress, clients: this.clientCount() },
      'ws client connected',
    );
  }

  handleDisconnect(): void {
    this.logger.info({ clients: this.clientCount() }, 'ws client disconnected');
  }

  onModuleDestroy(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
  }

  /** Возвращает, скольким клиентам реально отправили. */
  broadcast(message: WsServerMessage): number {
    if (!this.server) return 0;
    const data = JSON.stringify(message);
    let delivered = 0;
    for (const client of this.server.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      // Backpressure: медленный клиент не должен раздувать память gateway.
      if (client.bufferedAmount > MAX_BUFFERED_BYTES) {
        this.droppedForSlowClient++;
        this.dropped.inc();
        continue;
      }
      client.send(data);
      delivered++;
    }
    if (delivered > 0) this.sent.inc({ type: message.type }, delivered);
    return delivered;
  }

  stats() {
    return { clients: this.clientCount(), droppedForSlowClient: this.droppedForSlowClient };
  }

  private clientCount(): number {
    return this.server?.clients.size ?? 0;
  }

  private send(client: WebSocket, message: WsServerMessage): void {
    client.send(JSON.stringify(message));
  }
}
