import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { WsAdapter } from '@nestjs/platform-ws';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { APP_CONFIG, type GatewayConfig } from './config/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(Logger));
  // WsAdapter = библиотека ws (чистый протокол RFC 6455), а не socket.io со своим протоколом поверх.
  app.useWebSocketAdapter(new WsAdapter(app));
  // SIGTERM → consumer.disconnect() → LeaveGroup. Иначе после рестарта «зомби»-участник висит в
  // группе до session.timeout и держит свои партиции — события по ним не читаются.
  app.enableShutdownHooks();
  const config = app.get<GatewayConfig>(APP_CONFIG);
  await app.listen(config.port);
}

bootstrap().catch((err) => {
  console.error('realtime-gateway failed to start', err);
  process.exit(1);
});
