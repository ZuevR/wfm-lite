import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { APP_CONFIG, type SimulatorConfig } from './config/config';

async function bootstrap() {
  // bufferLogs: логи старта Nest копятся, пока не подключим pino, — чтобы всё было в JSON.
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(Logger));
  // SIGTERM/SIGINT → хуки shutdown → продюсер корректно закрывается (и при рестарте node --watch).
  app.enableShutdownHooks();
  const config = app.get<SimulatorConfig>(APP_CONFIG);
  await app.listen(config.port);
}

bootstrap().catch((err) => {
  console.error('channel-simulator failed to start', err);
  process.exit(1);
});
