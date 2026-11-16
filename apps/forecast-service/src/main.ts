import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { APP_CONFIG, type ForecastConfig } from './config/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(Logger));
  // SIGTERM → consumer.disconnect() (LeaveGroup, коммит offset'ов) → pool.end().
  app.enableShutdownHooks();
  const config = app.get<ForecastConfig>(APP_CONFIG);
  await app.listen(config.port);
}

bootstrap().catch((err) => {
  console.error('forecast-service failed to start', err);
  process.exit(1);
});
