import { Module } from '@nestjs/common';
import { LoggerModule } from 'nestjs-pino';
import { APP_CONFIG, loadConfig } from './config/config';
import { ConsumerService } from './kafka/consumer.service';
import { DatabaseService, pgPoolProvider } from './database/database.service';
import { ForecastController } from './forecast/forecast.controller';
import { HealthController } from './health/health.controller';
import { InteractionsRepository } from './database/interactions.repository';
import { loggerParams, MetricsModule } from '@wfm/service-kit';

const config = loadConfig();

@Module({
  imports: [
    LoggerModule.forRoot(loggerParams(config.serviceName, config.log)),
    MetricsModule.forRoot(config.serviceName),
  ],
  controllers: [HealthController, ForecastController],
  providers: [
    { provide: APP_CONFIG, useValue: config },
    pgPoolProvider,
    DatabaseService,
    InteractionsRepository,
    ConsumerService,
  ],
})
export class AppModule {}
