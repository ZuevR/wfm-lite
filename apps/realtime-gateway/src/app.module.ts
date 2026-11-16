import { Module } from '@nestjs/common';
import { LoggerModule } from 'nestjs-pino';
import { APP_CONFIG, loadConfig } from './config/config';
import { ConsumerService } from './kafka/consumer.service';
import { EventsGateway } from './ws/events.gateway';
import { HealthController } from './health/health.controller';
import { loggerParams, MetricsModule } from '@wfm/service-kit';
import { LagMonitorService } from './lag/lag-monitor.service';

const config = loadConfig();

@Module({
  imports: [
    LoggerModule.forRoot(loggerParams(config.serviceName, config.log)),
    MetricsModule.forRoot(config.serviceName),
  ],
  controllers: [HealthController],
  providers: [
    { provide: APP_CONFIG, useValue: config },
    EventsGateway,
    ConsumerService,
    LagMonitorService,
  ],
})
export class AppModule {}
