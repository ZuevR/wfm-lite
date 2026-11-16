import { Module } from '@nestjs/common';
import { LoggerModule } from 'nestjs-pino';
import { APP_CONFIG, loadConfig } from './config/config';
import { HealthController } from './health/health.controller';
import { loggerParams, MetricsModule } from '@wfm/service-kit';
import { ProducerService } from './kafka/producer.service';
import { SimulatorController } from './simulator/simulator.controller';
import { SimulatorService } from './simulator/simulator.service';

const config = loadConfig();

@Module({
  imports: [
    LoggerModule.forRoot(loggerParams(config.serviceName, config.log)),
    MetricsModule.forRoot(config.serviceName),
  ],
  controllers: [HealthController, SimulatorController],
  providers: [{ provide: APP_CONFIG, useValue: config }, ProducerService, SimulatorService],
})
export class AppModule {}
