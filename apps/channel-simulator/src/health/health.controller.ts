import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { ProducerService } from '../kafka/producer.service';

/**
 * liveness (/health): процесс жив и отвечает → «не надо перезапускать».
 * readiness (/ready): зависимости готовы → «можно слать трафик». Kafka недоступна — not ready,
 * но перезапуск процесса не поможет, поэтому это разные проверки.
 */
@Controller()
export class HealthController {
  constructor(private readonly producer: ProducerService) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  @Get('ready')
  ready() {
    const checks = { kafkaProducer: this.producer.isReady() };
    const details = {
      kafka: this.producer.readinessDetails(),
      buffer: this.producer.bufferStats(),
    };
    if (!checks.kafkaProducer)
      throw new ServiceUnavailableException({ status: 'not_ready', checks, details });
    return { status: 'ready', checks, details };
  }
}
