import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { ConsumerService } from '../kafka/consumer.service';
import { LagMonitorService } from '../lag/lag-monitor.service';

@Controller()
export class HealthController {
  constructor(
    private readonly consumer: ConsumerService,
    private readonly lagMonitor: LagMonitorService,
  ) {}

  /** liveness: процесс жив. */
  @Get('health')
  health() {
    return { status: 'ok' };
  }

  /** readiness: консьюмер в группе и получил партиции (во время ребаланса — not ready). */
  @Get('ready')
  ready() {
    const checks = { kafkaConsumerInGroup: this.consumer.isReady() };
    if (!checks.kafkaConsumerInGroup) {
      throw new ServiceUnavailableException({ status: 'not_ready', checks });
    }
    return { status: 'ready', checks };
  }

  /** Отладочная ручка (для человека): партиции, счётчики, lag групп снаружи. Для машин — /metrics. */
  @Get('stats')
  stats() {
    return { ...this.consumer.stats(), lag: this.lagMonitor.snapshot() };
  }
}
