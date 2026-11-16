import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { APP_CONFIG, type ForecastConfig } from '../config/config';
import { ConsumerService } from '../kafka/consumer.service';
import { DatabaseService } from '../database/database.service';
import { errorMessage } from '../kafka/error-policy';
import { InteractionsRepository } from '../database/interactions.repository';

@Controller()
export class HealthController {
  constructor(
    private readonly consumer: ConsumerService,
    private readonly db: DatabaseService,
    private readonly repo: InteractionsRepository,
    @Inject(APP_CONFIG) private readonly config: ForecastConfig,
  ) {}

  /** liveness: процесс жив. */
  @Get('health')
  health() {
    return { status: 'ok' };
  }

  /** readiness: в группе Kafka И БД отвечает. Любое «нет» → 503 (трафик не слать, но не перезапускать). */
  @Get('ready')
  ready() {
    const checks = { kafkaConsumerInGroup: this.consumer.isReady(), database: this.db.isReady() };
    if (!checks.kafkaConsumerInGroup || !checks.database) {
      throw new ServiceUnavailableException({ status: 'not_ready', checks, db: this.db.details() });
    }
    return { status: 'ready', checks };
  }

  /** Отладка: партиции, счётчики (в т.ч. дубли), lag, сводка целостности таблицы. */
  @Get('stats')
  async stats() {
    // /stats нужен как раз во время инцидента: БД лежит → отдаём состояние консьюмера без сводки.
    const interactions = await this.repo
      .summary(this.config.forecast.ahtMaturitySec)
      .catch((err: unknown) => ({ error: errorMessage(err) }));
    return { consumer: this.consumer.stats(), interactions };
  }
}
