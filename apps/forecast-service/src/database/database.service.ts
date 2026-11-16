import { Inject, Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { join } from 'node:path';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { Pool } from 'pg';
import { METRICS_REGISTRY, prom } from '@wfm/service-kit';
import { APP_CONFIG, type ForecastConfig } from '../config/config';

export const PG_POOL = Symbol('PG_POOL');

/** Провайдер пула: один Pool на процесс; репозиторий и DatabaseService получают его через DI. */
export const pgPoolProvider = {
  provide: PG_POOL,
  inject: [APP_CONFIG],
  useFactory: (config: ForecastConfig) =>
    new Pool({ connectionString: config.db.url, max: 10, connectionTimeoutMillis: 2_000 }),
};

const PING_INTERVAL_MS = 5_000;
const MIGRATIONS_DIR = join(__dirname, '..', '..', 'migrations');

/**
 * Владелец пула PostgreSQL:
 *   onModuleInit          → ping (fail fast: нет БД на старте — сервис не стартует) + миграции
 *   каждые 5 с            → ping для /ready (БД упала на ходу → not ready, но процесс жив)
 *   onApplicationShutdown → pool.end()
 * onModuleInit всех модулей завершается ДО onApplicationBootstrap, поэтому консьюмер начинает
 * читать Kafka только когда схема уже на месте.
 */
@Injectable()
export class DatabaseService implements OnModuleInit, OnApplicationShutdown {
  private healthy = false;
  private lastError: string | null = null;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    @Inject(APP_CONFIG) private readonly config: ForecastConfig,
    @Inject(PG_POOL) private readonly pool: Pool,
    @InjectPinoLogger(DatabaseService.name) private readonly logger: PinoLogger,
    @Inject(METRICS_REGISTRY) registry: prom.Registry,
  ) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- collect() вызывается с this = Gauge
    const self = this;
    new prom.Gauge({
      name: 'wfm_db_up',
      help: '1 — последний ping PostgreSQL (раз в 5 с) успешен',
      registers: [registry],
      collect() {
        this.set(self.healthy ? 1 : 0);
      },
    });
    // Без обработчика ошибка простаивающего клиента (БД перезапустилась) роняет процесс.
    this.pool.on('error', (err) => this.logger.warn({ err }, 'idle pg client error'));
  }

  async onModuleInit(): Promise<void> {
    await this.pool.query('SELECT 1');
    this.healthy = true;
    if (this.config.db.migrateOnStart) await this.migrate();
    this.timer = setInterval(() => void this.ping(), PING_INTERVAL_MS);
    this.logger.info('postgres connected');
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.pool.end();
  }

  isReady(): boolean {
    return this.healthy;
  }

  details() {
    return { healthy: this.healthy, lastError: this.lastError };
  }

  private async migrate(): Promise<void> {
    // node-pg-migrate 9 — чистый ESM, сервис — CommonJS: подключаем динамическим import().
    const { runner } = await import('node-pg-migrate');
    const applied = await runner({
      databaseUrl: this.config.db.url,
      dir: MIGRATIONS_DIR,
      direction: 'up',
      migrationsTable: 'pgmigrations',
      log: (msg: string) => this.logger.info({ migration: msg }, 'migration'),
    });
    this.logger.info({ applied: applied.map((m) => m.name) }, 'migrations up to date');
  }

  private async ping(): Promise<void> {
    const was = this.healthy;
    try {
      await this.pool.query('SELECT 1');
      this.healthy = true;
      this.lastError = null;
    } catch (err) {
      this.healthy = false;
      this.lastError = err instanceof Error ? err.message : String(err);
    }
    if (was !== this.healthy) {
      if (this.healthy) this.logger.info('postgres reachable again — ready');
      else this.logger.warn({ error: this.lastError }, 'postgres unreachable — not ready');
    }
  }
}
