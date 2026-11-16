import { Controller, Get, Header, Inject, Module, type DynamicModule } from '@nestjs/common';
import * as client from 'prom-client';

/**
 * Метрики Prometheus. Один Registry на процесс (а не глобальный default из prom-client): тесты и
 * несколько приложений в одном процессе не мешают друг другу, а метка `service` проставляется всем.
 *
 * Соглашения по именам (prometheus.io/docs/practices/naming):
 *  - префикс `wfm_` для наших доменных метрик; `kafka_consumergroup_*` — как у kafka-exporter,
 *    чтобы алерты и дашборды переносились;
 *  - счётчики оканчиваются на `_total`, длительности — `_seconds`;
 *  - метки — только с ОГРАНИЧЕННЫМ набором значений (topic, reason, result). Никаких eventId,
 *    interactionId, correlationId: каждое уникальное значение — отдельный временной ряд в Prometheus.
 */
export const METRICS_REGISTRY = Symbol('METRICS_REGISTRY');

export function createRegistry(service: string): client.Registry {
  const registry = new client.Registry();
  registry.setDefaultLabels({ service });
  // process_cpu, память, event loop lag, GC — бесплатная база для «почему тормозит».
  client.collectDefaultMetrics({ register: registry });
  return registry;
}

@Controller()
class MetricsController {
  constructor(@Inject(METRICS_REGISTRY) private readonly registry: client.Registry) {}

  /** Скрейп Prometheus. Текстовый формат exposition 0.0.4. */
  @Get('metrics')
  @Header('Content-Type', client.register.contentType)
  @Header('Cache-Control', 'no-store')
  metrics(): Promise<string> {
    return this.registry.metrics();
  }
}

/** MetricsModule.forRoot('forecast-service') → GET /metrics + Registry в DI (inject METRICS_REGISTRY). */
@Module({})
export class MetricsModule {
  static forRoot(service: string): DynamicModule {
    return {
      module: MetricsModule,
      global: true,
      controllers: [MetricsController],
      providers: [{ provide: METRICS_REGISTRY, useValue: createRegistry(service) }],
      exports: [METRICS_REGISTRY],
    };
  }
}

export { client as prom };
