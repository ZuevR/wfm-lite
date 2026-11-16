import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import {
  createInteractionEndedV1,
  createInteractionStartedV1,
  type InteractionEndedV1,
  type InteractionStartedV1,
} from '@wfm/contracts';
import { APP_CONFIG, type SimulatorConfig } from '../config/config';
import { planInteraction, type InteractionPlan } from './generator';
import { ProducerService, type PublishableEvent } from '../kafka/producer.service';
import { BufferClosedError, BufferFullError } from '../kafka/publish-buffer';

/** Исход приёма события: accepted=false — буфер полон/закрыт, событие отвергнуто у источника. */
interface Emitted {
  accepted: boolean;
  /** Резолвится true, когда Kafka подтвердила запись; false — потеряно после приёма. */
  delivered: Promise<boolean>;
}

/**
 * Поток входящих обращений с заданной частотой. Каждое обращение живёт: started → (handle time) → ended.
 * Небольшая доля — «переставленные»: ended уходит в Kafka раньше started (разные топики → общего
 * порядка нет, потребители обязаны это переживать).
 *
 * Отправка — через буфер ProducerService: emit() не ждёт Kafka, а только приёма в буфер.
 * Буфер полон (Kafka долго лежит) → событие отвергается и считается в `rejected` — явная потеря у источника.
 *
 * Graceful shutdown в два этапа (фазы Nest идут строго по порядку для всех модулей):
 *   beforeApplicationShutdown — перестать генерировать, снять таймеры ended; ProducerService досылает буфер
 *   onApplicationShutdown     — ProducerService закрывает соединение
 * Обращения, чей ended не успел уйти, так и останутся «открытыми» — как при падении реального канала.
 */
@Injectable()
export class SimulatorService implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private timer: NodeJS.Timeout | null = null;
  private ratePerSec: number;
  /** Таймеры будущих interaction.ended. */
  private readonly pendingEnded = new Set<NodeJS.Timeout>();
  private sent = 0;
  /** Приняты в буфер, но не доставлены (неретраемая ошибка / не успели при остановке). */
  private failed = 0;
  /** Отвергнуты на входе: буфер полон или закрыт. */
  private rejected = 0;
  private bufferFull = false;
  private endedSent = 0;
  private reordered = 0;

  constructor(
    @Inject(APP_CONFIG) private readonly config: SimulatorConfig,
    private readonly producer: ProducerService,
    @InjectPinoLogger(SimulatorService.name) private readonly logger: PinoLogger,
  ) {
    this.ratePerSec = config.simulator.ratePerSec;
  }

  onApplicationBootstrap(): void {
    if (this.config.simulator.autostart) this.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    this.stop();
    const dropped = this.pendingEnded.size;
    for (const t of this.pendingEnded) clearTimeout(t);
    this.pendingEnded.clear();
    if (dropped > 0) this.logger.warn({ dropped }, 'pending interaction.ended dropped on shutdown');
  }

  start(ratePerSec: number = this.ratePerSec): void {
    this.stop();
    this.ratePerSec = ratePerSec;
    const intervalMs = Math.max(1, Math.round(1000 / ratePerSec));
    // Каждое сгенерированное обращение — новая бизнес-цепочка → новый correlationId.
    this.timer = setInterval(() => void this.runInteraction(randomUUID()), intervalMs);
    this.logger.info({ ratePerSec, intervalMs }, 'simulator started');
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    this.logger.info('simulator stopped');
  }

  /**
   * N обращений разом (под одним correlationId запроса) — для smoke и экспериментов.
   * Возвращает исход started; ended уйдут позже по таймерам.
   */
  async burst(
    count: number,
    correlationId: string,
  ): Promise<{ published: number; failed: number; interactionIds: string[] }> {
    const results = Array.from({ length: count }, () => this.runInteraction(correlationId));
    const delivered = await Promise.all(results.map((r) => r.delivered));
    const published = delivered.filter(Boolean).length;
    return {
      published,
      failed: count - published,
      interactionIds: results.map((r) => r.interactionId),
    };
  }

  /** Влезет ли N обращений (×2: started + ended) — для HTTP backpressure «всё или ничего». */
  canAccept(count: number): boolean {
    return this.producer.freeSlots() >= count * 2;
  }

  stats() {
    return {
      running: this.timer !== null,
      ratePerSec: this.ratePerSec,
      sent: this.sent,
      failed: this.failed,
      endedSent: this.endedSent,
      pendingEnded: this.pendingEnded.size,
      reordered: this.reordered,
      rejected: this.rejected,
      buffer: this.producer.bufferStats(),
    };
  }

  /**
   * Одно обращение целиком: started сейчас и ended по таймеру (или сначала ended — если «переставлено»).
   * Порядок ended → started держит FIFO буфера (один воркер), ждать доставки между ними не нужно.
   * Таймер ended стартует от ПРИЁМА started, а не от доставки: handle time — время в домене,
   * простой Kafka его не удлиняет (события просто доедут позже со своим occurredAt).
   */
  private runInteraction(correlationId: string): {
    delivered: Promise<boolean>;
    interactionId: string;
  } {
    const plan = planInteraction(this.config.simulator.lifecycle);
    const interactionId = plan.payload.interactionId;

    if (plan.reordered) {
      this.reordered++;
      const endedAt = new Date();
      const startedAt = new Date(endedAt.getTime() - plan.handleSec * 1000);
      const ended = this.emit(this.endedEvent(plan, correlationId, endedAt));
      const started = this.emit(
        createInteractionStartedV1({ correlationId, payload: plan.payload, occurredAt: startedAt }),
      );
      const delivered = Promise.all([ended.delivered, started.delivered]).then(([a, b]) => a && b);
      return { delivered, interactionId };
    }

    const started: InteractionStartedV1 = createInteractionStartedV1({
      correlationId,
      payload: plan.payload,
    });
    const res = this.emit(started);
    if (res.accepted) this.scheduleEnded(plan, correlationId);
    return { delivered: res.delivered, interactionId };
  }

  private scheduleEnded(plan: InteractionPlan, correlationId: string): void {
    const t = setTimeout(() => {
      this.pendingEnded.delete(t);
      void this.emit(this.endedEvent(plan, correlationId, new Date()));
    }, plan.handleSec * 1000);
    this.pendingEnded.add(t);
  }

  private endedEvent(plan: InteractionPlan, correlationId: string, at: Date): InteractionEndedV1 {
    return createInteractionEndedV1({
      correlationId,
      payload: { interactionId: plan.payload.interactionId, outcome: plan.outcome },
      occurredAt: at,
    });
  }

  private emit(event: PublishableEvent): Emitted {
    const ctx = {
      correlationId: event.correlationId,
      eventId: event.eventId,
      topic: event.eventType,
    };
    let delivery: Promise<unknown>;
    try {
      delivery = this.producer.publish(event);
    } catch (err) {
      if (!(err instanceof BufferFullError || err instanceof BufferClosedError)) throw err;
      this.rejected++;
      // Логируем смену состояния, а не каждое отвергнутое событие (их могут быть тысячи в секунду).
      if (err instanceof BufferFullError && !this.bufferFull) {
        this.bufferFull = true;
        this.logger.error(
          { ...ctx, capacity: err.capacity },
          'publish buffer full — rejecting new events',
        );
      }
      return { accepted: false, delivered: Promise.resolve(false) };
    }
    if (this.bufferFull) {
      this.bufferFull = false;
      this.logger.info(
        { rejected: this.rejected },
        'publish buffer has room again — accepting events',
      );
    }
    const delivered = delivery.then(
      () => {
        if (event.eventType === 'interaction.ended') this.endedSent++;
        else this.sent++;
        return true;
      },
      (err: unknown) => {
        this.failed++;
        this.logger.error({ err, ...ctx }, 'event accepted but not delivered');
        return false;
      },
    );
    return { accepted: true, delivered };
  }
}
