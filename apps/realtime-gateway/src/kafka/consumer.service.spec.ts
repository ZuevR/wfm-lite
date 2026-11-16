import 'reflect-metadata';
import { describe, expect, it, vi } from 'vitest';
import type { PinoLogger } from 'nestjs-pino';
import { loadConfig } from '../config/config';
import { ConsumerService } from './consumer.service';
import type { EventsGateway } from '../ws/events.gateway';
import { createRegistry } from '@wfm/service-kit';

// Фейковый KafkaJS-консьюмер: run() «висит», как при вступлении в группу, пока координатор
// ждёт sessionTimeout старого участника после kill -9.
const fakeConsumer = {
  events: {
    GROUP_JOIN: 'group_join',
    REBALANCING: 'rebalancing',
    CRASH: 'crash',
    STOP: 'stop',
    DISCONNECT: 'disconnect',
    END_BATCH_PROCESS: 'end_batch_process',
  },
  on: vi.fn(),
  connect: vi.fn(async () => {}),
  subscribe: vi.fn(async () => {}),
  run: vi.fn(() => new Promise<void>(() => {})),
};

vi.mock('kafkajs', () => ({
  Kafka: class {
    consumer() {
      return fakeConsumer;
    }
  },
  logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
}));

const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };

describe('ConsumerService bootstrap', () => {
  it('не ждёт вступления в группу: bootstrap завершается, пока run() висит, и сервис not ready', async () => {
    const service = new ConsumerService(
      loadConfig({}),
      {} as EventsGateway,
      logger as unknown as PinoLogger,
      createRegistry('test'),
    );

    const outcome = await Promise.race([
      service.onApplicationBootstrap().then(() => 'bootstrapped'),
      new Promise((resolve) => setTimeout(() => resolve('blocked'), 100)),
    ]);

    expect(outcome).toBe('bootstrapped');
    expect(fakeConsumer.run).toHaveBeenCalledOnce();
    expect(service.isReady()).toBe(false);
  });
});
