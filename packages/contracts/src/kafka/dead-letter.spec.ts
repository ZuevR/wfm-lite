import { describe, expect, it } from 'vitest';
import { buildDeadLetter, DLQ_HEADERS, DLQ_TOPICS } from './dead-letter';
import { HEADERS } from './topics';

const src = {
  topic: 'interaction.started',
  partition: 2,
  message: {
    key: Buffer.from('id-1'),
    value: Buffer.from('{oops'),
    headers: { [HEADERS.CORRELATION_ID]: Buffer.from('corr-1') },
    offset: '42',
    timestamp: '1762941600000',
  },
};

describe('dead letter', () => {
  it('имя DLQ — от consumer group', () => {
    expect(DLQ_TOPICS.FORECAST_SERVICE).toBe('forecast-service.dlq');
  });

  it('key/value не меняются (replay как есть), исходные заголовки сохраняются, координаты — в заголовках', () => {
    const dl = buildDeadLetter(src, {
      consumerGroup: 'forecast-service',
      reason: 'invalid',
      error: 'invalid JSON',
      attempts: 1,
      failedAt: new Date('2026-11-12T10:00:00Z'),
    });
    expect(dl.key).toBe(src.message.key);
    expect(dl.value).toBe(src.message.value);
    expect(dl.headers).toEqual({
      [HEADERS.CORRELATION_ID]: Buffer.from('corr-1'),
      [DLQ_HEADERS.ORIGINAL_TOPIC]: 'interaction.started',
      [DLQ_HEADERS.ORIGINAL_PARTITION]: '2',
      [DLQ_HEADERS.ORIGINAL_OFFSET]: '42',
      [DLQ_HEADERS.ORIGINAL_TIMESTAMP]: '1762941600000',
      [DLQ_HEADERS.CONSUMER_GROUP]: 'forecast-service',
      [DLQ_HEADERS.REASON]: 'invalid',
      [DLQ_HEADERS.ERROR]: 'invalid JSON',
      [DLQ_HEADERS.ATTEMPTS]: '1',
      [DLQ_HEADERS.FAILED_AT]: '2026-11-12T10:00:00.000Z',
    });
  });

  it('длинный текст ошибки обрезается', () => {
    const dl = buildDeadLetter(src, {
      consumerGroup: 'g',
      reason: 'unknown',
      error: 'x'.repeat(5_000),
      attempts: 5,
    });
    expect((dl.headers[DLQ_HEADERS.ERROR] as string).length).toBe(1_001);
  });
});
