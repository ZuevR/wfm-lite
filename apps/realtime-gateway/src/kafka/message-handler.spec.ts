import { describe, expect, it } from 'vitest';
import { createInteractionStartedV1, HEADERS } from '@wfm/contracts';
import { handleInteractionStarted, readHeader } from './message-handler';

const event = createInteractionStartedV1({
  correlationId: 'corr-42',
  payload: {
    interactionId: '3f2504e0-4f89-41d3-9a0c-0305e82c3301',
    channel: 'voice',
    queue: 'credits',
    customerSegment: 'premium',
  },
});

const record = (value: Buffer | null, headers = {}) => ({
  topic: 'interaction.started',
  partition: 2,
  message: { value, headers, offset: '17' },
});

describe('handleInteractionStarted', () => {
  it('валидная запись → WS-сообщение с метаданными доставки', () => {
    const now = new Date('2026-11-05T10:00:00.000Z');
    const res = handleInteractionStarted(record(Buffer.from(JSON.stringify(event))), now);

    expect(res).toEqual({
      kind: 'valid',
      correlationId: 'corr-42',
      wsMessage: {
        type: 'event',
        event,
        meta: {
          topic: 'interaction.started',
          partition: 2,
          offset: '17',
          gatewayReceivedAt: now.toISOString(),
        },
      },
    });
  });

  it('битая запись → invalid, correlationId берём из заголовка', () => {
    const res = handleInteractionStarted(
      record(Buffer.from('garbage'), { [HEADERS.CORRELATION_ID]: Buffer.from('corr-from-header') }),
    );
    expect(res.kind).toBe('invalid');
    expect(res.correlationId).toBe('corr-from-header');
  });

  it('tombstone (value = null) → invalid, без исключения', () => {
    expect(handleInteractionStarted(record(null)).kind).toBe('invalid');
  });
});

describe('readHeader', () => {
  it('понимает Buffer, string и массив значений', () => {
    expect(readHeader({ a: Buffer.from('x') }, 'a')).toBe('x');
    expect(readHeader({ a: 'y' }, 'a')).toBe('y');
    expect(readHeader({ a: [Buffer.from('first'), 'second'] }, 'a')).toBe('first');
    expect(readHeader(undefined, 'a')).toBeUndefined();
  });
});
