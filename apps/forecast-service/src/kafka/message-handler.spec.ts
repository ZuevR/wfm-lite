import { describe, expect, it } from 'vitest';
import { createInteractionEndedV1, createInteractionStartedV1, HEADERS } from '@wfm/contracts';
import { handleRecord } from './message-handler';

const interactionId = '3f2504e0-4f89-41d3-9a0c-0305e82c3301';
const started = createInteractionStartedV1({
  correlationId: 'corr-1',
  payload: { interactionId, channel: 'voice', queue: 'credits', customerSegment: 'premium' },
  occurredAt: new Date('2026-11-08T10:07:00Z'),
});
const ended = createInteractionEndedV1({
  correlationId: 'corr-1',
  payload: { interactionId, outcome: 'abandoned' },
  occurredAt: new Date('2026-11-08T10:09:30Z'),
});
const msg = (value: unknown, headers = {}) => ({
  value: Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)),
  headers,
});

describe('forecast handleRecord', () => {
  it('started → строка с полями обращения и временем из occurredAt (а не временем обработки)', () => {
    const res = handleRecord({ topic: 'interaction.started', message: msg(started) });
    expect(res).toEqual({
      kind: 'started',
      correlationId: 'corr-1',
      row: {
        interactionId,
        channel: 'voice',
        queue: 'credits',
        customerSegment: 'premium',
        startedAt: new Date('2026-11-08T10:07:00Z'),
        eventId: started.eventId,
      },
    });
  });

  it('ended → только поля завершения (started-поля не трогает → порядок прихода неважен)', () => {
    const res = handleRecord({ topic: 'interaction.ended', message: msg(ended) });
    expect(res).toEqual({
      kind: 'ended',
      correlationId: 'corr-1',
      row: {
        interactionId,
        endedAt: new Date('2026-11-08T10:09:30Z'),
        outcome: 'abandoned',
        eventId: ended.eventId,
      },
    });
  });

  it('событие не того типа в топике → invalid, correlationId из заголовка', () => {
    const res = handleRecord({
      topic: 'interaction.started',
      message: msg(ended, { [HEADERS.CORRELATION_ID]: Buffer.from('corr-h') }),
    });
    expect(res).toMatchObject({ kind: 'invalid', correlationId: 'corr-h' });
  });

  it('битый JSON и чужой топик — invalid, без исключения', () => {
    expect(handleRecord({ topic: 'interaction.ended', message: msg('{oops') }).kind).toBe(
      'invalid',
    );
    expect(handleRecord({ topic: 'operator.status.changed', message: msg(started) })).toMatchObject(
      {
        kind: 'invalid',
        reason: 'unexpected topic operator.status.changed',
      },
    );
  });
});
