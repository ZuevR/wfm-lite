import { describe, expect, it } from 'vitest';
import { groupLagView, partitionLags, selectGroups } from './lag';

const marks = [
  {
    topic: 'interaction.started',
    partitions: [
      { partition: 0, high: '100', low: '0' },
      { partition: 1, high: '50', low: '10' },
      { partition: 2, high: '30', low: '20' },
    ],
  },
];

describe('partitionLags', () => {
  it('lag = high − committed по каждой партиции', () => {
    const res = partitionLags(
      [{ topic: 'interaction.started', partitions: [{ partition: 0, offset: '90' }] }],
      marks,
    );
    expect(res[0]).toEqual({
      topic: 'interaction.started',
      partition: 0,
      committed: '90',
      high: '100',
      lag: 10,
    });
  });

  it('нет коммита (-1 или партиции нет в ответе) → от low, committed = null', () => {
    const res = partitionLags(
      [{ topic: 'interaction.started', partitions: [{ partition: 1, offset: '-1' }] }],
      marks,
    );
    expect(res.find((p) => p.partition === 1)).toMatchObject({ committed: null, lag: 40 });
    expect(res.find((p) => p.partition === 2)).toMatchObject({ committed: null, lag: 10 });
  });

  it('committed < low (retention удалил непрочитанное) → lag 0, а не отрицательный', () => {
    const res = partitionLags(
      [{ topic: 'interaction.started', partitions: [{ partition: 2, offset: '5' }] }],
      marks,
    );
    expect(res.find((p) => p.partition === 2)).toMatchObject({ lag: 25 });
    const deleted = partitionLags(
      [{ topic: 'interaction.started', partitions: [{ partition: 2, offset: '40' }] }],
      marks,
    );
    expect(deleted.find((p) => p.partition === 2)!.lag).toBe(0);
  });

  it('топика нет среди watermarks — пропускаем', () => {
    expect(partitionLags([{ topic: 'gone', partitions: [] }], marks)).toEqual([]);
  });

  it('groupLagView суммирует lag', () => {
    const parts = partitionLags(
      [
        {
          topic: 'interaction.started',
          partitions: [
            { partition: 0, offset: '90' },
            { partition: 1, offset: '45' },
            { partition: 2, offset: '30' },
          ],
        },
      ],
      marks,
    );
    expect(groupLagView('g', 'Stable', 1, parts).totalLag).toBe(15);
  });
});

describe('selectGroups', () => {
  it('наблюдаемые — всегда (даже Empty); группы gateway — только живые', () => {
    const groups = selectGroups(
      [
        { groupId: 'forecast-service', state: 'Empty' },
        { groupId: 'dashboard-gateway-mac-3002', state: 'Stable' },
        { groupId: 'dashboard-gateway-mac-3012', state: 'Empty' },
        { groupId: 'console-consumer-1', state: 'Stable' },
      ],
      { watched: ['forecast-service'], prefixes: ['dashboard-gateway-'] },
    );
    expect(groups).toEqual(['dashboard-gateway-mac-3002', 'forecast-service']);
  });
});
