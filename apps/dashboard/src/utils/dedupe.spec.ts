import { describe, expect, it } from 'vitest';
import { RecentIds } from './dedupe';

describe('RecentIds', () => {
  it('повтор в окне — дубль', () => {
    const r = new RecentIds(3);
    expect(r.add('a')).toBe(true);
    expect(r.add('a')).toBe(false);
  });

  it('окно ограничено: самый старый вытесняется, размер не растёт', () => {
    const r = new RecentIds(2);
    r.add('a');
    r.add('b');
    r.add('c'); // вытеснил a
    expect(r.size).toBe(2);
    expect(r.add('b')).toBe(false);
    expect(r.add('a')).toBe(true); // a уже за окном — пропускаем как новый (известный компромисс)
  });
});
