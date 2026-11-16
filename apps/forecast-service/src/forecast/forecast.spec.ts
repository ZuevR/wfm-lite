import { describe, expect, it } from 'vitest';
import { fillIntervals, intervalStart, movingAverageForecast, type IntervalLoad } from './forecast';

const at = (hhmm: string) => new Date(`2026-11-08T${hhmm}:00.000Z`);
const load = (hhmm: string, started: number): IntervalLoad => ({
  start: at(hhmm),
  started,
  ended: 0,
  avgHandleSec: null,
});

describe('intervalStart', () => {
  it('округляет вниз до 15 минут', () => {
    expect(intervalStart(new Date('2026-11-08T10:14:59.999Z'))).toEqual(at('10:00'));
    expect(intervalStart(at('10:15'))).toEqual(at('10:15'));
    expect(intervalStart(new Date('2026-11-08T10:44:00Z'))).toEqual(at('10:30'));
  });
});

describe('fillIntervals', () => {
  it('заполняет пропуски нулями и помечает текущий интервал как partial', () => {
    const series = fillIntervals(
      [load('10:00', 8), load('10:30', 4)],
      at('10:00'),
      at('11:00'),
      new Date('2026-11-08T10:50:00Z'),
    );
    expect(series.map((i) => [i.start.toISOString().slice(11, 16), i.started, i.partial])).toEqual([
      ['10:00', 8, false],
      ['10:15', 0, false],
      ['10:30', 4, false],
      ['10:45', 0, true],
    ]);
  });
});

describe('fillIntervals: окно дозревания AHT', () => {
  const row = (hhmm: string, started: number, ended: number, avg: number): IntervalLoad => ({
    start: at(hhmm),
    started,
    ended,
    avgHandleSec: avg,
  });
  const now = new Date('2026-11-08T10:39:00Z');
  const series = fillIntervals(
    [row('10:00', 10, 9, 160), row('10:15', 10, 6, 90), row('10:30', 4, 1, 20)],
    at('10:00'),
    at('10:45'),
    now,
    { ahtMaturityMs: 10 * 60_000 },
  );

  it('AHT есть только у интервалов, с конца которых прошло ≥ окна; свежие → null, а не заниженное «что есть»', () => {
    expect(
      series.map((i) => [i.start.toISOString().slice(11, 16), i.ahtMature, i.avgHandleSec]),
    ).toEqual([
      ['10:00', true, 160], // конец 10:15 + 10 мин = 10:25 ≤ 10:39 — дозрел
      ['10:15', false, null], // конец 10:30 + 10 мин = 10:40 > 10:39 — ещё нет (90 с занижено)
      ['10:30', false, null], // интервал ещё идёт
    ]);
  });

  it('unclosed = started − ended только у дозревших (потерянные ended — метрика качества данных)', () => {
    expect(series.map((i) => i.unclosed)).toEqual([1, null, null]);
  });

  it('без окна (по умолчанию): закончившийся интервал дозрел сразу, идущий — нет', () => {
    const plain = fillIntervals(
      [row('10:15', 10, 6, 90), row('10:30', 4, 1, 20)],
      at('10:15'),
      at('10:45'),
      now,
    );
    expect(plain.map((i) => [i.ahtMature, i.avgHandleSec, i.unclosed])).toEqual([
      [true, 90, 4],
      [false, null, null],
    ]);
  });
});

describe('movingAverageForecast', () => {
  const now = new Date('2026-11-08T11:05:00Z');
  const series = fillIntervals(
    [load('10:00', 10), load('10:15', 20), load('10:30', 30), load('10:45', 40), load('11:00', 1)],
    at('10:00'),
    at('11:15'),
    now,
  );

  it('берёт только ПОЛНЫЕ интервалы: неполный текущий (1 обращение) не тянет среднее вниз', () => {
    const f = movingAverageForecast(series, { window: 2, horizon: 3, now });
    expect(f.map((p) => p.expectedStarted)).toEqual([35, 35, 35]); // (30 + 40) / 2
    expect(f[0].start).toEqual(at('11:00')); // прогноз начинается с текущего интервала
    expect(f[2].start).toEqual(at('11:30'));
  });

  it('нет полных интервалов — нет прогноза', () => {
    const onlyPartial = fillIntervals([load('11:00', 3)], at('11:00'), at('11:15'), now);
    expect(movingAverageForecast(onlyPartial, { window: 4, horizon: 2, now })).toEqual([]);
  });
});
