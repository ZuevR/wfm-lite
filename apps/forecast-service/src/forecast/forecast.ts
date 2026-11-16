/**
 * Нагрузка по 15-минутным интервалам и простой прогноз. Чистые функции, без I/O.
 *
 * Интервал 15 минут — стандартная гранулярность WFM-планирования (смены и перерывы режутся по 15 мин).
 * Прогноз — скользящее среднее последних полных интервалов. Это намеренно простая модель: ценность здесь —
 * конвейер (Kafka → БД → API), а не математика. Реальные WFM учитывают сезонность (день недели,
 * время суток) и переводят нагрузку в число операторов формулой Erlang C.
 */
export const INTERVAL_MINUTES = 15;
export const INTERVAL_MS = INTERVAL_MINUTES * 60_000;

/** Начало 15-минутного интервала, в который попадает момент (UTC, от эпохи — как date_bin в SQL). */
export const intervalStart = (at: Date): Date =>
  new Date(Math.floor(at.getTime() / INTERVAL_MS) * INTERVAL_MS);

export interface IntervalLoad {
  start: Date;
  /** Обращений, начавшихся в интервале. */
  started: number;
  /** Из них уже завершившихся. */
  ended: number;
  /** Средний handle time завершившихся, с; null — если завершившихся нет. */
  avgHandleSec: number | null;
}

export interface IntervalView extends IntervalLoad {
  /** Интервал ещё идёт — его счётчик неполный, в среднее его брать нельзя. */
  partial: boolean;
  /**
   * Интервал «дозрел» для AHT: с его конца прошло ≥ ahtMaturity — все обращения, начавшиеся в нём,
   * уже должны были завершиться. Только тогда avgHandleSec не null.
   */
  ahtMature: boolean;
  /**
   * Сколько обращений дозревшего интервала так и не получили ended (потеряны: продюсер остановлен,
   * событие отвергнуто). null — интервал ещё не дозрел. Это метрика качества данных: AHT по такому
   * интервалу посчитан без них и, скорее всего, занижен (теряются чаще длинные).
   */
  unclosed: number | null;
}

export interface FillOptions {
  /** Окно дозревания AHT, мс. 0 — без окна. */
  ahtMaturityMs: number;
}

/**
 * Непрерывный ряд интервалов [from, to): пропуски (интервалы без обращений) заполняются нулями.
 * Без этого «тихий» интервал просто выпал бы из ряда и завысил среднее.
 *
 * AHT — с окном дозревания: на свежем интервале успели завершиться только
 * короткие обращения, и среднее по ним занижено (цензурирование справа, «эффект выжившего»).
 * Заниженный AHT в WFM → недобор операторов. Поэтому до дозревания avgHandleSec = null, а не «что есть».
 */
export function fillIntervals(
  rows: IntervalLoad[],
  from: Date,
  to: Date,
  now: Date,
  opts: FillOptions = { ahtMaturityMs: 0 },
): IntervalView[] {
  const byStart = new Map(rows.map((r) => [intervalStart(r.start).getTime(), r]));
  const result: IntervalView[] = [];
  for (let t = intervalStart(from).getTime(); t < to.getTime(); t += INTERVAL_MS) {
    const row = byStart.get(t);
    const started = row?.started ?? 0;
    const ended = row?.ended ?? 0;
    const ahtMature = t + INTERVAL_MS + opts.ahtMaturityMs <= now.getTime();
    result.push({
      start: new Date(t),
      started,
      ended,
      avgHandleSec: ahtMature ? (row?.avgHandleSec ?? null) : null,
      partial: t + INTERVAL_MS > now.getTime(),
      ahtMature,
      unclosed: ahtMature ? started - ended : null,
    });
  }
  return result;
}

export interface ForecastPoint {
  start: Date;
  expectedStarted: number;
}

/**
 * Скользящее среднее по последним `window` ПОЛНЫМ интервалам → прогноз на `horizon` интервалов,
 * начиная с текущего (неполного). Нет полных интервалов — прогнозировать не из чего, пустой массив.
 */
export function movingAverageForecast(
  series: IntervalView[],
  opts: { window: number; horizon: number; now: Date },
): ForecastPoint[] {
  const complete = series.filter((i) => !i.partial);
  const lastN = complete.slice(-opts.window);
  if (lastN.length === 0) return [];
  const avg = lastN.reduce((sum, i) => sum + i.started, 0) / lastN.length;
  const first = intervalStart(opts.now).getTime();
  return Array.from({ length: opts.horizon }, (_, k) => ({
    start: new Date(first + k * INTERVAL_MS),
    expectedStarted: Math.round(avg * 10) / 10,
  }));
}
