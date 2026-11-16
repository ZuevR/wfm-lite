import {
  BadRequestException,
  Controller,
  Get,
  Inject,
  NotFoundException,
  Param,
  Query,
} from '@nestjs/common';
import { z } from 'zod';
import { CHANNELS } from '@wfm/contracts';
import { APP_CONFIG, type ForecastConfig } from '../config/config';
import { fillIntervals, INTERVAL_MINUTES, movingAverageForecast } from './forecast';
import { InteractionsRepository } from '../database/interactions.repository';

const forecastQuery = z.object({
  queue: z.string().min(1).max(64).optional(),
  channel: z.enum(CHANNELS).optional(),
  /** Сколько часов факта показать. */
  hours: z.coerce.number().int().min(1).max(72).default(6),
  /** Сколько последних полных интервалов брать в среднее. */
  window: z.coerce.number().int().min(1).max(96).default(4),
  /** На сколько интервалов вперёд прогноз. */
  horizon: z.coerce.number().int().min(1).max(96).default(4),
});

/**
 * GET /forecast?queue=cards&channel=chat — факт по 15-минутным интервалам + прогноз.
 * GET /interactions/:id — строка обращения (для smoke и отладки порядка started/ended).
 */
@Controller()
export class ForecastController {
  constructor(
    private readonly repo: InteractionsRepository,
    @Inject(APP_CONFIG) private readonly config: ForecastConfig,
  ) {}

  @Get('forecast')
  async forecast(@Query() raw: Record<string, string>) {
    const parsed = forecastQuery.safeParse(raw);
    if (!parsed.success) throw new BadRequestException(parsed.error.issues);
    const q = parsed.data;

    const now = new Date();
    const from = new Date(now.getTime() - q.hours * 3_600_000);
    const rows = await this.repo.loadByInterval({
      from,
      to: now,
      queue: q.queue,
      channel: q.channel,
    });
    const { ahtMaturitySec } = this.config.forecast;
    const actual = fillIntervals(rows, from, now, now, { ahtMaturityMs: ahtMaturitySec * 1000 });
    return {
      intervalMinutes: INTERVAL_MINUTES,
      filter: { queue: q.queue ?? null, channel: q.channel ?? null },
      method: { name: 'moving-average', window: q.window, ahtMaturitySec },
      actual,
      forecast: movingAverageForecast(actual, { window: q.window, horizon: q.horizon, now }),
    };
  }

  @Get('interactions/:id')
  async interaction(@Param('id') id: string) {
    if (!z.string().uuid().safeParse(id).success) throw new BadRequestException('id must be uuid');
    const row = await this.repo.find(id);
    if (!row) throw new NotFoundException();
    return row;
  }
}
