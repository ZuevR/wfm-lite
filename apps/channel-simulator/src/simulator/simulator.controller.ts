import {
  BadRequestException,
  Controller,
  Get,
  HttpCode,
  Post,
  Query,
  Req,
  Res,
  ServiceUnavailableException,
} from '@nestjs/common';
import type { ServerResponse } from 'node:http';
import type { IncomingMessage } from 'node:http';
import { correlationIdOf } from '@wfm/service-kit';
import { SimulatorService } from './simulator.service';

const parseNumber = (raw: string | undefined, name: string, min: number, max: number) => {
  if (raw === undefined) return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min || n > max) {
    throw new BadRequestException(`${name} must be a number in [${min}, ${max}]`);
  }
  return n;
};

/** Ручки управления симулятором — для экспериментов («поднять нагрузку», «остановить поток»). */
@Controller('simulator')
export class SimulatorController {
  constructor(private readonly simulator: SimulatorService) {}

  @Get()
  status() {
    return this.simulator.stats();
  }

  @Post('start')
  @HttpCode(200)
  start(@Query('rate') rate?: string) {
    this.simulator.start(parseNumber(rate, 'rate', 0.1, 500));
    return this.simulator.stats();
  }

  @Post('stop')
  @HttpCode(200)
  stop() {
    this.simulator.stop();
    return this.simulator.stats();
  }

  /** curl -X POST -H 'x-correlation-id: test-1' 'localhost:3001/simulator/burst?count=5' */
  @Post('burst')
  @HttpCode(200)
  async burst(
    @Req() req: IncomingMessage,
    @Res({ passthrough: true }) res: ServerResponse,
    @Query('count') count?: string,
  ) {
    const n = parseNumber(count, 'count', 1, 1000) ?? 1;
    // Backpressure: буфер не вмещает запрос — честный 503 + Retry-After, а не «принял и потерял».
    if (!this.simulator.canAccept(n)) {
      res.setHeader('Retry-After', '5');
      throw new ServiceUnavailableException('publish buffer is full — Kafka unavailable?');
    }
    const correlationId = correlationIdOf(req);
    return { correlationId, ...(await this.simulator.burst(n, correlationId)) };
  }
}
