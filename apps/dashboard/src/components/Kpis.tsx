import { useDashboard } from '../store/dashboard.store';
import { Tile } from './Tile';

export function Kpis() {
  const total = useDashboard((s) => s.total);
  const duplicates = useDashboard((s) => s.duplicates);
  const byChannel = useDashboard((s) => s.byChannel);
  const lastLatency = useDashboard((s) => s.feed[0]?.latencyMs);
  return (
    <section className="tiles">
      <Tile
        label="Обращений с открытия"
        value={total}
        hint={duplicates > 0 ? `дублей отсечено: ${duplicates}` : undefined}
      />
      <Tile label="Чаты" value={byChannel.chat} />
      <Tile label="Звонки" value={byChannel.voice} />
      <Tile
        label="Задержка последнего"
        value={lastLatency === undefined ? '—' : `${lastLatency} мс`}
        hint="occurredAt → браузер"
      />
    </section>
  );
}
