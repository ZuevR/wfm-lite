import { Distribution } from './components/Distribution';
import { Feed } from './components/Feed';
import { Kpis } from './components/Kpis';
import { LagPanel } from './components/LagPanel';
import { StatusPill } from './components/StatusPill';
import { useEventStream } from './hooks/useEventStream';
import { useDashboard } from './store/dashboard.store';

export function App() {
  useEventStream();
  const byPartition = useDashboard((s) => s.byPartition);
  const byQueue = useDashboard((s) => s.byQueue);
  return (
    <main>
      <header>
        <h1>
          WFM-Lite <span className="muted">· realtime</span>
        </h1>
        <StatusPill />
      </header>
      <Kpis />
      <section className="grid">
        <Distribution
          title="По партициям"
          data={byPartition}
          note="ключ = interactionId → murmur2(key) % 3"
        />
        <Distribution title="По очередям" data={byQueue} />
      </section>
      <LagPanel />
      <Feed />
    </main>
  );
}
