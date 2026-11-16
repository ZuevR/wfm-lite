import { useDashboard } from '../store/dashboard.store';

/** Порог «много» для подсветки — тот же, что в алерте WfmConsumerLagHigh (infra/prometheus/alerts.yml). */
const LAG_WARN = 1_000;

export function LagPanel() {
  const lag = useDashboard((s) => s.lag);
  return (
    <div className="panel panel--wide">
      <h2>Consumer lag</h2>
      <p className="muted small">
        high watermark − закоммиченный offset; меряет gateway через admin API брокера (снаружи
        групп)
        {lag && ` · ${new Date(lag.measuredAt).toLocaleTimeString()}`}
      </p>
      {!lag && <p className="muted">ждём первого замера…</p>}
      {lag && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Группа</th>
                <th>Состояние</th>
                <th>Участников</th>
                <th>Lag</th>
                <th>По партициям</th>
              </tr>
            </thead>
            <tbody>
              {lag.groups.map((g) => {
                const bad = g.members === 0 || g.totalLag > LAG_WARN;
                return (
                  <tr key={g.group} className={bad ? 'row--warn' : undefined}>
                    <td className="mono">{g.group}</td>
                    <td>{g.state}</td>
                    <td className="mono">{g.members}</td>
                    <td className="mono">{g.totalLag}</td>
                    <td className="mono muted">
                      {g.partitions
                        .map(
                          (p) => `${p.topic.replace('interaction.', '')}/${p.partition}: ${p.lag}`,
                        )
                        .join(' · ')}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
