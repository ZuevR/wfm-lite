export function Distribution({
  title,
  data,
  note,
}: {
  title: string;
  data: Record<string, number>;
  note?: string;
}) {
  const entries = Object.entries(data).sort(([a], [b]) =>
    a.localeCompare(b, 'en', { numeric: true }),
  );
  const max = Math.max(1, ...entries.map(([, v]) => v));
  return (
    <div className="panel">
      <h2>{title}</h2>
      {note && <p className="muted small">{note}</p>}
      {entries.length === 0 && <p className="muted">пока пусто</p>}
      {entries.map(([k, v]) => (
        <div key={k} className="bar">
          <span className="bar__label">{k}</span>
          <span className="bar__track">
            <span className="bar__fill" style={{ width: `${(v / max) * 100}%` }} />
          </span>
          <span className="bar__value">{v}</span>
        </div>
      ))}
    </div>
  );
}
