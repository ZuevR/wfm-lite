import { useDashboard } from '../store/dashboard.store';

export function Feed() {
  const feed = useDashboard((s) => s.feed);
  return (
    <div className="panel panel--wide">
      <h2>Последние события</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Время</th>
              <th>Канал</th>
              <th>Очередь</th>
              <th>Сегмент</th>
              <th>Партиция / офсет</th>
              <th>Задержка</th>
              <th>correlationId</th>
            </tr>
          </thead>
          <tbody>
            {feed.map(({ msg, latencyMs }) => (
              // key = eventId: стабильный id → React переиспользует строки, а не пересоздаёт все 20.
              <tr key={msg.event.eventId}>
                <td>{new Date(msg.event.occurredAt).toLocaleTimeString()}</td>
                <td>
                  <span className={`chip chip--${msg.event.payload.channel}`}>
                    {msg.event.payload.channel}
                  </span>
                </td>
                <td>{msg.event.payload.queue}</td>
                <td>{msg.event.payload.customerSegment}</td>
                <td className="mono">
                  p{msg.meta.partition} / {msg.meta.offset}
                </td>
                <td className="mono">{latencyMs} мс</td>
                <td className="mono muted" title={msg.event.correlationId}>
                  {msg.event.correlationId.slice(0, 13)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
