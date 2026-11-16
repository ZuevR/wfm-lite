import { useDashboard, type ConnectionStatus } from '../store/dashboard.store';

const STATUS_LABEL: Record<ConnectionStatus, string> = {
  connecting: 'подключение…',
  open: 'онлайн',
  reconnecting: 'переподключение',
};

export function StatusPill() {
  const status = useDashboard((s) => s.status);
  const retryInMs = useDashboard((s) => s.retryInMs);
  const group = useDashboard((s) => s.consumerGroup);
  return (
    <div className={`pill pill--${status}`}>
      <span className="dot" />
      {STATUS_LABEL[status]}
      {status === 'reconnecting' &&
        retryInMs !== null &&
        ` через ${(retryInMs / 1000).toFixed(1)} с`}
      {status === 'open' && group && <span className="muted"> · group {group}</span>}
    </div>
  );
}
