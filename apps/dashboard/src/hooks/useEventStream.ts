import { useEffect } from 'react';
import type { WsServerMessage } from '@wfm/contracts';
import { useDashboard } from '../store/dashboard.store';

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;

/** Экспоненциальный backoff с jitter: 1s, 2s, 4s… ≤30s, ×(0.5..1) — чтобы клиенты не ломились толпой. */
export const backoffDelay = (attempt: number) =>
  Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** attempt) * (0.5 + Math.random() / 2);

function wsUrl(): string {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${proto}://${location.host}/ws`;
}

/**
 * Одно WS-соединение на приложение + автопереподключение.
 * Cleanup обязателен: в StrictMode React в dev монтирует эффект дважды — без cleanup будет два сокета.
 */
export function useEventStream(): void {
  useEffect(() => {
    const { setStatus, setHello, addEvent, setLag } = useDashboard.getState();
    let socket: WebSocket | null = null;
    let attempt = 0;
    let retryTimer: number | undefined;
    let disposed = false;

    const connect = () => {
      setStatus(attempt === 0 ? 'connecting' : 'reconnecting');
      socket = new WebSocket(wsUrl());

      socket.onopen = () => {
        attempt = 0;
        setStatus('open');
      };

      socket.onmessage = (e: MessageEvent<string>) => {
        let msg: WsServerMessage;
        try {
          msg = JSON.parse(e.data) as WsServerMessage;
        } catch {
          console.warn('ws: non-JSON message ignored');
          return;
        }
        if (msg.type === 'hello') setHello(msg.consumerGroup);
        else if (msg.type === 'event') addEvent(msg);
        else if (msg.type === 'lag') setLag({ measuredAt: msg.measuredAt, groups: msg.groups });
      };

      // onclose приходит и после onerror, поэтому переподключаемся только здесь.
      socket.onclose = () => {
        if (disposed) return;
        const delay = backoffDelay(attempt++);
        setStatus('reconnecting', Math.round(delay));
        retryTimer = window.setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      disposed = true;
      window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, []);
}
