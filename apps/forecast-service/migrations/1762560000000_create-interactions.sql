-- Up Migration

-- Одна строка = одно обращение. Храним ФАКТЫ (когда началось/закончилось), а не счётчики:
-- агрегаты по интервалам считаем запросом. Так запись идемпотентна (повтор события → та же строка),
-- а счётчик count + 1 удвоился бы на дубле (at-least-once).
--
-- Все поля, кроме id, допускают NULL: interaction.started и interaction.ended — разные топики,
-- общего порядка нет. Если ended пришёл первым, строка временно без channel/queue/started_at.
CREATE TABLE interactions (
  interaction_id   uuid PRIMARY KEY,
  channel          text,
  queue            text,
  customer_segment text,
  started_at       timestamptz,
  ended_at         timestamptz,
  outcome          text CHECK (outcome IN ('resolved', 'abandoned')),
  -- eventId последнего применённого события каждого типа: повтор того же события — no-op (и виден в логах).
  started_event_id uuid,
  ended_event_id   uuid,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Нагрузка по интервалам: фильтр по очереди + диапазон времени.
CREATE INDEX interactions_queue_started_at_idx ON interactions (queue, started_at);
CREATE INDEX interactions_started_at_idx ON interactions (started_at);

-- Down Migration

DROP TABLE interactions;
