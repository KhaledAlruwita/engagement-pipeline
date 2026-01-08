
CREATE TABLE IF NOT EXISTS content (
  content_id      BIGINT PRIMARY KEY,
  content_type    TEXT NOT NULL,
  length_seconds  INT
);

CREATE TABLE IF NOT EXISTS engagement_events (
  id            BIGSERIAL PRIMARY KEY,
  content_id    BIGINT NOT NULL REFERENCES content(content_id),
  user_id       BIGINT NOT NULL,
  event_type    TEXT NOT NULL,
  event_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
  duration_ms   INT,
  device        TEXT,
  raw_payload   JSONB
);

-- Seed content dimension (simple sample)
INSERT INTO content (content_id, content_type, length_seconds)
VALUES
  (1, 'video', 300),
  (2, 'video', 120),
  (3, 'article', NULL)
ON CONFLICT (content_id) DO NOTHING;
