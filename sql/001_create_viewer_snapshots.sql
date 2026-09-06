CREATE TABLE viewer_snapshots (
  stream_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (stream_id, observed_at),
  viewer_count INT NOT NULL CHECK (viewer_count >= 0)
);
