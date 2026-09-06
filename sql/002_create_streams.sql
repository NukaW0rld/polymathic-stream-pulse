CREATE TABLE streams (
  stream_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  first_observed_at TIMESTAMPTZ NOT NULL,
  offline_observed_at TIMESTAMPTZ
);

ALTER TABLE viewer_snapshots
ADD CONSTRAINT fk_stream_id
FOREIGN KEY (stream_id)
REFERENCES streams (stream_id);
