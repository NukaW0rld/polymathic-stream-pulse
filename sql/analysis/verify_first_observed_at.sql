SELECT
  (SELECT first_observed_at FROM streams WHERE stream_id = :'stream_id') AS stream_first_observed_at,
  (SELECT MIN(observed_at) FROM viewer_snapshots WHERE stream_id = :'stream_id') AS first_snapshot_observed_at;
