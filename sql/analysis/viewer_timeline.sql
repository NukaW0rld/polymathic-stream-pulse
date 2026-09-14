SELECT observed_at, viewer_count
FROM viewer_snapshots
WHERE stream_id = :'stream_id'
ORDER BY observed_at ASC;
