SELECT stream_id, started_at FROM streams
WHERE started_at::date = :'target_date';
