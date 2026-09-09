SELECT 'follow_events' AS table_name,
       COUNT(*) AS row_count,
       COUNT(*) FILTER (WHERE stream_id IS NULL) AS null_stream_id,
       MIN(received_at) AS first_received_at,
       MAX(received_at) AS last_received_at
FROM follow_events
WHERE received_at BETWEEN :'start' AND :'stop'
UNION ALL
SELECT 'incoming_raids' AS table_name,
       COUNT(*) AS row_count,
       COUNT(*) FILTER (WHERE stream_id IS NULL) AS null_stream_id,
       MIN(received_at) AS first_received_at,
       MAX(received_at) AS last_received_at
FROM incoming_raids
WHERE received_at BETWEEN :'start' AND :'stop';
