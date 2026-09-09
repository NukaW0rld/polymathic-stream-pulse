SELECT COUNT(*) AS message_count,
       COUNT(DISTINCT stream_id) AS distinct_streams,
       MIN(received_at) AS first_received_at,
       MAX(received_at) AS last_received_at
FROM chat_messages
WHERE received_at BETWEEN :'start' AND :'stop';
