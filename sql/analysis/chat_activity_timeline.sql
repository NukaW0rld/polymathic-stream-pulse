SELECT date_trunc('minute', notification_at) AS timeline_minute, COUNT(*) AS message_count FROM chat_messages
WHERE stream_id = :'stream_id'
GROUP BY timeline_minute
ORDER BY timeline_minute ASC;
