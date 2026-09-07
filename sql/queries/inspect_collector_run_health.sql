SELECT health_id, source, observed_at, status, reason_code
FROM collection_health
WHERE run_id = 3
ORDER BY observed_at ASC, health_id ASC;
