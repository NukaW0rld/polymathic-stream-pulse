SELECT gap_id, detected_at, recovered_at, reason_code FROM reconnection_gaps
WHERE run_id = :rid
ORDER BY detected_at ASC, gap_id ASC;
