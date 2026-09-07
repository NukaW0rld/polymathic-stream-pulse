UPDATE reconnection_gaps
SET recovered_at = %(recovered_at)s
WHERE gap_id = %(gap_id)s
  AND recovered_at IS NULL;
