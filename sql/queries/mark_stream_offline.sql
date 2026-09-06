UPDATE streams
SET offline_observed_at = %(offline_observed_at)s
WHERE stream_id = %(stream_id)s
  AND offline_observed_at IS NULL;
