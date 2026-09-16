INSERT INTO raid_source_context (
	eventsub_message_id, run_id, requested_at, status, reason_code, attempt_count
) VALUES (
	%(eventsub_message_id)s, %(run_id)s, %(requested_at)s,
	'in_progress', 'request_queued', 0
)
ON CONFLICT (eventsub_message_id) DO NOTHING;
