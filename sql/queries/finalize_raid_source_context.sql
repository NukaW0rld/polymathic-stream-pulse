UPDATE raid_source_context
SET observed_at = %(observed_at)s,
	status = %(status)s,
	reason_code = %(reason_code)s,
	attempt_count = %(attempt_count)s,
	category_id = %(category_id)s,
	category_name = %(category_name)s,
	title = %(title)s,
	language = %(language)s,
	tags = %(tags)s
WHERE eventsub_message_id = %(eventsub_message_id)s
	AND status = 'in_progress';
