UPDATE chatter_presence_snapshots
SET completed_at = %(completed_at)s,
	reported_total = %(reported_total)s,
	collected_distinct_count = %(collected_distinct_count)s,
	status = %(status)s,
	reason_code = %(reason_code)s,
	reported_total_changed = %(reported_total_changed)s
WHERE snapshot_id = %(snapshot_id)s
	AND status = 'in_progress';
