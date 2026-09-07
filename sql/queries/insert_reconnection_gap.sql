INSERT INTO reconnection_gaps (
	run_id,
	detected_at,
	reason_code
) VALUES (
	%(run_id)s,
	%(detected_at)s,
	%(reason_code)s
)
RETURNING gap_id;
