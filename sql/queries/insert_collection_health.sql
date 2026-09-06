INSERT INTO collection_health (
	run_id,
	source,
	observed_at,
	status,
	reason_code
) VALUES (
	%(run_id)s,
	%(source)s,
	%(observed_at)s,
	%(status)s,
	%(reason_code)s
);
