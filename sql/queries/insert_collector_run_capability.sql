INSERT INTO collector_run_capabilities (
	run_id, capability, status, observed_at, reason_code
) VALUES (
	%(run_id)s, %(capability)s, %(status)s, %(observed_at)s, %(reason_code)s
)
ON CONFLICT (run_id, capability) DO UPDATE
SET status = EXCLUDED.status,
	observed_at = EXCLUDED.observed_at,
	reason_code = EXCLUDED.reason_code;
