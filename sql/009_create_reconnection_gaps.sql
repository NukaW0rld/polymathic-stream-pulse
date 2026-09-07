CREATE TABLE reconnection_gaps (
	gap_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	detected_at TIMESTAMPTZ NOT NULL,
	recovered_at TIMESTAMPTZ CHECK (recovered_at IS NULL OR recovered_at >= detected_at),
	reason_code TEXT NOT NULL CHECK (reason_code IN ('network_error', 'keepalive_timeout'))
);
