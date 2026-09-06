CREATE TABLE collection_health (
	health_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	source TEXT NOT NULL CHECK (source IN ('stream_poll', 'chat', 'raids', 'follows')),
	observed_at TIMESTAMPTZ NOT NULL,
	status TEXT NOT NULL CHECK (status IN ('starting', 'healthy', 'error', 'stopped')),
	reason_code TEXT NOT NULL
);
