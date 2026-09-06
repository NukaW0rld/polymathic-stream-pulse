CREATE TABLE collector_runs (
	run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	started_at TIMESTAMPTZ NOT NULL,
	last_heartbeat_at TIMESTAMPTZ NOT NULL CHECK (last_heartbeat_at >= started_at),
	stopped_at TIMESTAMPTZ CHECK (stopped_at IS NULL OR stopped_at >= last_heartbeat_at)
);
