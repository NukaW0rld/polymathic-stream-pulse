CREATE TABLE stream_metadata_history (
	metadata_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	stream_id TEXT NOT NULL REFERENCES streams (stream_id),
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	observed_at TIMESTAMPTZ NOT NULL,
	title TEXT NOT NULL,
	category_id TEXT NOT NULL,
	category_name TEXT NOT NULL,
	language TEXT NOT NULL,
	tags JSONB NOT NULL,
	UNIQUE (stream_id, observed_at),
	CHECK (jsonb_typeof(tags) = 'array')
);
