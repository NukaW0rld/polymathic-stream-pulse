CREATE TABLE raid_source_context (
	eventsub_message_id TEXT PRIMARY KEY REFERENCES incoming_raids (eventsub_message_id) ON DELETE CASCADE,
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	requested_at TIMESTAMPTZ NOT NULL,
	observed_at TIMESTAMPTZ,
	status TEXT NOT NULL CHECK (status IN ('in_progress', 'complete', 'not_found', 'failed', 'interrupted')),
	reason_code TEXT NOT NULL,
	attempt_count SMALLINT NOT NULL CHECK (attempt_count BETWEEN 0 AND 2),
	category_id TEXT,
	category_name TEXT,
	title TEXT,
	language TEXT,
	tags JSONB,
	CHECK ((status = 'in_progress' AND observed_at IS NULL)
		OR (status <> 'in_progress' AND observed_at IS NOT NULL)),
	CHECK (observed_at IS NULL OR observed_at >= requested_at),
	CHECK (tags IS NULL OR jsonb_typeof(tags) = 'array'),
	CHECK (status <> 'complete' OR (
		category_id IS NOT NULL AND category_name IS NOT NULL AND title IS NOT NULL
		AND language IS NOT NULL AND tags IS NOT NULL
	))
);

CREATE INDEX raid_source_context_run_id_idx ON raid_source_context (run_id);
