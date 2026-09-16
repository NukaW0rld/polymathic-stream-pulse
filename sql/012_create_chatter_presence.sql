BEGIN;

ALTER TABLE collection_health
DROP CONSTRAINT collection_health_source_check;

ALTER TABLE collection_health
ADD CONSTRAINT collection_health_source_check CHECK (
	source IN ('stream_poll', 'chat', 'raids', 'follows', 'chatter_presence')
);

CREATE TABLE chatter_presence_snapshots (
	snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	stream_id TEXT NOT NULL REFERENCES streams (stream_id),
	requested_at TIMESTAMPTZ NOT NULL,
	completed_at TIMESTAMPTZ,
	reported_total INTEGER CHECK (reported_total IS NULL OR reported_total >= 0),
	collected_distinct_count INTEGER CHECK (
		collected_distinct_count IS NULL OR collected_distinct_count >= 0
	),
	status TEXT NOT NULL CHECK (status IN ('in_progress', 'complete', 'partial', 'rejected', 'interrupted')),
	reason_code TEXT NOT NULL,
	reported_total_changed BOOLEAN,
	CHECK ((status = 'in_progress' AND completed_at IS NULL)
		OR (status <> 'in_progress' AND completed_at IS NOT NULL)),
	CHECK (completed_at IS NULL OR completed_at >= requested_at)
);

CREATE TABLE chatter_presence_members (
	snapshot_id BIGINT NOT NULL REFERENCES chatter_presence_snapshots (snapshot_id) ON DELETE CASCADE,
	chatter_user_id TEXT NOT NULL,
	PRIMARY KEY (snapshot_id, chatter_user_id)
);

CREATE UNIQUE INDEX chatter_presence_one_in_progress_per_run
ON chatter_presence_snapshots (run_id)
WHERE status = 'in_progress';

CREATE INDEX chatter_presence_snapshots_stream_time_idx
ON chatter_presence_snapshots (stream_id, requested_at);

COMMIT;
