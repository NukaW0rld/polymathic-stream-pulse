BEGIN;

ALTER TABLE collector_runs
ADD COLUMN collector_version TEXT;

CREATE TABLE collector_run_capabilities (
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	capability TEXT NOT NULL CHECK (capability IN (
		'stream_poll', 'chat', 'raids', 'follows', 'chatter_presence',
		'chat_context', 'stream_metadata_history', 'raid_source_context'
	)),
	status TEXT NOT NULL CHECK (status IN ('configured', 'disabled', 'failed_to_initialize')),
	observed_at TIMESTAMPTZ NOT NULL,
	reason_code TEXT NOT NULL,
	PRIMARY KEY (run_id, capability)
);

CREATE TABLE collector_run_streams (
	run_id BIGINT NOT NULL REFERENCES collector_runs (run_id),
	stream_id TEXT NOT NULL REFERENCES streams (stream_id),
	first_successful_observed_at TIMESTAMPTZ NOT NULL,
	last_successful_observed_at TIMESTAMPTZ NOT NULL,
	attribution_method TEXT NOT NULL CHECK (
		attribution_method IN ('direct_observation', 'historical_unique_time_match')
	),
	PRIMARY KEY (run_id, stream_id),
	CHECK (last_successful_observed_at >= first_successful_observed_at)
);

ALTER TABLE viewer_snapshots ADD COLUMN run_id BIGINT REFERENCES collector_runs (run_id);
ALTER TABLE chat_messages ADD COLUMN run_id BIGINT REFERENCES collector_runs (run_id);
ALTER TABLE incoming_raids ADD COLUMN run_id BIGINT REFERENCES collector_runs (run_id);
ALTER TABLE follow_events ADD COLUMN run_id BIGINT REFERENCES collector_runs (run_id);

CREATE INDEX viewer_snapshots_run_id_idx ON viewer_snapshots (run_id);
CREATE INDEX chat_messages_run_id_idx ON chat_messages (run_id);
CREATE INDEX incoming_raids_run_id_idx ON incoming_raids (run_id);
CREATE INDEX follow_events_run_id_idx ON follow_events (run_id);
CREATE INDEX collector_run_streams_stream_id_idx ON collector_run_streams (stream_id);

COMMIT;
