CREATE TABLE follow_events (
	eventsub_message_id TEXT PRIMARY KEY,
	stream_id TEXT REFERENCES streams (stream_id),
	user_id TEXT NOT NULL,
	followed_at TIMESTAMPTZ NOT NULL,
	notification_at TIMESTAMPTZ NOT NULL,
	received_at TIMESTAMPTZ NOT NULL
);
