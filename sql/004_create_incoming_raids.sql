CREATE TABLE incoming_raids (
	eventsub_message_id TEXT PRIMARY KEY,
	stream_id TEXT REFERENCES streams (stream_id),
	from_broadcaster_user_id TEXT NOT NULL,
	raid_viewer_count INTEGER NOT NULL CHECK (raid_viewer_count >= 0),
	notification_at TIMESTAMPTZ NOT NULL,
	received_at TIMESTAMPTZ NOT NULL
);
