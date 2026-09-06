CREATE TABLE chat_messages (
  message_id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(stream_id),
  chatter_user_id TEXT NOT NULL,
  message_text TEXT NOT NULL,
  notification_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  message_fragments JSONB NOT NULL,
  source_broadcaster_user_id TEXT
);
