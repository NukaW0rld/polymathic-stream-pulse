BEGIN;

ALTER TABLE chat_messages
RENAME COLUMN message_id TO eventsub_message_id;

ALTER TABLE chat_messages
ADD COLUMN chat_message_id TEXT NOT NULL;

COMMIT;
