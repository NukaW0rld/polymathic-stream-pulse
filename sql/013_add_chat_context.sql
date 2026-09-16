ALTER TABLE chat_messages
ADD COLUMN message_type TEXT,
ADD COLUMN reply_parent_message_id TEXT,
ADD COLUMN reply_parent_user_id TEXT,
ADD COLUMN badges JSONB,
ADD COLUMN context_complete BOOLEAN;

ALTER TABLE chat_messages
ADD CONSTRAINT chat_context_shape CHECK (
	(context_complete IS NULL AND message_type IS NULL AND badges IS NULL
		AND reply_parent_message_id IS NULL AND reply_parent_user_id IS NULL)
	OR (context_complete = FALSE AND message_type IS NULL AND badges IS NULL
		AND reply_parent_message_id IS NULL AND reply_parent_user_id IS NULL)
	OR (context_complete = TRUE AND message_type IS NOT NULL AND badges IS NOT NULL
		AND ((reply_parent_message_id IS NULL AND reply_parent_user_id IS NULL)
			OR (reply_parent_message_id IS NOT NULL AND reply_parent_user_id IS NOT NULL)))
);
