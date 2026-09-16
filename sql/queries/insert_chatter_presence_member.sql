INSERT INTO chatter_presence_members (snapshot_id, chatter_user_id)
VALUES (%(snapshot_id)s, %(chatter_user_id)s)
ON CONFLICT (snapshot_id, chatter_user_id) DO NOTHING;
