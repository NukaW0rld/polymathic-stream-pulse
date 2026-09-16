INSERT INTO stream_metadata_history (
	stream_id, run_id, observed_at, title, category_id, category_name, language, tags
)
SELECT
	%(stream_id)s, %(run_id)s, %(observed_at)s, %(title)s, %(category_id)s,
	%(category_name)s, %(language)s, %(tags)s
WHERE NOT EXISTS (
	SELECT 1
	FROM stream_metadata_history previous
	WHERE previous.stream_id = %(stream_id)s
	ORDER BY previous.observed_at DESC, previous.metadata_id DESC
	LIMIT 1
)
OR EXISTS (
	SELECT 1
	FROM (
		SELECT title, category_id, category_name, language, tags
		FROM stream_metadata_history
		WHERE stream_id = %(stream_id)s
		ORDER BY observed_at DESC, metadata_id DESC
		LIMIT 1
	) previous
	WHERE (previous.title, previous.category_id, previous.category_name,
		previous.language, previous.tags)
		IS DISTINCT FROM
		(%(title)s, %(category_id)s, %(category_name)s, %(language)s, %(tags)s)
)
ON CONFLICT (stream_id, observed_at) DO NOTHING;
