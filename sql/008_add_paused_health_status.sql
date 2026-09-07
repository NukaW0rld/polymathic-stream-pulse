BEGIN;

ALTER TABLE collection_health
DROP CONSTRAINT collection_health_status_check;

ALTER TABLE collection_health
ADD CONSTRAINT collection_health_status_check CHECK (status IN ('starting', 'healthy', 'paused', 'error', 'stopped'));

COMMIT;
