-- 0029_dvr_volume_and_cache_hygiene.sql
--
-- 1. Persist a fast stat-metadata signature on validated DVR artifacts and a
--    cryptographically proven dev:<st_dev> -> vol:<uuid> volume alias table so
--    rebooting a host (which can shift minor st_dev numbers across mounts)
--    rebinds validated/legacy/missing artifacts cleanly instead of stranding
--    current owners in indeterminate inspection errors.
-- 2. Index cache/audit expiry columns and tighten autovacuum scale factors on
--    high-churn EPG and response-cache tables.

ALTER TABLE dvr_recording
    ADD COLUMN IF NOT EXISTS artifact_stat_sig text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS dvr_filesystem_alias (
    legacy_filesystem_id text PRIMARY KEY CHECK (legacy_filesystem_id <> ''),
    volume_filesystem_id text NOT NULL CHECK (volume_filesystem_id LIKE 'vol:%'),
    proven_by_recording_id uuid NOT NULL REFERENCES dvr_recording(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tmdb_response_cache_expires_idx
    ON tmdb_response_cache (expires_at);

CREATE INDEX IF NOT EXISTS tvdb_response_cache_expires_idx
    ON tvdb_response_cache (expires_at);

ALTER TABLE epg_program SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE tmdb_response_cache SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE tvdb_response_cache SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE dvr_indexer_search_log SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
