-- 0028_tvmaze_cache_kinds.sql
--
-- The TVmaze client shares tvdb_response_cache with the TVDb client (main.go
-- wires the same adapter) but caches under kinds this enum never had. Every
-- cache read and write failed, the client discarded the error, and the table
-- stayed empty: each enrichment pass re-queried TVmaze, and Postgres logged
-- about 4,000 errors a day.

ALTER TYPE tvdb_kind ADD VALUE IF NOT EXISTS 'tvmaze_search';
ALTER TYPE tvdb_kind ADD VALUE IF NOT EXISTS 'tvmaze_show';
