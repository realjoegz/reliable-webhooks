-- The server, and the settings that the numbers of a run depend on most. compose.yaml changes
-- none of them, so they are what the image sets.
SELECT version();

SELECT name, setting, unit
FROM pg_settings
WHERE name IN ('autovacuum', 'autovacuum_naptime', 'checkpoint_timeout', 'effective_cache_size',
               'fsync', 'full_page_writes', 'max_connections', 'max_wal_size', 'min_wal_size',
               'shared_buffers', 'synchronous_commit', 'wal_sync_method', 'work_mem')
ORDER BY name;
