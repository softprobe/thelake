SELECT action, status, files_processed
FROM {{schema}}.maintenance_outcome
WHERE table_name = '*'
  AND action IN ('expire_snapshots', 'cleanup_scheduled_files')
ORDER BY action;
