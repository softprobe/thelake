SELECT data_type
FROM information_schema.columns
WHERE table_schema = 'lisa'
  AND table_name = 'policies'
  AND column_name = 'id';
