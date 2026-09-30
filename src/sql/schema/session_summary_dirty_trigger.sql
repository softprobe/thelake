DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'session_summary_dirty_bump_generation'
      AND tgrelid = to_regclass({{schema_literal}} || '.session_summary_dirty')
      AND NOT tgisinternal
  ) THEN
    CREATE TRIGGER session_summary_dirty_bump_generation
      BEFORE UPDATE ON {{schema}}.session_summary_dirty
      FOR EACH ROW EXECUTE FUNCTION {{schema}}.session_summary_dirty_bump_generation();
  END IF;
END $$;
