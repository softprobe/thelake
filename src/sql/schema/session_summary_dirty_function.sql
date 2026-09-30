CREATE OR REPLACE FUNCTION {{schema}}.session_summary_dirty_bump_generation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.generation := OLD.generation + 1; RETURN NEW; END;
$$;
