WITH shared_activities AS MATERIALIZED (
    SELECT activity_id,
           'Task ' || row_number() OVER (
               ORDER BY created_at, activity_id
           )::text AS safe_title
    FROM (
        SELECT DISTINCT activity_id, min(created_at) AS created_at
        FROM lisa.activity_chat_turns
        WHERE turn_id = %(turn_id)s
        GROUP BY activity_id
    ) AS activities
), sanitized_activities AS (
    UPDATE lisa.activities AS activity
    SET title = shared.safe_title,
        summary = shared.safe_title,
        title_neutralized = true,
        updated_at = now(),
        version = activity.version + 1
    FROM shared_activities AS shared
    WHERE activity.id = shared.activity_id
    RETURNING activity.id
), sanitized_feed AS (
    UPDATE lisa.activity_feed_entries AS feed
    SET subtitle = NULL,
        status_title = NULL,
        task_label = NULL,
        details = '{}'::jsonb,
        updated_at = now()
    FROM shared_activities AS shared
    WHERE feed.activity_id = shared.activity_id
    RETURNING feed.activity_id
), sanitized_threads AS (
    UPDATE lisa.activity_monitor_threads AS thread
    SET details = '{}'::jsonb,
        updated_at = now()
    FROM shared_activities AS shared
    WHERE thread.activity_id = shared.activity_id
    RETURNING thread.activity_id
), sanitized_events AS (
    -- A progress event can be written before the resolver sees a second task,
    -- and its title is copied from the then-unsanitized activity title. An
    -- update_activity call can also repeat that title in its raw arguments.
    -- Once the turn is known to cover multiple tasks, neutralize those control
    -- fields while retaining the progress summary written to this activity.
    UPDATE lisa.activity_events AS event
    SET title = CASE
            WHEN event.event_type = 'accepted' THEN shared.safe_title
            WHEN event.event_type = 'chat.request' THEN 'Task request recorded'
            WHEN event.event_type = 'progress' THEN 'Shared progress update withheld'
            WHEN event.event_type IN ('runtime.tool', 'runtime.result')
                 AND event.title ILIKE '%%update_activity%%'
                THEN 'Activity presentation write withheld'
            WHEN event.event_type IN ('chat.user', 'chat.assistant', 'chat.progress')
                THEN 'Shared chat context withheld'
            ELSE 'Shared runtime event withheld'
        END,
        summary = CASE
            WHEN event.event_type = 'progress' THEN event.summary
            ELSE NULL
        END
    FROM shared_activities AS shared
    WHERE event.activity_id = shared.activity_id
      AND (
          event.event_type = 'accepted'
          OR (
              event.chat_turn_id = %(turn_id)s
              AND event.event_type IN (
                  'chat.request', 'chat.user', 'chat.assistant', 'chat.progress', 'progress'
              )
          )
          OR (
              event.chat_turn_id = %(turn_id)s
              AND event.event_type IN ('runtime.tool', 'runtime.result')
              AND event.title ILIKE '%%update_activity%%'
          )
          OR (
              event.chat_turn_id = %(turn_id)s
              AND event.event_type IN ('succeeded', 'failed', 'cancelled')
          )
          OR (
              event.chat_turn_id = %(turn_id)s
              AND event.event_type IN (
                  'runtime.tool', 'runtime.result', 'succeeded', 'failed', 'cancelled'
              )
              AND EXISTS (
                  SELECT 1
                  FROM lisa.activity_events AS duplicate
                  WHERE duplicate.activity_id <> event.activity_id
                    AND duplicate.chat_turn_id = event.chat_turn_id
                    AND duplicate.event_type = event.event_type
                    AND (
                        (
                            event.reference_type IS NOT NULL
                            AND duplicate.reference_type = event.reference_type
                            AND duplicate.reference_id = event.reference_id
                        )
                        OR (
                            event.reference_type IS NULL
                            AND duplicate.reference_type IS NULL
                            AND duplicate.title = event.title
                            AND coalesce(duplicate.summary, '') = coalesce(event.summary, '')
                            AND duplicate.actor = event.actor
                        )
                    )
              )
          )
      )
    RETURNING event.activity_id, event.seq, event.title, event.summary
), sanitized_actions AS (
    -- Thread reads use this rebuildable projection, so sanitize it from the
    -- exact rows returned by the canonical event update in the same transaction.
    UPDATE lisa.activity_monitor_thread_actions AS action
    SET title = safe_event.title,
        summary = safe_event.summary
    FROM sanitized_events AS safe_event
    WHERE action.activity_id = safe_event.activity_id
      AND action.seq = safe_event.seq
    RETURNING action.activity_id
)
SELECT count(*) FROM sanitized_activities;
