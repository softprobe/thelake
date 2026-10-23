SELECT action.seq, action.event_type, action.title, action.summary, action.actor,
       action.reference_type, action.reference_id, action.status, action.created_at
FROM lisa.activity_monitor_thread_actions AS action
LEFT JOIN lisa.activity_events AS event
  ON event.activity_id = action.activity_id AND event.seq = action.seq
WHERE action.activity_id = %(activity_id)s
  AND NOT (
      event.event_type IN ('runtime.tool', 'runtime.result')
      AND event.title ILIKE '%%resolve_chat_activity%%'
      AND coalesce(event.reference_id, '') NOT LIKE 'fallback-resolution:%%'
  )
  AND NOT (
      event.chat_turn_id IS NOT NULL
      AND event.event_type IN (
          'chat.request', 'chat.user', 'chat.assistant', 'chat.progress'
      )
      AND (
          SELECT count(DISTINCT resolved.activity_id)
          FROM lisa.activity_chat_turns AS resolved
          WHERE resolved.turn_id = event.chat_turn_id
      ) > 1
  )
  AND NOT (
      event.chat_turn_id IS NOT NULL
      AND event.event_type IN (
          'runtime.tool', 'runtime.result', 'succeeded', 'failed', 'cancelled'
      )
      AND EXISTS (
          SELECT 1
          FROM lisa.activity_events AS duplicate
          JOIN lisa.activity_chat_turns AS resolved
            ON resolved.activity_id = duplicate.activity_id
           AND resolved.turn_id = event.chat_turn_id
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
ORDER BY action.seq;
