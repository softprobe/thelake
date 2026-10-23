WITH eligible AS (
    SELECT activity_id, seq, event_type, title, summary, actor,
           reference_type, reference_id, created_at, chat_turn_id,
           count(*) OVER () AS _activity_event_count
    FROM lisa.activity_events
    WHERE activity_id = %(activity_id)s
      AND seq > %(after_event_seq)s
      AND seq <= %(event_watermark)s
      AND (
          %(sequences)s::bigint[] IS NULL
          OR seq = ANY(%(sequences)s::bigint[])
      )
      AND event_type <> 'accepted'
      AND event_type <> 'chat.request'
      AND NOT (
          event_type IN ('runtime.tool', 'runtime.result')
          AND title ILIKE '%%resolve_chat_activity%%'
          AND coalesce(reference_id, '') NOT LIKE 'fallback-resolution:%%'
      )
      AND NOT (
          chat_turn_id IS NOT NULL
          AND event_type IN ('chat.user', 'chat.assistant', 'chat.progress')
          AND (
              SELECT count(DISTINCT activity_id)
              FROM lisa.activity_chat_turns AS resolved
              WHERE resolved.turn_id = activity_events.chat_turn_id
          ) > 1
      )
      AND NOT (
          chat_turn_id IS NOT NULL
          AND event_type IN (
              'runtime.tool', 'runtime.result', 'succeeded', 'failed', 'cancelled'
          )
          AND EXISTS (
              SELECT 1
              FROM lisa.activity_events AS duplicate
              JOIN lisa.activity_chat_turns AS resolved
                ON resolved.activity_id = duplicate.activity_id
               AND resolved.turn_id = activity_events.chat_turn_id
              WHERE duplicate.activity_id <> activity_events.activity_id
                AND duplicate.chat_turn_id = activity_events.chat_turn_id
                AND duplicate.event_type = activity_events.event_type
                AND (
                    (
                        activity_events.reference_type IS NOT NULL
                        AND duplicate.reference_type = activity_events.reference_type
                        AND duplicate.reference_id = activity_events.reference_id
                    )
                    OR (
                        activity_events.reference_type IS NULL
                        AND duplicate.reference_type IS NULL
                        AND duplicate.title = activity_events.title
                        AND coalesce(duplicate.summary, '') =
                            coalesce(activity_events.summary, '')
                        AND duplicate.actor = activity_events.actor
                    )
                )
          )
      )
), recent AS (
    SELECT *
    FROM eligible
    ORDER BY seq DESC
    LIMIT %(limit)s
)
SELECT *
FROM recent
ORDER BY seq;
