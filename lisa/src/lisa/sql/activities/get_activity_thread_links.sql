SELECT link.reference_type, link.reference_id
FROM lisa.readable_activity_monitor_links AS link
WHERE link.activity_id = %(activity_id)s
  AND NOT (
      link.reference_type = 'runtime_event'
      AND EXISTS (
          SELECT 1
          FROM lisa.activity_events AS event
          WHERE event.activity_id = link.activity_id
            AND event.reference_type = link.reference_type
            AND event.reference_id = link.reference_id
            AND event.event_type IN ('runtime.tool', 'runtime.result')
            AND event.title ILIKE '%%resolve_chat_activity%%'
            AND coalesce(event.reference_id, '') NOT LIKE 'fallback-resolution:%%'
      )
  )
ORDER BY link.reference_type, link.reference_id;
