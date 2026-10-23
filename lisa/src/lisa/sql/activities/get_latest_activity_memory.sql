SELECT id, activity_id, seq, content, content_hash,
       source_event_seq, configuration_fingerprint, title_neutralized,
       is_terminal, created_at
FROM lisa.activity_context
WHERE activity_id = %(activity_id)s
  AND kind = 'chat_activity_memory'
  AND title_neutralized = %(title_neutralized)s
  AND (%(configuration_fingerprint)s::text IS NULL
       OR configuration_fingerprint = %(configuration_fingerprint)s)
ORDER BY source_event_seq DESC, seq DESC
LIMIT 1;
