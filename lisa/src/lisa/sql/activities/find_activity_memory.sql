SELECT id, seq, content, content_hash, source_event_seq,
       configuration_fingerprint, title_neutralized, is_terminal, created_at
FROM lisa.activity_context
WHERE activity_id = %(activity_id)s
  AND kind = 'chat_activity_memory'
  AND source_event_seq = %(event_watermark)s
  AND configuration_fingerprint = %(configuration_fingerprint)s
  AND title_neutralized = %(title_neutralized)s
LIMIT 1;
