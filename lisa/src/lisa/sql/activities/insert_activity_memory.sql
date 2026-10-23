WITH allocated AS (
    UPDATE lisa.activities
    SET next_context_seq = next_context_seq + 1
    WHERE id = %(activity_id)s
    RETURNING next_context_seq - 1 AS seq
)
INSERT INTO lisa.activity_context(
    id, activity_id, seq, kind, content, source_ref, content_hash,
    source_event_seq, configuration_fingerprint, title_neutralized, is_terminal
)
SELECT %(id)s, %(activity_id)s, allocated.seq, 'chat_activity_memory',
       %(content)s::jsonb, %(source_ref)s, %(content_hash)s,
       %(source_event_seq)s, %(configuration_fingerprint)s,
       %(title_neutralized)s, %(is_terminal)s
FROM allocated
ON CONFLICT (
    activity_id, source_event_seq, configuration_fingerprint, title_neutralized
)
    WHERE kind = 'chat_activity_memory'
DO NOTHING
RETURNING id, seq, content, content_hash,
          source_event_seq, configuration_fingerprint, title_neutralized,
          is_terminal, created_at;
