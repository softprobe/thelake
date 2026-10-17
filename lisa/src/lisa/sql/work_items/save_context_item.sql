WITH allocated AS (
    UPDATE lisa.work_items
    SET next_context_seq = next_context_seq + 1,
        updated_at = now(),
        version = version + 1
    WHERE id = %(work_item_id)s
      AND status = 'running'
      AND lease_owner = %(owner)s
      AND fencing_token = %(fencing_token)s
      AND lease_expires_at > clock_timestamp()
    RETURNING next_context_seq - 1 AS seq
)
INSERT INTO lisa.work_context(id, work_item_id, seq, kind, content, source_ref, content_hash)
SELECT %(id)s, %(work_item_id)s, allocated.seq, %(kind)s,
       %(content)s::jsonb, %(source_ref)s, %(content_hash)s
FROM allocated
RETURNING id;
