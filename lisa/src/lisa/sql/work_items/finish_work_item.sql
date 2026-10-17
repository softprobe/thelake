WITH finished AS (
    UPDATE lisa.work_items AS wi
    SET status = %(status)s,
        summary = COALESCE(%(summary)s, summary),
        finished_at = now(),
        updated_at = now(),
        lease_owner = NULL,
        lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1,
        version = version + 1
    WHERE wi.id = %(work_item_id)s
      AND wi.status = 'running'
      AND wi.lease_owner = %(owner)s
      AND wi.fencing_token = %(fencing_token)s
      AND wi.lease_expires_at > clock_timestamp()
      AND (
          %(status)s <> 'succeeded'
          OR EXISTS (
              SELECT 1 FROM lisa.score_outbox AS outbox
              WHERE outbox.work_item_id = wi.id
                AND outbox.status = 'acknowledged'
          )
      )
    RETURNING wi.id, wi.next_event_seq - 1 AS seq
)
INSERT INTO lisa.work_events(work_item_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s
FROM finished
RETURNING work_item_id;
