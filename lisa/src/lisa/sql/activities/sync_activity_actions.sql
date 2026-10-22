INSERT INTO lisa.activity_monitor_thread_actions
    (activity_id, seq, event_type, title, summary, actor,
     reference_type, reference_id, status, created_at)
SELECT events.activity_id, events.seq, events.event_type, events.title, events.summary,
       events.actor, events.reference_type, events.reference_id,
       CASE activity.status WHEN 'succeeded' THEN 'success' WHEN 'failed' THEN 'error'
            WHEN 'cancelled' THEN 'stopped' WHEN 'waiting' THEN 'blocked'
            WHEN 'waiting_approval' THEN 'blocked' ELSE 'pending' END,
       events.created_at
FROM lisa.activity_events AS events
JOIN lisa.activities AS activity ON activity.id = events.activity_id
WHERE events.activity_id = %(activity_id)s
ON CONFLICT (activity_id, seq) DO UPDATE SET
    reference_type = EXCLUDED.reference_type,
    reference_id = EXCLUDED.reference_id,
    status = EXCLUDED.status;
