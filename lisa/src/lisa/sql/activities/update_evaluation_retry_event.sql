WITH updated AS (
    UPDATE lisa.activity_events
    SET event_type = 'session_evaluation_attempt_failed',
        title = %(title)s,
        summary = %(summary)s,
        actor = 'lisa-session-evaluation-worker',
        reference_type = 'session_evaluation_attempt',
        reference_id = %(reference_id)s
    WHERE activity_id = %(activity_id)s AND seq = %(seq)s
    RETURNING activity_id, seq, event_type, title, summary, actor,
              reference_type, reference_id
)
UPDATE lisa.activity_monitor_thread_actions AS action
SET event_type = updated.event_type,
    title = updated.title,
    summary = updated.summary,
    actor = updated.actor,
    reference_type = updated.reference_type,
    reference_id = updated.reference_id
FROM updated
WHERE action.activity_id = updated.activity_id AND action.seq = updated.seq;
