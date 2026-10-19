SELECT seq, event_type, title, summary, actor, status, created_at
FROM lisa.activity_monitor_thread_actions
WHERE activity_id = %(activity_id)s
ORDER BY seq;
