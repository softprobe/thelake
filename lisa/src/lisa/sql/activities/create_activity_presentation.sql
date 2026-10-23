INSERT INTO lisa.activity_feed_entries AS feed
    (activity_id, activity_type, search_title, is_goal, message_id, subtitle,
     status_title, task_label, agent_id, details)
VALUES
    (%(activity_id)s, %(activity_type)s, %(search_title)s, %(is_goal)s, %(message_id)s,
     %(subtitle)s, %(status_title)s, %(task_label)s, %(agent_id)s, %(details)s::jsonb)
ON CONFLICT (activity_id) DO UPDATE SET
    activity_type = EXCLUDED.activity_type,
    search_title = EXCLUDED.search_title,
    is_goal = EXCLUDED.is_goal,
    message_id = EXCLUDED.message_id,
    subtitle = CASE WHEN EXISTS (
                        SELECT 1 FROM lisa.activities AS activity
                        WHERE activity.id = feed.activity_id AND activity.title_neutralized
                    )
                    THEN NULL ELSE EXCLUDED.subtitle END,
    status_title = CASE WHEN EXISTS (
                            SELECT 1 FROM lisa.activities AS activity
                            WHERE activity.id = feed.activity_id AND activity.title_neutralized
                        )
                        THEN NULL ELSE EXCLUDED.status_title END,
    task_label = CASE WHEN EXISTS (
                          SELECT 1 FROM lisa.activities AS activity
                          WHERE activity.id = feed.activity_id AND activity.title_neutralized
                      )
                      THEN NULL ELSE EXCLUDED.task_label END,
    agent_id = EXCLUDED.agent_id,
    details = CASE WHEN EXISTS (
                        SELECT 1 FROM lisa.activities AS activity
                        WHERE activity.id = feed.activity_id AND activity.title_neutralized
                    )
                   THEN '{}'::jsonb ELSE EXCLUDED.details END,
    updated_at = now();
