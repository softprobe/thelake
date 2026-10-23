UPDATE lisa.activity_feed_entries AS feed
SET subtitle = CASE WHEN activity.title_neutralized THEN NULL
                    ELSE COALESCE(%(subtitle)s::text, subtitle) END,
    status_title = CASE WHEN activity.title_neutralized THEN NULL
                        ELSE COALESCE(%(status_title)s::text, status_title) END,
    task_label = CASE WHEN activity.title_neutralized THEN NULL
                      ELSE COALESCE(%(task_label)s::text, task_label) END,
    details = CASE WHEN activity.title_neutralized THEN '{}'::jsonb
                   WHEN %(details)s::text IS NULL THEN details
                   ELSE %(details)s::jsonb END,
    updated_at = now()
FROM lisa.activities AS activity
WHERE feed.activity_id = %(activity_id)s
  AND activity.id = feed.activity_id;
