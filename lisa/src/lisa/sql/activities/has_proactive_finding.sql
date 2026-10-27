WITH findings AS (
    SELECT evaluation.activity_id,
           jsonb_agg(
               outbox.score_payload ->> 'comment'
               ORDER BY evaluation.finished_at, evaluation.id
           ) AS messages
    FROM lisa.evaluation_runs AS evaluation
    JOIN lisa.score_outbox AS outbox ON outbox.evaluation_id = evaluation.id
    WHERE evaluation.activity_id = %(activity_id)s
      AND evaluation.verdict = 'fail'
      AND outbox.status = 'acknowledged'
      AND NULLIF(outbox.score_payload ->> 'comment', '') IS NOT NULL
    GROUP BY evaluation.activity_id
)
SELECT activity.source,
       findings.messages IS NOT NULL AS has_finding,
       COALESCE(findings.messages, '[]'::jsonb) AS messages
FROM lisa.activities AS activity
LEFT JOIN findings ON findings.activity_id = activity.id
WHERE activity.id = %(activity_id)s;
