SELECT activity.source,
       EXISTS (
           SELECT 1
           FROM lisa.evaluation_runs AS run
           WHERE run.activity_id = activity.id
             AND run.verdict = 'fail'
       ) AS has_finding
FROM lisa.activities AS activity
WHERE activity.id = %(activity_id)s;
