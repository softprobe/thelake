SELECT EXISTS (
    SELECT 1
    FROM lisa.score_outbox AS outbox
    JOIN lisa.evaluation_runs AS runs ON runs.id = outbox.evaluation_id
    WHERE runs.activity_id = %(activity_id)s AND outbox.status = 'acknowledged'
) AS acknowledged;
