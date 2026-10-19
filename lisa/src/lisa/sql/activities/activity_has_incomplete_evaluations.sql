SELECT EXISTS (
    SELECT 1
    FROM lisa.evaluation_runs AS runs
    WHERE runs.activity_id = %(activity_id)s
      AND (
          runs.verdict IS NULL
          OR EXISTS (
              SELECT 1 FROM lisa.score_outbox AS outbox
              WHERE outbox.evaluation_id = runs.id AND outbox.status = 'pending'
          )
      )
) AS has_incomplete;
