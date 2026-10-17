SELECT EXISTS (
    SELECT 1
    FROM lisa.score_outbox
    WHERE work_item_id = %(work_item_id)s
      AND status = 'acknowledged'
) AS acknowledged;
