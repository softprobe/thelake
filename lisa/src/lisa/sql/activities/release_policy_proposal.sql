UPDATE lisa.policy_proposals
SET status = 'pending', applying_at = NULL
WHERE id = %(id)s AND status = 'applying';
