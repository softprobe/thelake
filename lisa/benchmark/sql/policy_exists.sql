SELECT EXISTS (
    SELECT 1
    FROM lisa.policies
    WHERE id = %(policy_id)s
);
