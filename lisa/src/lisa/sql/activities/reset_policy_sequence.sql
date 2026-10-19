SELECT setval(
    'lisa.policy_paragraph_id_seq',
    COALESCE(max(substring(id FROM 4)::bigint), 1),
    count(*) > 0
)
FROM lisa.policies;
