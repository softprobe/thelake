INSERT INTO lisa.policies(
    id, scope, target_agent_name, name, criteria, approved, archived_at
)
VALUES (
    COALESCE(
        (
            SELECT id
            FROM lisa.policies
            WHERE scope = %(scope)s
              AND target_agent_name = %(target_agent_name)s
              AND name = %(name)s
        ),
        format('P1.%%s', nextval('lisa.policy_paragraph_id_seq'))
    ),
    %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, %(approved)s, NULL
)
ON CONFLICT (scope, target_agent_name, name)
DO UPDATE SET
    criteria = EXCLUDED.criteria,
    approved = EXCLUDED.approved,
    archived_at = NULL,
    updated_at = now();
