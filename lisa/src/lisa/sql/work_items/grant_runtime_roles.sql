REVOKE ALL ON SCHEMA lisa FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA lisa FROM PUBLIC;
GRANT USAGE ON SCHEMA lisa TO {writer}, {reader};

GRANT SELECT, INSERT, UPDATE ON lisa.work_items TO {writer};
GRANT SELECT, INSERT, UPDATE ON lisa.evaluation_runs TO {writer};
GRANT SELECT, INSERT ON lisa.work_events TO {writer};
GRANT SELECT, INSERT ON lisa.work_context TO {writer};
GRANT SELECT, INSERT, UPDATE ON lisa.approval_requests TO {writer};
GRANT SELECT, INSERT, UPDATE ON lisa.score_outbox TO {writer};
GRANT SELECT, INSERT, UPDATE ON lisa.policies TO {writer};
GRANT USAGE, SELECT ON SEQUENCE lisa.policy_paragraph_id_seq TO {writer};
GRANT SELECT ON lisa.deployment TO {writer};
GRANT SELECT ON lisa.readable_work_items TO {reader};
GRANT SELECT ON lisa.readable_work_events TO {reader};
GRANT SELECT ON lisa.readable_work_context TO {reader};
GRANT SELECT ON lisa.readable_approvals TO {reader};
GRANT SELECT ON lisa.readable_evaluations TO {reader};
GRANT SELECT (id, scope, target_agent_name, name, criteria, approved,
              created_at, updated_at, archived_at) ON lisa.policies TO {reader};
GRANT SELECT ON lisa.readable_deployment TO {reader};
