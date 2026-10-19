INSERT INTO lisa.deployment(singleton, workspace_fingerprint)
VALUES (true, %(workspace_fingerprint)s)
ON CONFLICT (singleton) DO NOTHING;
