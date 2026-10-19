-- Lisa operational schema. One clean definition; no upgrade migrations.
-- Applied only after DROP SCHEMA lisa CASCADE (see provision_lisa.py).

CREATE SCHEMA lisa;

CREATE SEQUENCE lisa.policy_paragraph_id_seq;

CREATE TABLE lisa.deployment (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    workspace_fingerprint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lisa.activities (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (length(trim(kind)) > 0),
    parent_id uuid REFERENCES lisa.activities(id),
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'waiting_approval', 'waiting', 'succeeded', 'failed', 'cancelled')),
    title text NOT NULL,
    summary text,
    source text NOT NULL,
    context_path text,
    lease_owner text,
    lease_expires_at timestamptz,
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    next_event_seq bigint NOT NULL DEFAULT 1 CHECK (next_event_seq > 0),
    next_context_seq bigint NOT NULL DEFAULT 1 CHECK (next_context_seq > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);

CREATE INDEX activities_status_updated_idx
    ON lisa.activities(status, updated_at DESC);
CREATE INDEX activities_parent_idx
    ON lisa.activities(parent_id) WHERE parent_id IS NOT NULL;

CREATE TABLE lisa.policies (
    id text PRIMARY KEY CHECK (id ~ '^P1[.][1-9][0-9]*$'),
    scope text NOT NULL CHECK (scope IN ('workspace', 'agent')),
    target_agent_name text NOT NULL DEFAULT '',
    name text NOT NULL,
    criteria text NOT NULL,
    approved boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz,
    CHECK (
        (scope = 'workspace' AND target_agent_name = '') OR
        (scope = 'agent' AND target_agent_name <> '')
    ),
    UNIQUE (scope, target_agent_name, name)
);

CREATE INDEX policies_scope_agent_approved_idx
    ON lisa.policies(scope, target_agent_name, approved)
    WHERE archived_at IS NULL;

CREATE TABLE lisa.evaluation_runs (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    policy_id text NOT NULL REFERENCES lisa.policies(id),
    target_agent_name text NOT NULL,
    session_id text NOT NULL,
    policy_name text NOT NULL,
    policy_hash text NOT NULL,
    judge_config jsonb NOT NULL,
    request_key uuid NOT NULL,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    score_id text,
    verdict text CHECK (verdict IS NULL OR verdict IN (
        'pass', 'fail', 'uncertain', 'insufficient_evidence'
    )),
    started_at timestamptz,
    finished_at timestamptz,
    UNIQUE (policy_hash, session_id, request_key)
);

CREATE TABLE lisa.evaluation_triage (
    session_id text NOT NULL,
    policy_id text NOT NULL REFERENCES lisa.policies(id),
    decision text NOT NULL CHECK (decision IN (
        'suspected_violation', 'no_violation_found', 'insufficient_evidence'
    )),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, policy_id)
);

CREATE TABLE lisa.activity_events (
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    event_type text NOT NULL,
    title text NOT NULL,
    summary text,
    actor text NOT NULL,
    payload_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_id, seq)
);

CREATE TABLE lisa.activity_context (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    kind text NOT NULL,
    content jsonb NOT NULL,
    source_ref text,
    content_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (activity_id, seq)
);

CREATE TABLE lisa.approval_requests (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    action jsonb NOT NULL,
    action_hash text NOT NULL,
    scope text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'revoked', 'expired', 'consumed')),
    request_summary text NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    resolved_at timestamptz,
    resolved_by text,
    decision_ref text,
    consumed_at timestamptz
);

CREATE TABLE lisa.score_outbox (
    id uuid PRIMARY KEY,
    evaluation_id uuid NOT NULL REFERENCES lisa.evaluation_runs(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL UNIQUE,
    score_payload jsonb NOT NULL,
    payload_hash text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'acknowledged')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    score_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz
);

CREATE VIEW lisa.readable_activities AS
SELECT id, kind, parent_id, status, title, summary, source,
       created_at, started_at, updated_at, finished_at
FROM lisa.activities;

CREATE VIEW lisa.readable_activity_events AS
SELECT activity_id, seq, event_type, title, summary, actor, created_at
FROM lisa.activity_events;

CREATE VIEW lisa.readable_activity_context AS
SELECT id, activity_id, seq, kind, source_ref, content_hash, created_at
FROM lisa.activity_context;

CREATE VIEW lisa.readable_approvals AS
SELECT id, activity_id, scope, status, request_summary,
       requested_at, expires_at, resolved_at, resolved_by, decision_ref
FROM lisa.approval_requests;

CREATE VIEW lisa.readable_evaluations AS
SELECT activity_id, session_id, policy_id, id AS evaluation_id,
       target_agent_name, policy_name, verdict, score_id, started_at, finished_at
FROM lisa.evaluation_runs;

CREATE VIEW lisa.readable_evaluation_triage AS
SELECT session_id, policy_id, decision, created_at
FROM lisa.evaluation_triage;

CREATE VIEW lisa.readable_deployment AS
SELECT workspace_fingerprint FROM lisa.deployment;
