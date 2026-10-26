-- Lisa operational schema, recreated as one complete definition.
-- Applied only after DROP SCHEMA lisa CASCADE (see provision_lisa.py).
--
-- This schema belongs to one workspace-bound Lisa deployment. Workspace-facing
-- records below are therefore isolated by the deployment, not by workspace_id
-- columns. The table comments distinguish those records from Lisa's runtime
-- bookkeeping and UI projections.

CREATE SCHEMA lisa;

-- Lisa deployment identity; prevents this database from being used for another
-- workspace by mistake.
CREATE TABLE lisa.deployment (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    workspace_fingerprint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Canonical workspace-visible record and lifecycle state for each Lisa task or
-- request; the activity event log and presentation rows attach to this record.
CREATE TABLE lisa.activities (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (length(trim(kind)) > 0),
    parent_id uuid REFERENCES lisa.activities(id),
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'waiting_approval', 'waiting', 'succeeded', 'failed', 'cancelled')),
    title text NOT NULL,
    -- Once a shared chat turn has made the public title neutral, later title
    -- and card writes must preserve that privacy boundary.
    title_neutralized boolean NOT NULL DEFAULT false,
    summary text,
    source text NOT NULL,
    next_action text,
    next_action_at timestamptz,
    origin_conversation_id text,
    origin_slack_channel_id text,
    origin_slack_thread_ts text,
    slack_channel_id text,
    slack_thread_ts text,
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
CREATE INDEX activities_next_action_idx
    ON lisa.activities(next_action_at) WHERE next_action_at IS NOT NULL;
CREATE INDEX activities_parent_idx
    ON lisa.activities(parent_id) WHERE parent_id IS NOT NULL;

-- Workspace policy catalog, including approval state. This is policy source
-- of truth, not a transient Lisa prompt cache.
CREATE TABLE lisa.policies (
    id text PRIMARY KEY CHECK (id ~ '^P1[.][1-9][0-9]*([.][1-9][0-9]*)?$'),
    scope text NOT NULL CHECK (scope IN ('workspace', 'agent')),
    target_agent_name text NOT NULL DEFAULT '',
    name text NOT NULL,
    criteria text NOT NULL,
    approved boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (scope = 'workspace' AND target_agent_name = '') OR
        (scope = 'agent' AND target_agent_name <> '')
    ),
    UNIQUE (scope, target_agent_name, name)
);

CREATE INDEX policies_scope_agent_approved_idx
    ON lisa.policies(scope, target_agent_name, approved);

-- Workspace QA history: the judge result and the policy snapshot/configuration
-- used for that run, linked to the activity that requested it.
CREATE TABLE lisa.evaluation_runs (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    policy_id text NOT NULL,
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

-- Lisa-owned routing cache for workspace evidence, keyed by session/policy pair.
CREATE TABLE lisa.evaluation_triage (
    session_id text NOT NULL,
    policy_id text NOT NULL REFERENCES lisa.policies(id),
    decision text NOT NULL CHECK (decision IN (
        'suspected_violation', 'no_violation_found', 'insufficient_evidence'
    )),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, policy_id)
);

-- Workspace-visible audit timeline for actions and outcomes on an activity.
CREATE TABLE lisa.activity_events (
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    chat_turn_id text,
    event_type text NOT NULL,
    title text NOT NULL,
    summary text,
    actor text NOT NULL,
    reference_type text,
    reference_id text,
    payload_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_id, seq),
    CHECK ((reference_type IS NULL) = (reference_id IS NULL))
);
CREATE UNIQUE INDEX activity_events_reference_idx
    ON lisa.activity_events(activity_id, reference_type, reference_id)
    WHERE reference_id IS NOT NULL;

CREATE UNIQUE INDEX activity_events_runtime_reference_unique
    ON lisa.activity_events(reference_type, reference_id)
    WHERE reference_type = 'runtime_event' AND reference_id IS NOT NULL;

-- Feed and monitor projections. Lisa's lifecycle tables remain authoritative.
-- Rebuildable workspace activity-feed card metadata for the UI.
CREATE TABLE lisa.activity_feed_entries (
    activity_id uuid PRIMARY KEY REFERENCES lisa.activities(id) ON DELETE CASCADE,
    activity_type text NOT NULL,
    search_title text NOT NULL,
    is_goal boolean NOT NULL DEFAULT false,
    message_id text,
    subtitle text,
    status_title text,
    task_label text,
    agent_id text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX activity_feed_recent_idx ON lisa.activity_feed_entries(updated_at DESC);

-- Lisa chat-routing/idempotency bookkeeping: maps each request in a turn to its
-- resolved activity and records whether it created, continued, or fell back.
CREATE TABLE lisa.activity_chat_turns (
    turn_id text NOT NULL,
    request_key text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    disposition text NOT NULL CHECK (disposition IN ('create', 'continue', 'fallback')),
    is_active boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (turn_id, request_key)
);
CREATE INDEX activity_chat_turn_activity_idx ON lisa.activity_chat_turns(activity_id, turn_id);

-- Rebuildable Lisa monitor-thread presentation metadata for an activity.
CREATE TABLE lisa.activity_monitor_threads (
    activity_id uuid PRIMARY KEY REFERENCES lisa.activities(id) ON DELETE CASCADE,
    icon text NOT NULL DEFAULT 'activity',
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
-- Rebuildable ordered section projection for a monitor thread.
CREATE TABLE lisa.activity_monitor_thread_sections (
    activity_id uuid NOT NULL REFERENCES lisa.activity_monitor_threads(activity_id) ON DELETE CASCADE,
    section_id text NOT NULL,
    title text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'pending',
    section_order integer NOT NULL DEFAULT 0,
    section_depth integer,
    parent_section_key text,
    ordinal_label text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_id, section_id)
);
-- Rebuildable monitor action projection; canonical action history is in
-- lisa.activity_events.
CREATE TABLE lisa.activity_monitor_thread_actions (
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    event_type text NOT NULL,
    title text NOT NULL,
    summary text,
    actor text NOT NULL,
    reference_type text,
    reference_id text,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL,
    PRIMARY KEY (activity_id, seq)
);
-- Lisa lookup link from an external agent ID to the activity thread.
CREATE TABLE lisa.activity_monitor_agent_threads (
    agent_id text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id, activity_id)
);
-- Lisa lookup link from an external message ID to the activity thread.
CREATE TABLE lisa.activity_monitor_message_threads (
    message_id text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    PRIMARY KEY (message_id, activity_id)
);
-- Lisa lookup link from a runtime work ID to the activity thread.
CREATE TABLE lisa.activity_monitor_runtime_work_threads (
    runtime_work_id text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    PRIMARY KEY (runtime_work_id, activity_id)
);
-- Lisa lookup link from a runtime event ID to the activity thread.
CREATE TABLE lisa.activity_monitor_runtime_event_threads (
    runtime_event_id text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    PRIMARY KEY (runtime_event_id, activity_id)
);
CREATE UNIQUE INDEX activity_monitor_runtime_event_global_idx
    ON lisa.activity_monitor_runtime_event_threads(runtime_event_id);
-- Lisa lookup link from a carrier message ID to the activity thread.
CREATE TABLE lisa.activity_monitor_carrier_user_messages (
    carrier_message_id text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    PRIMARY KEY (carrier_message_id, activity_id)
);

-- Workspace task inputs/checkpoints. Content may be sensitive; larger payloads
-- can live in Lisa's private activity-artifact directory via source_ref.
CREATE TABLE lisa.activity_context (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    kind text NOT NULL,
    content jsonb NOT NULL,
    source_ref text,
    content_hash text NOT NULL,
    source_event_seq bigint,
    configuration_fingerprint text,
    -- A neutralized snapshot is a distinct immutable privacy generation. This
    -- lets a safe replacement publish at the same event watermark.
    title_neutralized boolean NOT NULL DEFAULT false,
    is_terminal boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (activity_id, seq),
    CHECK (source_event_seq IS NULL OR source_event_seq >= 0),
    CHECK ((kind = 'chat_activity_memory') =
           (source_event_seq IS NOT NULL AND configuration_fingerprint IS NOT NULL))
);
CREATE UNIQUE INDEX activity_context_chat_memory_identity_idx
    ON lisa.activity_context(
        activity_id, source_event_seq, configuration_fingerprint, title_neutralized
    )
    WHERE kind = 'chat_activity_memory';

-- Lisa-owned write-ahead journal for idempotent local result recovery.
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

-- Read-only, bounded inspection surface for lisa_db. These views intentionally
-- expose selected columns/projections; the agent's reader role does not get
-- direct access to the underlying tables.
CREATE VIEW lisa.readable_activities AS
SELECT id, kind, parent_id, status, title, summary, source,
       created_at, started_at, updated_at, finished_at
FROM lisa.activities;

CREATE VIEW lisa.readable_activity_events AS
SELECT activity_id, seq, event_type, title, summary, actor,
       reference_type, reference_id, created_at
FROM lisa.activity_events;

CREATE VIEW lisa.readable_activity_feed AS
SELECT a.id AS activity_id, f.activity_type, f.is_goal, f.message_id,
       f.subtitle, f.status_title, f.task_label, f.agent_id, f.details,
       a.kind, a.status, a.title, a.summary, a.source,
       a.created_at, a.started_at, a.updated_at, a.finished_at,
       a.next_action, a.next_action_at, a.origin_conversation_id,
       a.origin_slack_channel_id, a.origin_slack_thread_ts,
       a.slack_channel_id, a.slack_thread_ts
FROM lisa.activities AS a
JOIN lisa.activity_feed_entries AS f ON f.activity_id = a.id;

CREATE VIEW lisa.readable_activity_monitor_threads AS
SELECT thread.activity_id, thread.icon, thread.details,
       section.section_id, section.title AS section_title,
       section.status AS section_status, section.section_order
FROM lisa.activity_monitor_threads AS thread
LEFT JOIN lisa.activity_monitor_thread_sections AS section
  ON section.activity_id = thread.activity_id;

CREATE VIEW lisa.readable_activity_monitor_thread_actions AS
SELECT activity_id, seq, event_type, title, summary, actor,
       reference_type, reference_id, status, created_at
FROM lisa.activity_monitor_thread_actions;

CREATE VIEW lisa.readable_activity_monitor_links AS
SELECT activity_id, 'agent'::text AS reference_type, agent_id AS reference_id
FROM lisa.activity_monitor_agent_threads
UNION ALL
SELECT activity_id, 'message'::text, message_id
FROM lisa.activity_monitor_message_threads
UNION ALL
SELECT activity_id, 'runtime_work'::text, runtime_work_id
FROM lisa.activity_monitor_runtime_work_threads
UNION ALL
SELECT activity_id, 'runtime_event'::text, runtime_event_id
FROM lisa.activity_monitor_runtime_event_threads
UNION ALL
SELECT activity_id, 'carrier_message'::text, carrier_message_id
FROM lisa.activity_monitor_carrier_user_messages;

CREATE VIEW lisa.readable_activity_context AS
SELECT id, activity_id, seq, kind, source_ref, content_hash, created_at
FROM lisa.activity_context;

CREATE VIEW lisa.readable_evaluations AS
SELECT activity_id, session_id, policy_id, id AS evaluation_id,
       target_agent_name, policy_name, verdict, score_id, started_at, finished_at
FROM lisa.evaluation_runs;

CREATE VIEW lisa.readable_evaluation_triage AS
SELECT session_id, policy_id, decision, created_at
FROM lisa.evaluation_triage;

CREATE VIEW lisa.readable_deployment AS
SELECT workspace_fingerprint FROM lisa.deployment;

-- Bounded, evidence-linked policy discovery. These records are private service
-- state and are exposed only through Lisa's purpose-built API projections.
CREATE TABLE lisa.policy_discovery_samples (
    session_id text PRIMARY KEY,
    agent_name text NOT NULL,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    sampled_hour timestamptz NOT NULL,
    evaluation_complete boolean NOT NULL DEFAULT false,
    gap_reviewed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX policy_discovery_samples_hour_idx
    ON lisa.policy_discovery_samples(sampled_hour);
CREATE INDEX policy_discovery_samples_pending_idx
    ON lisa.policy_discovery_samples(sampled_hour)
    WHERE evaluation_complete AND NOT gap_reviewed;

CREATE TABLE lisa.policy_discovery_signals (
    id uuid PRIMARY KEY,
    session_id text NOT NULL UNIQUE,
    activity_id uuid NOT NULL REFERENCES lisa.activities(id) ON DELETE CASCADE,
    agent_name text NOT NULL,
    summary text NOT NULL,
    span_ids jsonb NOT NULL,
    evidence jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'synthesized', 'reviewed', 'dismissed')),
    suggestion_id uuid,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX policy_discovery_signals_pending_idx
    ON lisa.policy_discovery_signals(created_at) WHERE state = 'pending';

CREATE TABLE lisa.policy_suggestions (
    id uuid PRIMARY KEY,
    candidate_hash text NOT NULL UNIQUE,
    name text NOT NULL,
    criteria text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('workspace', 'agent')),
    target_agent_name text NOT NULL DEFAULT '',
    rationale text NOT NULL,
    signal_ids jsonb NOT NULL,
    evidence jsonb NOT NULL,
    overlap_details jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'dismissed')),
    policy_id text REFERENCES lisa.policies(id),
    reviewed_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    reviewed_at timestamptz,
    CHECK ((scope = 'workspace' AND target_agent_name = '') OR
           (scope = 'agent' AND target_agent_name <> ''))
);
CREATE INDEX policy_suggestions_pending_idx
    ON lisa.policy_suggestions(created_at DESC) WHERE status = 'pending';

CREATE TABLE lisa.policy_discovery_schedule (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    last_synthesis_at timestamptz,
    lease_until timestamptz
);
INSERT INTO lisa.policy_discovery_schedule(singleton) VALUES (true);
