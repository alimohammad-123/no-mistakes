package db

const schemaSQL = `
CREATE TABLE IF NOT EXISTS repos (
    id             TEXT PRIMARY KEY,
    working_path   TEXT NOT NULL UNIQUE,
    upstream_url   TEXT NOT NULL,
    fork_url       TEXT,
    default_branch TEXT NOT NULL DEFAULT 'main',
    base_branch    TEXT,
    created_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS bootstrap_test_retirements (
    repository  TEXT NOT NULL,
    base_branch TEXT NOT NULL,
    retired_at  INTEGER NOT NULL,
    PRIMARY KEY (repository, base_branch)
);

CREATE TABLE IF NOT EXISTS runs (
    id                   TEXT PRIMARY KEY,
    repo_id              TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch               TEXT NOT NULL,
    head_sha                TEXT NOT NULL,
    base_sha                TEXT NOT NULL,
    base_branch                 TEXT,
    source_ref                  TEXT,
    bootstrap_test_repository   TEXT,
    bootstrap_test_base_branch  TEXT,
    bootstrap_test_command      TEXT,
    bootstrap_test_policy_sha256 TEXT,
    submitted_head_sha          TEXT,
    status                      TEXT NOT NULL DEFAULT 'pending',
    pr_url                  TEXT,
    pr_state                TEXT,
    pr_state_observed_at    INTEGER,
    ci_ready_at             INTEGER,
    test_head_sha           TEXT,
    validation_target_sha   TEXT,
    validation_replay_count INTEGER NOT NULL DEFAULT 0,
    head_advance_generation INTEGER NOT NULL DEFAULT 0,
    last_pushed_sha         TEXT,
    push_target_kind        TEXT,
    push_target_fingerprint TEXT,
    push_ref                TEXT,
    last_pushed_at          INTEGER,
    push_generation         INTEGER,
    push_active             INTEGER NOT NULL DEFAULT 0,
    error                   TEXT,
    awaiting_agent_since INTEGER,
    parked_ms            INTEGER,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_head_transitions (
    run_id                 TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    source_ref             TEXT NOT NULL,
    previous_sha           TEXT NOT NULL,
    candidate_sha          TEXT NOT NULL,
    require_validation     INTEGER NOT NULL,
    phase                  TEXT NOT NULL,
    expected_push_active   INTEGER NOT NULL,
    prior_target_sha       TEXT,
    next_target_sha        TEXT,
    prior_replay_count     INTEGER NOT NULL,
    next_replay_count      INTEGER NOT NULL,
    ownership_generation  INTEGER NOT NULL,
    created_at             INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_recovery_events (
    id                    TEXT PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    kind                  TEXT NOT NULL,
    recovered_at          INTEGER NOT NULL,
    evidence_token        TEXT NOT NULL,
    prior_status          TEXT NOT NULL,
    prior_error           TEXT NOT NULL,
    head_sha              TEXT NOT NULL,
    test_head_sha         TEXT NOT NULL,
    validation_target_sha TEXT NOT NULL,
    replay_count          INTEGER NOT NULL,
    source_ref            TEXT NOT NULL,
    pr_url                  TEXT NOT NULL,
    last_pushed_sha         TEXT NOT NULL,
    push_target_kind        TEXT NOT NULL,
    push_target_fingerprint TEXT NOT NULL,
    push_generation         INTEGER NOT NULL,
    last_pushed_at          INTEGER NOT NULL,
    document_step_id        TEXT NOT NULL,
    prior_step_status     TEXT NOT NULL,
    prior_step_error      TEXT NOT NULL,
    delivery_protocol_version INTEGER NOT NULL DEFAULT 0,
    anchor_ref            TEXT,
    UNIQUE (run_id, kind)
);

CREATE TABLE IF NOT EXISTS run_recovery_pr_updates (
    run_id               TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    step_result_id       TEXT NOT NULL REFERENCES step_results(id) ON DELETE CASCADE,
    target_url           TEXT NOT NULL,
    head_sha             TEXT NOT NULL,
    prior_content_hash   TEXT NOT NULL,
    intended_content_hash TEXT NOT NULL,
    intended_title       TEXT NOT NULL,
    intended_body        TEXT NOT NULL,
    state                TEXT NOT NULL,
    prepared_at          INTEGER NOT NULL,
    applied_at           INTEGER
);

CREATE TABLE IF NOT EXISTS run_recovery_ref_observations (
    run_id               TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    provider             TEXT NOT NULL,
    source_ref           TEXT NOT NULL,
    stale_oid            TEXT NOT NULL,
    expected_oid         TEXT NOT NULL,
    deadline_at          INTEGER NOT NULL,
    attempts             INTEGER NOT NULL,
    state                TEXT NOT NULL,
    last_observation     TEXT NOT NULL,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_recovery_push_operations (
    run_id               TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    operation_id         TEXT NOT NULL UNIQUE,
    attempt              INTEGER NOT NULL,
    phase                TEXT NOT NULL,
    source_ref           TEXT NOT NULL,
    stale_oid            TEXT NOT NULL,
    target_oid           TEXT NOT NULL,
    target_kind          TEXT NOT NULL,
    target_fingerprint   TEXT NOT NULL,
    prior_generation     INTEGER NOT NULL,
    target_generation    INTEGER NOT NULL,
    prior_pushed_at      INTEGER NOT NULL,
    created_at           INTEGER NOT NULL,
    invoked_at           INTEGER,
    bound_at             INTEGER,
    updated_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_recovery_push_operation_events (
    run_id               TEXT NOT NULL REFERENCES run_recovery_push_operations(run_id) ON DELETE CASCADE,
    sequence             INTEGER NOT NULL,
    operation_id         TEXT NOT NULL,
    attempt              INTEGER NOT NULL,
    phase                TEXT NOT NULL,
    occurred_at          INTEGER NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS run_recovery_ref_observation_events (
    run_id               TEXT NOT NULL REFERENCES run_recovery_ref_observations(run_id) ON DELETE CASCADE,
    attempt              INTEGER NOT NULL,
    observation          TEXT NOT NULL,
    prior_state          TEXT NOT NULL,
    state                TEXT NOT NULL,
    observed_at          INTEGER NOT NULL,
    PRIMARY KEY (run_id, attempt)
);

CREATE TABLE IF NOT EXISTS step_results (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name        TEXT NOT NULL,
    step_order       INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    exit_code        INTEGER,
    duration_ms      INTEGER,
    log_path         TEXT,
    findings_json    TEXT,
    error            TEXT,
    started_at       INTEGER,
    completed_at     INTEGER,
    last_activity_at INTEGER,
    last_activity    TEXT,
    agent_pid        INTEGER,
    auto_fix_limit   INTEGER
);

CREATE TABLE IF NOT EXISTS step_rounds (
    id                   TEXT PRIMARY KEY,
    step_result_id       TEXT NOT NULL REFERENCES step_results(id) ON DELETE CASCADE,
    round                INTEGER NOT NULL,
    trigger_type         TEXT NOT NULL,
    findings_json        TEXT,
    user_findings_json   TEXT,
    selected_finding_ids TEXT,
    selection_source     TEXT,
    fix_summary          TEXT,
    duration_ms          INTEGER NOT NULL,
    created_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_invocations (
    id                    TEXT PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name             TEXT NOT NULL,
    round                 INTEGER NOT NULL,
    purpose               TEXT NOT NULL,
    agent                 TEXT NOT NULL,
    model                 TEXT,
    model_provider        TEXT,
    session_mode          TEXT NOT NULL,
    session_key           TEXT,
    fallback_reason       TEXT,
    started_at            INTEGER NOT NULL,
    completed_at          INTEGER NOT NULL,
    duration_ms           INTEGER NOT NULL,
    subprocess_wait_ms    INTEGER,
    exit_status           TEXT NOT NULL,
    failure_category      TEXT,
    input_tokens          INTEGER,
    output_tokens         INTEGER,
    cache_read_tokens     INTEGER,
    cache_creation_tokens INTEGER,
    fresh_input_tokens    INTEGER,
    reasoning_tokens      INTEGER,
    delta_input_tokens    INTEGER,
    delta_output_tokens   INTEGER,
    delta_cache_read_tokens INTEGER,
    model_roundtrips      INTEGER,
    tool_calls            INTEGER,
    tool_wait_calls       INTEGER,
    tool_test_lint_calls  INTEGER,
    tool_edit_calls       INTEGER,
    tool_read_calls       INTEGER,
    tool_git_calls        INTEGER,
    tool_other_calls      INTEGER,
    workload_files        INTEGER,
    workload_lines        INTEGER,
    finding_count         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_invocations_run_started_id
    ON agent_invocations (run_id, started_at, id);

CREATE TABLE IF NOT EXISTS run_agent_sessions (
    run_id     TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role       TEXT NOT NULL,
    agent      TEXT NOT NULL,
    session_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (run_id, role)
);

CREATE TABLE IF NOT EXISTS intent_cache (
    cache_key   TEXT PRIMARY KEY,
    summary     TEXT NOT NULL,
    agent_name  TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
`

// migrationStatements hold additive schema changes applied to databases that
// were created before the referenced columns existed. Each statement must be
// idempotent via its error being tolerated when the column already exists.
var migrationStatements = []string{
	`ALTER TABLE repos ADD COLUMN fork_url TEXT`,
	// The repo value is an explicit future-run override. The run value is a
	// frozen effective-base snapshot; historical rows remain NULL and preserve
	// pre-feature behavior by falling back only to repos.default_branch.
	`ALTER TABLE repos ADD COLUMN base_branch TEXT`,
	`ALTER TABLE runs ADD COLUMN base_branch TEXT`,
	// Source-ref provenance is nullable only for pre-upgrade rows. New runs
	// freeze it at intake; active legacy runs migrate it one way from branch.
	`ALTER TABLE runs ADD COLUMN source_ref TEXT`,
	// A bootstrap Test authorization is all-null for ordinary/historical runs
	// and all-present for an exact first-policy adoption. Recovery rejects any
	// partial row rather than inferring missing authorization.
	`ALTER TABLE runs ADD COLUMN bootstrap_test_repository TEXT`,
	`ALTER TABLE runs ADD COLUMN bootstrap_test_base_branch TEXT`,
	`ALTER TABLE runs ADD COLUMN bootstrap_test_command TEXT`,
	`ALTER TABLE runs ADD COLUMN bootstrap_test_policy_sha256 TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selected_finding_ids TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selection_source TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN fix_summary TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN user_findings_json TEXT`,
	`ALTER TABLE runs ADD COLUMN intent TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_source TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_session_id TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_score REAL`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_since INTEGER`,
	`ALTER TABLE runs ADD COLUMN parked_ms INTEGER`,
	// Branch synchronization provenance is intentionally nullable. Historical
	// rows stay unbound because mutable head_sha cannot prove a successful push.
	`ALTER TABLE runs ADD COLUMN submitted_head_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_kind TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_fingerprint TEXT`,
	`ALTER TABLE runs ADD COLUMN push_ref TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_generation INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_active INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN pr_state TEXT`,
	`ALTER TABLE runs ADD COLUMN pr_state_observed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN ci_ready_at INTEGER`,
	// Final-head Test provenance is nullable so historical rows remain
	// explicitly unknown. The replay counter is persisted to bound convergence
	// across crashes and daemon upgrades rather than restarting the budget.
	`ALTER TABLE runs ADD COLUMN test_head_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN validation_target_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN validation_replay_count INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN head_advance_generation INTEGER NOT NULL DEFAULT 0`,
	// Custody return is nullable: NULL means the pipeline still owns any
	// unpublished head this run produced; a timestamp means an explicit
	// guarded recovery ended that ownership (internal/branchsync).
	`ALTER TABLE runs ADD COLUMN custody_returned_at INTEGER`,
	`ALTER TABLE step_results ADD COLUMN last_activity_at INTEGER`,
	`ALTER TABLE step_results ADD COLUMN last_activity TEXT`,
	`ALTER TABLE step_results ADD COLUMN agent_pid INTEGER`,
	`ALTER TABLE step_results ADD COLUMN auto_fix_limit INTEGER`,
	`ALTER TABLE run_recovery_events ADD COLUMN delivery_protocol_version INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE run_recovery_events ADD COLUMN anchor_ref TEXT`,
	// Session-fidelity telemetry columns (all nullable so pre-existing rows read
	// back as unknown, never a fabricated zero).
	`ALTER TABLE agent_invocations ADD COLUMN model_provider TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN fallback_reason TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN subprocess_wait_ms INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN fresh_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN reasoning_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_output_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_cache_read_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN model_roundtrips INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_wait_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_test_lint_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_edit_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_read_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_git_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_other_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_files INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_lines INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN finding_count INTEGER`,
}
