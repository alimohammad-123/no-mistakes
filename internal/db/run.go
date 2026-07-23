package db

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

var ErrHeadValidationMutationExhausted = errors.New("head validation mutation capacity exhausted")

// Run represents a pipeline run.
type Run struct {
	ID      string
	RepoID  string
	Branch  string
	HeadSHA string
	BaseSHA string
	// BaseBranch freezes the effective pipeline integration and trusted-config
	// branch for this run. Empty is reserved for migrated historical rows.
	BaseBranch string
	// SourceRef is the canonical full refs/heads identity frozen from Branch at
	// authoritative run intake. Nil is reserved for pre-upgrade rows.
	SourceRef *string
	// Bootstrap Test authorization fields are either all nil or all present.
	// They freeze the exact user-owned first-policy authorization before any
	// pipeline step executes so recovery never replaces it with mutable global
	// bootstrap values.
	BootstrapTestRepository   *string
	BootstrapTestBaseBranch   *string
	BootstrapTestCommand      *string
	BootstrapTestPolicySHA256 *string
	SubmittedHeadSHA          *string
	Status                    types.RunStatus
	PRURL                     *string
	PRState                   *string
	PRStateObservedAt         *int64
	CIReadyAt                 *int64
	// TestHeadSHA is the exact candidate on which this run's configured Test
	// command most recently succeeded. Nil is unknown, including every
	// pre-upgrade historical row; it is never inferred from mutable HeadSHA.
	TestHeadSHA *string
	// ValidationTargetSHA is the stale candidate currently being replayed. A
	// restart of the same target is idempotent and does not consume another
	// convergence attempt; only a newly changed target advances the counter.
	ValidationTargetSHA *string
	// ValidationReplayCount persists the bounded final-head convergence budget
	// so a crash or daemon upgrade cannot reset an otherwise non-converging run.
	ValidationReplayCount int
	HeadAdvanceGeneration int64
	LastPushedSHA         *string
	PushTargetKind        *string
	PushTargetFingerprint *string
	PushRef               *string
	LastPushedAt          *int64
	PushGeneration        *int64
	PushActive            bool
	// CustodyReturnedAt is non-nil once a guarded branch-sync recovery
	// explicitly ended this run's ownership of an unpublished pipeline head
	// (terminal run whose head was never successfully pushed, or moved after
	// the last push). It never changes push provenance; it only records that
	// the operator worktree took the branch back.
	CustodyReturnedAt *int64
	Error             *string
	// AwaitingAgentSince is the unix-seconds timestamp at which the run parked
	// at a gate awaiting the driving agent's response (an awaiting_approval or
	// fix_review step). It is nil whenever the run is not parked: the executor
	// sets it on gate entry and clears it the moment the agent responds (or the
	// wait is cancelled). It is observability only and does not affect gate
	// resolution.
	AwaitingAgentSince *int64
	// ParkedMS accumulates the run's total parked-at-gate wall time in
	// milliseconds across every gate wait (local performance telemetry;
	// step duration_ms values exclude this time).
	ParkedMS        int64
	Intent          *string
	IntentSource    *string
	IntentSessionID *string
	IntentScore     *float64
	CreatedAt       int64
	UpdatedAt       int64
}

const runColumns = `id, repo_id, branch, head_sha, base_sha, COALESCE(base_branch, ''), source_ref, bootstrap_test_repository, bootstrap_test_base_branch, bootstrap_test_command, bootstrap_test_policy_sha256, submitted_head_sha, status, pr_url, pr_state, pr_state_observed_at, ci_ready_at, test_head_sha, validation_target_sha, COALESCE(validation_replay_count, 0), COALESCE(head_advance_generation, 0), last_pushed_sha, push_target_kind, push_target_fingerprint, push_ref, last_pushed_at, push_generation, COALESCE(push_active, 0), custody_returned_at, error, awaiting_agent_since, COALESCE(parked_ms, 0), intent, intent_source, intent_session_id, intent_score, created_at, updated_at`

func scanRun(row interface {
	Scan(...any) error
}, r *Run) error {
	return row.Scan(
		&r.ID, &r.RepoID, &r.Branch, &r.HeadSHA, &r.BaseSHA, &r.BaseBranch, &r.SourceRef,
		&r.BootstrapTestRepository, &r.BootstrapTestBaseBranch, &r.BootstrapTestCommand, &r.BootstrapTestPolicySHA256,
		&r.SubmittedHeadSHA, &r.Status,
		&r.PRURL, &r.PRState, &r.PRStateObservedAt, &r.CIReadyAt, &r.TestHeadSHA, &r.ValidationTargetSHA, &r.ValidationReplayCount, &r.HeadAdvanceGeneration,
		&r.LastPushedSHA, &r.PushTargetKind, &r.PushTargetFingerprint, &r.PushRef,
		&r.LastPushedAt, &r.PushGeneration, &r.PushActive,
		&r.CustodyReturnedAt, &r.Error, &r.AwaitingAgentSince, &r.ParkedMS,
		&r.Intent, &r.IntentSource, &r.IntentSessionID, &r.IntentScore,
		&r.CreatedAt, &r.UpdatedAt,
	)
}

// BootstrapTestAuthorization is the immutable authorization snapshot for one
// first-policy Test command.
type BootstrapTestAuthorization struct {
	Repository   string
	BaseBranch   string
	Command      string
	PolicySHA256 string
}

// FrozenBootstrapTestAuthorization returns nil for an ordinary run and rejects
// partial or malformed snapshots. Recovery must never fill missing fields from
// mutable configuration.
func (r *Run) FrozenBootstrapTestAuthorization() (*BootstrapTestAuthorization, error) {
	if r == nil {
		return nil, nil
	}
	fields := []*string{r.BootstrapTestRepository, r.BootstrapTestBaseBranch, r.BootstrapTestCommand, r.BootstrapTestPolicySHA256}
	present := 0
	for _, field := range fields {
		if field != nil {
			present++
		}
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(fields) {
		return nil, fmt.Errorf("run has incomplete bootstrap Test authorization")
	}
	auth := &BootstrapTestAuthorization{
		Repository:   *r.BootstrapTestRepository,
		BaseBranch:   *r.BootstrapTestBaseBranch,
		Command:      *r.BootstrapTestCommand,
		PolicySHA256: *r.BootstrapTestPolicySHA256,
	}
	identity, err := repoidentity.Canonical(auth.Repository)
	if err != nil || identity != auth.Repository ||
		strings.TrimSpace(auth.BaseBranch) != auth.BaseBranch || auth.BaseBranch == "" || auth.BaseBranch != r.BaseBranch ||
		strings.TrimSpace(auth.Command) != auth.Command || auth.Command == "" {
		return nil, fmt.Errorf("run has invalid bootstrap Test authorization")
	}
	if len(auth.PolicySHA256) != 64 || strings.ToLower(auth.PolicySHA256) != auth.PolicySHA256 {
		return nil, fmt.Errorf("run has invalid bootstrap Test policy digest")
	}
	if decoded, err := hex.DecodeString(auth.PolicySHA256); err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("run has invalid bootstrap Test policy digest")
	}
	return auth, nil
}

// FrozenSourceRef returns the run's canonical full source ref and rejects
// missing, malformed, non-head, or branch-mismatched provenance.
func (r *Run) FrozenSourceRef() (string, error) {
	if r == nil || r.SourceRef == nil {
		return "", fmt.Errorf("run source ref is not frozen")
	}
	if err := sourceprovenance.ValidateFrozenSourceRef(*r.SourceRef, r.Branch); err != nil {
		return "", fmt.Errorf("run has invalid source ref: %w", err)
	}
	return *r.SourceRef, nil
}

// EnsureActiveRunSourceRef performs the one-way compatibility migration for a
// pre-upgrade active run. It derives only from the already-frozen Branch field
// and uses a compare-and-set write so an existing value is never replaced.
func (d *DB) EnsureActiveRunSourceRef(r *Run) (string, error) {
	if r == nil {
		return "", fmt.Errorf("ensure run source ref: run is nil")
	}
	if r.SourceRef != nil {
		return r.FrozenSourceRef()
	}
	if r.Status != types.RunPending && r.Status != types.RunRunning {
		return "", fmt.Errorf("ensure run source ref: legacy run is not active")
	}
	ref, err := sourceprovenance.CanonicalSourceRefFromBranch(r.Branch)
	if err != nil {
		return "", fmt.Errorf("ensure run source ref: invalid frozen branch: %w", err)
	}
	result, err := d.sql.Exec(
		`UPDATE runs SET source_ref = ?, updated_at = ? WHERE id = ? AND branch = ? AND source_ref IS NULL AND status IN (?, ?)`,
		ref, now(), r.ID, r.Branch, types.RunPending, types.RunRunning,
	)
	if err != nil {
		return "", fmt.Errorf("ensure run source ref: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("ensure run source ref: %w", err)
	}
	if count != 1 {
		current, getErr := d.GetRun(r.ID)
		if getErr == nil && current != nil && (current.Status == types.RunPending || current.Status == types.RunRunning) {
			if currentRef, frozenErr := current.FrozenSourceRef(); frozenErr == nil {
				r.SourceRef = current.SourceRef
				return currentRef, nil
			}
		}
		return "", fmt.Errorf("ensure run source ref: run changed or is no longer active")
	}
	r.SourceRef = &ref
	return ref, nil
}

// EffectiveBaseBranch returns this run's frozen pipeline base. Historical rows
// without a snapshot fall back only to the repository's recorded remote
// default, never to a newer repo base override.
func (r *Run) EffectiveBaseBranch(repo *Repo) string {
	if r != nil {
		if base := strings.TrimSpace(r.BaseBranch); base != "" {
			return base
		}
	}
	if repo == nil {
		return ""
	}
	return strings.TrimSpace(repo.DefaultBranch)
}

// InsertRun creates a compatibility run record without a base snapshot. New
// production runs must use InsertRunWithBaseBranch.
func (d *DB) InsertRun(repoID, branch, headSHA, baseSHA string) (*Run, error) {
	return d.insertRun(repoID, branch, headSHA, baseSHA, "")
}

// InsertRunWithBaseBranch creates a run with an immutable effective-base
// snapshot used by execution and crash recovery.
func (d *DB) InsertRunWithBaseBranch(repoID, branch, headSHA, baseSHA, baseBranch string) (*Run, error) {
	baseBranch = strings.TrimSpace(baseBranch)
	if baseBranch == "" {
		return nil, fmt.Errorf("insert run: base branch must not be empty")
	}
	return d.insertRun(repoID, branch, headSHA, baseSHA, baseBranch)
}

func (d *DB) insertRun(repoID, branch, headSHA, baseSHA, baseBranch string) (*Run, error) {
	sourceRef, err := sourceprovenance.CanonicalSourceRefFromBranch(branch)
	if err != nil {
		return nil, fmt.Errorf("insert run: invalid source branch: %w", err)
	}
	ts := now()
	r := &Run{
		ID:               newID(),
		RepoID:           repoID,
		Branch:           branch,
		HeadSHA:          headSHA,
		BaseSHA:          baseSHA,
		BaseBranch:       baseBranch,
		SourceRef:        &sourceRef,
		SubmittedHeadSHA: &headSHA,
		Status:           types.RunPending,
		CreatedAt:        ts,
		UpdatedAt:        ts,
	}
	_, err = d.sql.Exec(
		`INSERT INTO runs (id, repo_id, branch, head_sha, base_sha, base_branch, source_ref, submitted_head_sha, status, pr_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'none', ?, ?)`,
		r.ID, r.RepoID, r.Branch, r.HeadSHA, r.BaseSHA, nullableString(r.BaseBranch), sourceRef, headSHA, r.Status, r.CreatedAt, r.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert run: %w", err)
	}
	return r, nil
}

// GetRun returns a run by ID.
func (d *DB) GetRun(id string) (*Run, error) {
	r := &Run{}
	err := scanRun(d.sql.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, id), r)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}
	return r, nil
}

// GetRunsByRepo returns all runs for a repo, newest first.
func (d *DB) GetRunsByRepo(repoID string) ([]*Run, error) {
	rows, err := d.sql.Query(`SELECT `+runColumns+` FROM runs WHERE repo_id = ? ORDER BY created_at DESC, id DESC`, repoID)
	if err != nil {
		return nil, fmt.Errorf("get runs by repo: %w", err)
	}
	defer rows.Close()
	var runs []*Run
	for rows.Next() {
		r := &Run{}
		if err := scanRun(rows, r); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// GetRunsByRepoHead returns the runs for a repo matching an exact branch and
// head SHA, newest first. It lets a caller detect the run created by a specific
// push without scanning (and rebuilding step data for) the repo's entire run
// history, so the cost stays bounded to the handful of runs for one head.
func (d *DB) GetRunsByRepoHead(repoID, branch, headSHA string) ([]*Run, error) {
	rows, err := d.sql.Query(
		`SELECT `+runColumns+` FROM runs WHERE repo_id = ? AND branch = ? AND head_sha = ? ORDER BY created_at DESC, id DESC`,
		repoID, branch, headSHA,
	)
	if err != nil {
		return nil, fmt.Errorf("get runs by repo head: %w", err)
	}
	defer rows.Close()
	var runs []*Run
	for rows.Next() {
		r := &Run{}
		if err := scanRun(rows, r); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// GetLatestRunForBranch returns the newest run for one exact repository branch.
// It is intentionally bounded to one row for compatibility recovery and other
// branch-local decisions that must not scan unbounded run history.
func (d *DB) GetLatestRunForBranch(repoID, branch string) (*Run, error) {
	r := &Run{}
	err := scanRun(d.sql.QueryRow(
		`SELECT `+runColumns+` FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC LIMIT 1`, repoID, branch,
	), r)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest branch run: %w", err)
	}
	return r, nil
}

// GetActiveRun returns the currently active run (pending or running) for a repo,
// if any. When branch is non-empty, only a run on that exact branch is returned
// - the setup wizard relies on this to decide whether a new run is needed for
// the current branch. When branch is empty, returns the most recently created
// active run across any branch.
func (d *DB) GetActiveRun(repoID, branch string) (*Run, error) {
	r := &Run{}
	var err error
	if branch == "" {
		err = scanRun(d.sql.QueryRow(
			`SELECT `+runColumns+` FROM runs WHERE repo_id = ? AND status IN ('pending', 'running') ORDER BY created_at DESC, id DESC LIMIT 1`, repoID,
		), r)
	} else {
		err = scanRun(d.sql.QueryRow(
			`SELECT `+runColumns+` FROM runs WHERE repo_id = ? AND branch = ? AND status IN ('pending', 'running') ORDER BY created_at DESC, id DESC LIMIT 1`, repoID, branch,
		), r)
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active run: %w", err)
	}
	return r, nil
}

// GetActiveRuns returns all pending or running runs across all repos, newest first.
func (d *DB) GetActiveRuns() ([]*Run, error) {
	rows, err := d.sql.Query(
		`SELECT `+runColumns+` FROM runs WHERE status IN (?, ?) ORDER BY created_at DESC, id DESC`,
		types.RunPending, types.RunRunning,
	)
	if err != nil {
		return nil, fmt.Errorf("get active runs: %w", err)
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		if err := scanRun(rows, r); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// SetRunBootstrapTestAuthorization atomically freezes a complete bootstrap
// authorization. The all-null guard makes the snapshot write-once, the base
// comparison prevents cross-run stamping, and the retirement predicate
// linearizes admission against permanent repository/base retirement.
func (d *DB) SetRunBootstrapTestAuthorization(id string, auth BootstrapTestAuthorization) error {
	repository, baseBranch, command, digest := auth.Repository, auth.BaseBranch, auth.Command, auth.PolicySHA256
	candidate := &Run{
		BaseBranch:                baseBranch,
		BootstrapTestRepository:   &repository,
		BootstrapTestBaseBranch:   &baseBranch,
		BootstrapTestCommand:      &command,
		BootstrapTestPolicySHA256: &digest,
	}
	if _, err := candidate.FrozenBootstrapTestAuthorization(); err != nil {
		return err
	}
	result, err := d.sql.Exec(
		`UPDATE runs SET bootstrap_test_repository = ?, bootstrap_test_base_branch = ?, bootstrap_test_command = ?, bootstrap_test_policy_sha256 = ?, updated_at = ?
		 WHERE id = ? AND base_branch = ? AND bootstrap_test_repository IS NULL AND bootstrap_test_base_branch IS NULL AND bootstrap_test_command IS NULL AND bootstrap_test_policy_sha256 IS NULL
		 AND NOT EXISTS (SELECT 1 FROM bootstrap_test_retirements WHERE repository = ? AND base_branch = ?)`,
		auth.Repository, auth.BaseBranch, auth.Command, auth.PolicySHA256, now(), id, auth.BaseBranch, auth.Repository, auth.BaseBranch,
	)
	if err != nil {
		return fmt.Errorf("set run bootstrap Test authorization: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set run bootstrap Test authorization: %w", err)
	}
	if count != 1 {
		retired, retiredErr := d.IsBootstrapTestRetired(auth.Repository, auth.BaseBranch)
		if retiredErr != nil {
			return fmt.Errorf("set run bootstrap Test authorization: %w", retiredErr)
		}
		if retired {
			return fmt.Errorf("set run bootstrap Test authorization: %w for repository %q and pipeline base %q", ErrBootstrapTestRetired, auth.Repository, auth.BaseBranch)
		}
		return fmt.Errorf("set run bootstrap Test authorization: run is missing, mismatched, or already frozen")
	}
	return nil
}

// UpdateRunStatus updates a run's status and updated_at timestamp.
func (d *DB) UpdateRunStatus(id string, status types.RunStatus) error {
	_, err := d.sql.Exec(`UPDATE runs SET status = ?, push_active = CASE WHEN ? IN ('completed', 'failed', 'cancelled') THEN 0 ELSE push_active END, updated_at = ? WHERE id = ?`, status, status, now(), id)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

// UpdateRunPRURL sets the PR URL on a run.
func (d *DB) UpdateRunPRURL(id, prURL string) error {
	_, err := d.sql.Exec(`UPDATE runs SET pr_url = ?, pr_state = 'open', pr_state_observed_at = ?, updated_at = ? WHERE id = ?`, prURL, now(), now(), id)
	if err != nil {
		return fmt.Errorf("update run pr url: %w", err)
	}
	return nil
}

// PushBinding records the exact target and commit proven by a successful
// pipeline-owned push. TargetFingerprint is a one-way digest and must never be
// a raw URL.
type PushBinding struct {
	HeadSHA           string
	TargetKind        string
	TargetFingerprint string
	Ref               string
}

type HeadAdvancePhase string

const (
	HeadAdvancePipeline HeadAdvancePhase = "pipeline"
	HeadAdvancePush     HeadAdvancePhase = "push"
)

// UpdateRunPushBinding advances a run's successful-push provenance and
// increments its generation. It is called for both a completed push and a
// freshly verified already-up-to-date push.
func (d *DB) UpdateRunPushBinding(id string, binding PushBinding) error {
	ts := now()
	_, err := d.sql.Exec(
		`UPDATE runs SET last_pushed_sha = ?, push_target_kind = ?, push_target_fingerprint = ?, push_ref = ?, last_pushed_at = ?, push_generation = COALESCE(push_generation, 0) + 1, updated_at = ? WHERE id = ?`,
		binding.HeadSHA, binding.TargetKind, binding.TargetFingerprint, binding.Ref, ts, ts, id,
	)
	if err != nil {
		return fmt.Errorf("update run push binding: %w", err)
	}
	return nil
}

// SetRunCustodyReturned stamps the moment a guarded recovery explicitly
// returned custody of this run's branch to the operator worktree. Stamping is
// idempotent: the first timestamp wins so the record keeps the original
// recovery moment.
func (d *DB) SetRunCustodyReturned(id string) error {
	ts := now()
	_, err := d.sql.Exec(`UPDATE runs SET custody_returned_at = COALESCE(custody_returned_at, ?), updated_at = ? WHERE id = ?`, ts, ts, id)
	if err != nil {
		return fmt.Errorf("set run custody returned: %w", err)
	}
	return nil
}

// SetRunPushActive marks whether a pipeline phase currently owns a possible
// branch-head update. Sync refuses while this marker is set.
func (d *DB) SetRunPushActive(id string, active bool) error {
	var (
		result sql.Result
		err    error
	)
	if active {
		result, err = d.sql.Exec(
			`UPDATE runs SET push_active = 1, updated_at = ?
			 WHERE id = ? AND status = ? AND COALESCE(push_active, 0) = 0 AND custody_returned_at IS NULL`,
			now(), id, types.RunRunning,
		)
	} else {
		result, err = d.sql.Exec(`UPDATE runs SET push_active = 0, updated_at = ? WHERE id = ? AND COALESCE(push_active, 0) = 1`, now(), id)
	}
	if err != nil {
		return fmt.Errorf("set run push active: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set run push active: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("set run push active: run is unavailable or custody state changed")
	}
	return nil
}

// UpdateRunPRState persists normalized lifecycle truth independently of logs.
func (d *DB) UpdateRunPRState(id, state string) error {
	ts := now()
	_, err := d.sql.Exec(`UPDATE runs SET pr_state = ?, pr_state_observed_at = ?, updated_at = ? WHERE id = ?`, state, ts, ts, id)
	if err != nil {
		return fmt.Errorf("update run PR state: %w", err)
	}
	return nil
}

// SetRunCIReady persists checks-passed readiness so fresh TUI and AXI attaches
// do not depend on receiving a historical log line.
func (d *DB) SetRunCIReady(id string, ready bool) error {
	var readyAt any
	if ready {
		readyAt = now()
	}
	_, err := d.sql.Exec(`UPDATE runs SET ci_ready_at = ?, updated_at = ? WHERE id = ? AND ((ci_ready_at IS NULL AND ? = 1) OR (ci_ready_at IS NOT NULL AND ? = 0))`, readyAt, now(), id, ready, ready)
	if err != nil {
		return fmt.Errorf("set run CI ready: %w", err)
	}
	return nil
}

// BeginConfiguredTestAttempt invalidates any prior proof before each configured
// Test execution. This prevents a failed or approved-failing retry at the same
// SHA from reusing an older success.
func (d *DB) BeginConfiguredTestAttempt(id, headSHA string) error {
	if strings.TrimSpace(headSHA) == "" {
		return fmt.Errorf("begin configured Test attempt: head SHA is empty")
	}
	result, err := d.sql.Exec(
		`UPDATE runs SET test_head_sha = NULL, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = 0`,
		now(), id, types.RunRunning, headSHA,
	)
	if err != nil {
		return fmt.Errorf("begin configured Test attempt: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("begin configured Test attempt: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("begin configured Test attempt: run is missing, stale, terminal, publishing, or outside pipeline custody")
	}
	return nil
}

// RecordSuccessfulTestHead records positive configured-Test evidence for the
// exact active candidate. It refuses stale, terminal, custody-returned, or
// push-active rows rather than inferring proof from mutable run state.
func (d *DB) RecordSuccessfulTestHead(id, headSHA string) error {
	if strings.TrimSpace(headSHA) == "" {
		return fmt.Errorf("record successful Test head: head SHA is empty")
	}
	result, err := d.sql.Exec(
		`UPDATE runs SET test_head_sha = ?, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = 0`,
		headSHA, now(), id, types.RunRunning, headSHA,
	)
	if err != nil {
		return fmt.Errorf("record successful Test head: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record successful Test head: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("record successful Test head: run is missing, stale, terminal, publishing, or outside pipeline custody")
	}
	return nil
}

// ScheduleHeadValidationReplay atomically consumes one persisted convergence
// attempt, clears stale CI readiness, and resets the existing Test-through-CI
// step rows for same-run replay. Round history and run/PR/push identity remain
// intact. Terminal history and custody-returned branches are never rewritten.
func (d *DB) ScheduleHeadValidationReplay(id string, maxReplays int) (int, error) {
	if maxReplays <= 0 {
		return 0, fmt.Errorf("schedule head validation replay: replay bound must be positive")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, fmt.Errorf("schedule head validation replay: begin: %w", err)
	}
	defer tx.Rollback()

	var status types.RunStatus
	var headSHA string
	var targetSHA *string
	var replayCount int
	var custodyReturnedAt *int64
	var pushActive bool
	if err := tx.QueryRow(
		`SELECT status, head_sha, validation_target_sha, COALESCE(validation_replay_count, 0), custody_returned_at, COALESCE(push_active, 0) FROM runs WHERE id = ?`, id,
	).Scan(&status, &headSHA, &targetSHA, &replayCount, &custodyReturnedAt, &pushActive); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("schedule head validation replay: run is missing")
		}
		return 0, fmt.Errorf("schedule head validation replay: read run: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil || pushActive {
		return replayCount, fmt.Errorf("schedule head validation replay: run is not an active pipeline-owned candidate")
	}
	if replayCount > maxReplays {
		return replayCount, fmt.Errorf("final-head validation did not converge after %d replay attempts", maxReplays)
	}
	next := replayCount
	if targetSHA == nil || *targetSHA != headSHA {
		if replayCount >= maxReplays {
			return replayCount, fmt.Errorf("final-head validation did not converge after %d replay attempts", replayCount)
		}
		next++
	}
	result, err := tx.Exec(
		`UPDATE runs SET validation_target_sha = ?, validation_replay_count = ?, ci_ready_at = NULL, awaiting_agent_since = NULL, updated_at = ?
		 WHERE id = ? AND status = ? AND COALESCE(validation_replay_count, 0) = ? AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = 0`,
		headSHA, next, now(), id, types.RunRunning, replayCount,
	)
	if err != nil {
		return replayCount, fmt.Errorf("schedule head validation replay: update run: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return replayCount, fmt.Errorf("schedule head validation replay: update run: %w", err)
		}
		return replayCount, fmt.Errorf("schedule head validation replay: run changed concurrently")
	}
	if _, err := tx.Exec(
		`UPDATE step_results
		 SET status = ?, exit_code = NULL, duration_ms = NULL, log_path = NULL, findings_json = NULL, error = NULL,
		     started_at = NULL, completed_at = NULL, last_activity_at = NULL, last_activity = NULL, agent_pid = NULL, auto_fix_limit = NULL
		 WHERE run_id = ? AND step_order >= ?`,
		types.StepStatusPending, id, types.StepTest.Order(),
	); err != nil {
		return replayCount, fmt.Errorf("schedule head validation replay: reset steps: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return replayCount, fmt.Errorf("schedule head validation replay: commit: %w", err)
	}
	return next, nil
}

// CompleteHeadValidation clears an in-progress validation target only when the
// exact active candidate and its configured-Test proof still agree. A stale or
// concurrently changed run fails closed and keeps the target for recovery.
func (d *DB) CompleteHeadValidation(id, headSHA string) error {
	result, err := d.sql.Exec(
		`UPDATE runs SET validation_target_sha = NULL, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND test_head_sha = ?
		   AND (validation_target_sha IS NULL OR validation_target_sha = ?)
		   AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = 0`,
		now(), id, types.RunRunning, headSHA, headSHA, headSHA,
	)
	if err != nil {
		return fmt.Errorf("complete head validation: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("complete head validation: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("complete head validation: candidate proof is missing, stale, or outside pipeline custody")
	}
	return nil
}

func (d *DB) CheckHeadValidationMutationCapacity(id string, maxReplays int) error {
	if maxReplays <= 0 {
		return fmt.Errorf("check head validation mutation capacity: replay bound must be positive")
	}
	var status types.RunStatus
	var headSHA string
	var targetSHA *string
	var replayCount int
	var custodyReturnedAt *int64
	var pendingTransition bool
	if err := d.sql.QueryRow(
		`SELECT status, head_sha, validation_target_sha, COALESCE(validation_replay_count, 0),
		        custody_returned_at, EXISTS(SELECT 1 FROM run_head_transitions WHERE run_id = runs.id)
		 FROM runs WHERE id = ?`, id,
	).Scan(&status, &headSHA, &targetSHA, &replayCount, &custodyReturnedAt, &pendingTransition); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("check head validation mutation capacity: run is missing")
		}
		return fmt.Errorf("check head validation mutation capacity: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil {
		return fmt.Errorf("check head validation mutation capacity: run is outside active pipeline custody")
	}
	if pendingTransition {
		return fmt.Errorf("check head validation mutation capacity: a head transition is already pending")
	}
	if replayCount < 0 {
		return fmt.Errorf("check head validation mutation capacity: replay state is inconsistent")
	}
	if replayCount >= maxReplays {
		return fmt.Errorf("%w: final-head validation did not converge after %d replay attempts", ErrHeadValidationMutationExhausted, maxReplays)
	}
	if targetSHA == nil {
		return nil
	}
	if *targetSHA != headSHA {
		return fmt.Errorf("check head validation mutation capacity: replay state is inconsistent")
	}
	return nil
}

func (d *DB) CheckHeadValidationDeliveryEligibility(id, headSHA string, maxReplays int) error {
	if strings.TrimSpace(headSHA) == "" || maxReplays <= 0 {
		return fmt.Errorf("check head validation delivery eligibility: head and replay bound are required")
	}
	var status types.RunStatus
	var durableHeadSHA string
	var testHeadSHA *string
	var targetSHA *string
	var replayCount int
	var custodyReturnedAt *int64
	var pendingTransition bool
	if err := d.sql.QueryRow(
		`SELECT status, head_sha, test_head_sha, validation_target_sha,
		        COALESCE(validation_replay_count, 0), custody_returned_at,
		        EXISTS(SELECT 1 FROM run_head_transitions WHERE run_id = runs.id)
		 FROM runs WHERE id = ?`, id,
	).Scan(
		&status, &durableHeadSHA, &testHeadSHA, &targetSHA,
		&replayCount, &custodyReturnedAt, &pendingTransition,
	); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("check head validation delivery eligibility: run is missing")
		}
		return fmt.Errorf("check head validation delivery eligibility: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil || durableHeadSHA != headSHA {
		return fmt.Errorf("check head validation delivery eligibility: run is outside exact active pipeline custody")
	}
	if replayCount < 0 || replayCount > maxReplays {
		return fmt.Errorf("check head validation delivery eligibility: replay state is outside bounded policy")
	}
	if testHeadSHA == nil || *testHeadSHA != headSHA || targetSHA != nil || pendingTransition {
		return fmt.Errorf("configured Test proof is missing, stale, or still active for delivery head %s", headSHA)
	}
	return nil
}

func headAdvancePushActive(phase HeadAdvancePhase) (bool, error) {
	switch phase {
	case HeadAdvancePipeline:
		return false, nil
	case HeadAdvancePush:
		return true, nil
	default:
		return false, fmt.Errorf("unknown head advance phase %q", phase)
	}
}

func (d *DB) ValidateRunHeadAdvance(id, previousSHA string, phase HeadAdvancePhase) error {
	expectedPushActive, err := headAdvancePushActive(phase)
	if err != nil {
		return err
	}
	var status types.RunStatus
	var headSHA string
	var custodyReturnedAt *int64
	var pushActive bool
	if err := d.sql.QueryRow(
		`SELECT status, head_sha, custody_returned_at, COALESCE(push_active, 0) FROM runs WHERE id = ?`, id,
	).Scan(&status, &headSHA, &custodyReturnedAt, &pushActive); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("validate run head advance: run is missing")
		}
		return fmt.Errorf("validate run head advance: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil || pushActive != expectedPushActive || headSHA != previousSHA {
		return fmt.Errorf("validate run head advance: run head, custody, or phase changed")
	}
	return nil
}

type RunHeadTransition struct {
	RunID               string
	SourceRef           string
	PreviousSHA         string
	CandidateSHA        string
	RequireValidation   bool
	Phase               HeadAdvancePhase
	ExpectedPushActive  bool
	PriorTargetSHA      *string
	NextTargetSHA       *string
	PriorReplayCount    int
	NextReplayCount     int
	OwnershipGeneration int64
}

func scanRunHeadTransition(row interface {
	Scan(...any) error
}, transition *RunHeadTransition) error {
	return row.Scan(
		&transition.RunID, &transition.SourceRef, &transition.PreviousSHA, &transition.CandidateSHA,
		&transition.RequireValidation, &transition.Phase, &transition.ExpectedPushActive,
		&transition.PriorTargetSHA, &transition.NextTargetSHA,
		&transition.PriorReplayCount, &transition.NextReplayCount, &transition.OwnershipGeneration,
	)
}

const runHeadTransitionColumns = `run_id, source_ref, previous_sha, candidate_sha, require_validation, phase, expected_push_active, prior_target_sha, next_target_sha, prior_replay_count, next_replay_count, ownership_generation`

func (d *DB) GetRunHeadTransition(runID string) (*RunHeadTransition, error) {
	transition := &RunHeadTransition{}
	err := scanRunHeadTransition(d.sql.QueryRow(
		`SELECT `+runHeadTransitionColumns+` FROM run_head_transitions WHERE run_id = ?`, runID,
	), transition)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get run head transition: %w", err)
	}
	return transition, nil
}

func (d *DB) ValidateRecoverableRunHeadTransition(transition *RunHeadTransition, maxReplays int) (*Run, error) {
	if transition == nil || maxReplays <= 0 {
		return nil, fmt.Errorf("validate recoverable run head transition: transition and replay bound are required")
	}
	run, err := d.GetRun(transition.RunID)
	if err != nil {
		return nil, fmt.Errorf("validate recoverable run head transition: %w", err)
	}
	if run == nil || run.Status != types.RunRunning || run.AwaitingAgentSince != nil || run.CustodyReturnedAt != nil {
		return nil, fmt.Errorf("validate recoverable run head transition: run is not active pipeline custody")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		return nil, fmt.Errorf("validate recoverable run head transition: %w", err)
	}
	phase := HeadAdvancePipeline
	if run.PushActive {
		phase = HeadAdvancePush
	}
	replayRetarget := run.TestHeadSHA == nil &&
		run.ValidationTargetSHA != nil &&
		*run.ValidationTargetSHA == run.HeadSHA &&
		transition.Phase == HeadAdvancePipeline
	staleProof := run.TestHeadSHA != nil && *run.TestHeadSHA == run.HeadSHA
	if transition.RunID != run.ID ||
		transition.SourceRef != ref ||
		transition.PreviousSHA != run.HeadSHA ||
		transition.CandidateSHA == "" ||
		transition.CandidateSHA == run.HeadSHA ||
		(!staleProof && !replayRetarget) ||
		transition.RequireValidation != true ||
		transition.Phase != phase ||
		transition.ExpectedPushActive != run.PushActive ||
		!sameNullableString(transition.PriorTargetSHA, run.ValidationTargetSHA) ||
		run.ValidationReplayCount < 0 ||
		transition.PriorReplayCount != run.ValidationReplayCount ||
		transition.OwnershipGeneration <= 0 ||
		transition.OwnershipGeneration != run.HeadAdvanceGeneration {
		return nil, fmt.Errorf("validate recoverable run head transition: transition claims do not match authoritative run state")
	}
	if replayRetarget {
		var activeTestCount int
		if err := d.sql.QueryRow(
			`SELECT COUNT(*) FROM step_results
			 WHERE run_id = ? AND step_name = ? AND status IN (?, ?)`,
			run.ID, types.StepTest, types.StepStatusRunning, types.StepStatusFixing,
		).Scan(&activeTestCount); err != nil {
			return nil, fmt.Errorf("validate recoverable run head transition: read active Test state: %w", err)
		}
		var activeCount int
		if err := d.sql.QueryRow(
			`SELECT COUNT(*) FROM step_results WHERE run_id = ? AND status IN (?, ?)`,
			run.ID, types.StepStatusRunning, types.StepStatusFixing,
		).Scan(&activeCount); err != nil {
			return nil, fmt.Errorf("validate recoverable run head transition: read active step state: %w", err)
		}
		if activeTestCount != 1 || activeCount != 1 {
			return nil, fmt.Errorf("validate recoverable run head transition: replay retarget lacks exact active Test state")
		}
	}
	nextReplayCount := run.ValidationReplayCount + 1
	if run.ValidationReplayCount > maxReplays ||
		nextReplayCount > maxReplays+1 ||
		transition.NextReplayCount != nextReplayCount ||
		transition.NextTargetSHA == nil ||
		*transition.NextTargetSHA != transition.CandidateSHA {
		return nil, fmt.Errorf("validate recoverable run head transition: replay claims exceed or contradict bounded policy")
	}
	return run, nil
}

func sameNullableString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func sameRunHeadTransition(left, right *RunHeadTransition) bool {
	return left != nil && right != nil &&
		left.RunID == right.RunID &&
		left.SourceRef == right.SourceRef &&
		left.PreviousSHA == right.PreviousSHA &&
		left.CandidateSHA == right.CandidateSHA &&
		left.RequireValidation == right.RequireValidation &&
		left.Phase == right.Phase &&
		left.ExpectedPushActive == right.ExpectedPushActive &&
		sameNullableString(left.PriorTargetSHA, right.PriorTargetSHA) &&
		sameNullableString(left.NextTargetSHA, right.NextTargetSHA) &&
		left.PriorReplayCount == right.PriorReplayCount &&
		left.NextReplayCount == right.NextReplayCount &&
		left.OwnershipGeneration == right.OwnershipGeneration
}

func (d *DB) BeginRunHeadAdvance(id, sourceRef, previousSHA, candidateSHA string, requireValidation bool, maxReplays int, phase HeadAdvancePhase) (*RunHeadTransition, error) {
	if strings.TrimSpace(sourceRef) == "" || strings.TrimSpace(previousSHA) == "" || strings.TrimSpace(candidateSHA) == "" || maxReplays <= 0 {
		return nil, fmt.Errorf("begin run head advance: source ref, SHAs, and replay bound are required")
	}
	expectedPushActive, err := headAdvancePushActive(phase)
	if err != nil {
		return nil, fmt.Errorf("begin run head advance: %w", err)
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin run head advance: begin: %w", err)
	}
	defer tx.Rollback()

	var status types.RunStatus
	var headSHA string
	var testHeadSHA *string
	var targetSHA *string
	var replayCount int
	var generation int64
	var custodyReturnedAt *int64
	var pushActive bool
	if err := tx.QueryRow(
		`SELECT status, head_sha, test_head_sha, validation_target_sha, COALESCE(validation_replay_count, 0),
		        COALESCE(head_advance_generation, 0), custody_returned_at, COALESCE(push_active, 0)
		 FROM runs WHERE id = ?`, id,
	).Scan(&status, &headSHA, &testHeadSHA, &targetSHA, &replayCount, &generation, &custodyReturnedAt, &pushActive); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("begin run head advance: run is missing")
		}
		return nil, fmt.Errorf("begin run head advance: read run: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil || pushActive != expectedPushActive || headSHA != previousSHA {
		return nil, fmt.Errorf("begin run head advance: run head, custody, or phase changed")
	}
	if targetSHA != nil && *targetSHA != headSHA {
		return nil, fmt.Errorf("begin run head advance: active replay target does not match run head")
	}
	if replayCount > maxReplays {
		return nil, fmt.Errorf("final-head validation did not converge after %d replay attempts", maxReplays)
	}
	replayRetarget := testHeadSHA == nil && targetSHA != nil && *targetSHA == headSHA
	staleProof := testHeadSHA != nil && *testHeadSHA == headSHA && *testHeadSHA != candidateSHA
	if requireValidation != (staleProof || replayRetarget) {
		return nil, fmt.Errorf("begin run head advance: validation policy does not match authoritative proof state")
	}
	if replayRetarget {
		if phase != HeadAdvancePipeline {
			return nil, fmt.Errorf("begin run head advance: replay retarget is outside Test pipeline custody")
		}
		var activeTestCount int
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM step_results
			 WHERE run_id = ? AND step_name = ? AND status IN (?, ?)`,
			id, types.StepTest, types.StepStatusRunning, types.StepStatusFixing,
		).Scan(&activeTestCount); err != nil {
			return nil, fmt.Errorf("begin run head advance: read active Test state: %w", err)
		}
		var activeCount int
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM step_results WHERE run_id = ? AND status IN (?, ?)`,
			id, types.StepStatusRunning, types.StepStatusFixing,
		).Scan(&activeCount); err != nil {
			return nil, fmt.Errorf("begin run head advance: read active step state: %w", err)
		}
		if activeTestCount != 1 || activeCount != 1 {
			return nil, fmt.Errorf("begin run head advance: replay retarget lacks exact active Test state")
		}
	}

	nextReplayCount := replayCount
	nextTargetSHA := targetSHA
	if requireValidation && (targetSHA == nil || *targetSHA != candidateSHA) {
		nextReplayCount++
		target := candidateSHA
		nextTargetSHA = &target
	}
	transition := &RunHeadTransition{
		RunID:               id,
		SourceRef:           sourceRef,
		PreviousSHA:         previousSHA,
		CandidateSHA:        candidateSHA,
		RequireValidation:   requireValidation,
		Phase:               phase,
		ExpectedPushActive:  expectedPushActive,
		PriorTargetSHA:      targetSHA,
		NextTargetSHA:       nextTargetSHA,
		PriorReplayCount:    replayCount,
		NextReplayCount:     nextReplayCount,
		OwnershipGeneration: generation + 1,
	}

	existing := &RunHeadTransition{}
	err = scanRunHeadTransition(tx.QueryRow(
		`SELECT `+runHeadTransitionColumns+` FROM run_head_transitions WHERE run_id = ?`, id,
	), existing)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("begin run head advance: read transition: %w", err)
	}
	if err == nil {
		transition.OwnershipGeneration = existing.OwnershipGeneration
		if sameRunHeadTransition(existing, transition) && generation == existing.OwnershipGeneration {
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("begin run head advance: commit retry: %w", err)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("begin run head advance: conflicting durable transition")
	}

	if _, err := tx.Exec(
		`INSERT INTO run_head_transitions
		 (run_id, source_ref, previous_sha, candidate_sha, require_validation, phase, expected_push_active,
		  prior_target_sha, next_target_sha, prior_replay_count, next_replay_count, ownership_generation, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, sourceRef, previousSHA, candidateSHA, requireValidation, phase, expectedPushActive,
		targetSHA, nextTargetSHA, replayCount, nextReplayCount, transition.OwnershipGeneration, now(),
	); err != nil {
		return nil, fmt.Errorf("begin run head advance: persist transition: %w", err)
	}
	result, err := tx.Exec(
		`UPDATE runs SET head_advance_generation = ?, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND COALESCE(head_advance_generation, 0) = ?
		   AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = ?`,
		transition.OwnershipGeneration, now(), id, types.RunRunning, previousSHA, generation, expectedPushActive,
	)
	if err != nil {
		return nil, fmt.Errorf("begin run head advance: claim generation: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return nil, fmt.Errorf("begin run head advance: claim generation: %w", err)
		}
		return nil, fmt.Errorf("begin run head advance: run changed concurrently")
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("begin run head advance: commit: %w", err)
	}
	return transition, nil
}

func (d *DB) FinalizeRunHeadAdvance(transition *RunHeadTransition, recovering bool, maxReplays int) (int, error) {
	if transition == nil || maxReplays <= 0 {
		return 0, fmt.Errorf("finalize run head advance: transition and replay bound are required")
	}
	exhausted := transition.RequireValidation && transition.NextReplayCount > maxReplays
	exhaustionError := fmt.Sprintf("final-head validation did not converge after %d replay attempts", maxReplays)
	clearPushActive := transition.ExpectedPushActive && (recovering || exhausted)
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, fmt.Errorf("finalize run head advance: begin: %w", err)
	}
	defer tx.Rollback()

	persisted := &RunHeadTransition{}
	err = scanRunHeadTransition(tx.QueryRow(
		`SELECT `+runHeadTransitionColumns+` FROM run_head_transitions WHERE run_id = ?`, transition.RunID,
	), persisted)
	if err == sql.ErrNoRows {
		var headSHA string
		var targetSHA *string
		var replayCount int
		var generation int64
		var status types.RunStatus
		var runError *string
		var pushActive bool
		if err := tx.QueryRow(
			`SELECT head_sha, validation_target_sha, COALESCE(validation_replay_count, 0),
			        COALESCE(head_advance_generation, 0), status, error, COALESCE(push_active, 0)
			 FROM runs WHERE id = ?`, transition.RunID,
		).Scan(&headSHA, &targetSHA, &replayCount, &generation, &status, &runError, &pushActive); err != nil {
			return 0, fmt.Errorf("finalize run head advance: read finalized run: %w", err)
		}
		if headSHA == transition.CandidateSHA && sameNullableString(targetSHA, transition.NextTargetSHA) &&
			replayCount == transition.NextReplayCount && generation == transition.OwnershipGeneration &&
			(!exhausted || (status == types.RunFailed && runError != nil && *runError == exhaustionError)) &&
			(!clearPushActive || !pushActive) {
			return replayCount, nil
		}
		return replayCount, fmt.Errorf("finalize run head advance: durable transition is missing")
	}
	if err != nil {
		return 0, fmt.Errorf("finalize run head advance: read transition: %w", err)
	}
	if !sameRunHeadTransition(persisted, transition) {
		return 0, fmt.Errorf("finalize run head advance: durable transition is corrupt or changed")
	}

	result, err := tx.Exec(
		`UPDATE runs
		 SET head_sha = ?, validation_target_sha = ?, validation_replay_count = ?,
		     ci_ready_at = CASE WHEN ? THEN NULL ELSE ci_ready_at END,
		     push_active = CASE WHEN ? THEN 0 ELSE push_active END,
		     status = CASE WHEN ? THEN ? ELSE status END,
		     error = CASE WHEN ? THEN ? ELSE error END, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND COALESCE(validation_replay_count, 0) = ?
		   AND ((validation_target_sha IS NULL AND ? IS NULL) OR validation_target_sha = ?)
		   AND COALESCE(head_advance_generation, 0) = ?
		   AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = ?`,
		transition.CandidateSHA, transition.NextTargetSHA, transition.NextReplayCount,
		transition.RequireValidation, clearPushActive,
		exhausted, types.RunFailed, exhausted, exhaustionError, now(),
		transition.RunID, types.RunRunning, transition.PreviousSHA, transition.PriorReplayCount,
		transition.PriorTargetSHA, transition.PriorTargetSHA, transition.OwnershipGeneration,
		transition.ExpectedPushActive,
	)
	if err != nil {
		return transition.PriorReplayCount, fmt.Errorf("finalize run head advance: update: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return transition.PriorReplayCount, fmt.Errorf("finalize run head advance: update: %w", err)
		}
		return transition.PriorReplayCount, fmt.Errorf("finalize run head advance: run changed concurrently")
	}
	if _, err := tx.Exec(
		`DELETE FROM run_head_transitions WHERE run_id = ? AND ownership_generation = ?`,
		transition.RunID, transition.OwnershipGeneration,
	); err != nil {
		return transition.PriorReplayCount, fmt.Errorf("finalize run head advance: clear transition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return transition.PriorReplayCount, fmt.Errorf("finalize run head advance: commit: %w", err)
	}
	return transition.NextReplayCount, nil
}

func (d *DB) FinalizeRecoveredRunHeadAdvance(transition *RunHeadTransition, maxReplays int) (int, error) {
	if _, err := d.ValidateRecoverableRunHeadTransition(transition, maxReplays); err != nil {
		return 0, err
	}
	return d.FinalizeRunHeadAdvance(transition, true, maxReplays)
}

func (d *DB) AdvanceRunHeadSHA(id, previousSHA, candidateSHA string, requireValidation bool, phase HeadAdvancePhase) (int, error) {
	if strings.TrimSpace(previousSHA) == "" || strings.TrimSpace(candidateSHA) == "" {
		return 0, fmt.Errorf("advance run head sha: previous and candidate SHAs are required")
	}
	expectedPushActive, err := headAdvancePushActive(phase)
	if err != nil {
		return 0, fmt.Errorf("advance run head sha: %w", err)
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, fmt.Errorf("advance run head sha: begin: %w", err)
	}
	defer tx.Rollback()

	var status types.RunStatus
	var headSHA string
	var targetSHA *string
	var replayCount int
	var custodyReturnedAt *int64
	var pushActive bool
	if err := tx.QueryRow(
		`SELECT status, head_sha, validation_target_sha, COALESCE(validation_replay_count, 0), custody_returned_at, COALESCE(push_active, 0)
		 FROM runs WHERE id = ?`, id,
	).Scan(&status, &headSHA, &targetSHA, &replayCount, &custodyReturnedAt, &pushActive); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("advance run head sha: run is missing")
		}
		return 0, fmt.Errorf("advance run head sha: read run: %w", err)
	}
	if status != types.RunRunning || custodyReturnedAt != nil || pushActive != expectedPushActive {
		return replayCount, fmt.Errorf("advance run head sha: run is not an active pipeline-owned candidate")
	}
	if headSHA != previousSHA && headSHA != candidateSHA {
		return replayCount, fmt.Errorf("advance run head sha: durable head changed from %s to %s", previousSHA, headSHA)
	}

	nextReplayCount := replayCount
	nextTargetSHA := targetSHA
	if requireValidation && (targetSHA == nil || *targetSHA != candidateSHA) {
		nextReplayCount++
		target := candidateSHA
		nextTargetSHA = &target
	}
	var targetValue any
	if nextTargetSHA != nil {
		targetValue = *nextTargetSHA
	}
	result, err := tx.Exec(
		`UPDATE runs
		 SET head_sha = ?, validation_target_sha = ?, validation_replay_count = ?,
		     ci_ready_at = CASE WHEN ? THEN NULL ELSE ci_ready_at END, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND COALESCE(validation_replay_count, 0) = ?
		   AND ((validation_target_sha IS NULL AND ? IS NULL) OR validation_target_sha = ?)
		   AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = ?`,
		candidateSHA, targetValue, nextReplayCount, requireValidation, now(),
		id, types.RunRunning, headSHA, replayCount, targetSHA, targetSHA, expectedPushActive,
	)
	if err != nil {
		return replayCount, fmt.Errorf("advance run head sha: update: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return replayCount, fmt.Errorf("advance run head sha: update: %w", err)
	}
	if changed != 1 {
		return replayCount, fmt.Errorf("advance run head sha: run changed concurrently")
	}
	if err := tx.Commit(); err != nil {
		return replayCount, fmt.Errorf("advance run head sha: commit: %w", err)
	}
	return nextReplayCount, nil
}

// ResetActiveStepForRecovery changes one interrupted active step back to
// pending without altering its round history. It is used only after daemon
// recovery has independently validated the run/worktree/topology.
func (d *DB) ResetActiveStepForRecovery(runID string, stepName types.StepName) error {
	result, err := d.sql.Exec(
		`UPDATE step_results
		 SET status = ?, exit_code = NULL, duration_ms = NULL, log_path = NULL, findings_json = NULL, error = NULL,
		     started_at = NULL, completed_at = NULL, last_activity_at = NULL, last_activity = NULL, agent_pid = NULL, auto_fix_limit = NULL
		 WHERE run_id = ? AND step_name = ? AND status = ?`,
		types.StepStatusPending, runID, stepName, types.StepStatusRunning,
	)
	if err != nil {
		return fmt.Errorf("reset active step for recovery: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("reset active step for recovery: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("reset active step for recovery: active step changed or is missing")
	}
	return nil
}

// UpdateRunHeadSHA updates the run head SHA and timestamp.
func (d *DB) UpdateRunHeadSHA(id, headSHA string) error {
	_, err := d.sql.Exec(`UPDATE runs SET head_sha = ?, updated_at = ? WHERE id = ?`, headSHA, now(), id)
	if err != nil {
		return fmt.Errorf("update run head sha: %w", err)
	}
	return nil
}

// UpdateRunError sets the error message on a run.
func (d *DB) UpdateRunError(id, errMsg string) error {
	return d.UpdateRunErrorStatus(id, errMsg, types.RunFailed)
}

// UpdateRunErrorStatus sets the error message and terminal status on a run.
func (d *DB) UpdateRunErrorStatus(id, errMsg string, status types.RunStatus) error {
	_, err := d.sql.Exec(`UPDATE runs SET error = ?, status = ?, push_active = 0, updated_at = ? WHERE id = ?`, errMsg, status, now(), id)
	if err != nil {
		return fmt.Errorf("update run error: %w", err)
	}
	return nil
}

// RunIntentSourceAgent is the intent_source value stamped when the driving
// agent supplied the intent explicitly via `axi run --intent`. It marks an
// authoritative, author-stated goal (score 1) as opposed to a transcript
// inference (whose source is the matched agent name: "claude", "codex", ...).
// Prompt-construction code branches on this to frame an explicit intent as
// authoritative acceptance criteria rather than a low-confidence hint.
const RunIntentSourceAgent = "agent"

// RunIntent carries the four intent-related columns persisted on a run.
type RunIntent struct {
	Summary   string
	Source    string
	SessionID string
	Score     float64
}

// UpdateRunIntent persists the inferred user intent for a run.
func (d *DB) UpdateRunIntent(id string, intent RunIntent) error {
	_, err := d.sql.Exec(
		`UPDATE runs SET intent = ?, intent_source = ?, intent_session_id = ?, intent_score = ?, updated_at = ? WHERE id = ?`,
		intent.Summary, intent.Source, intent.SessionID, intent.Score, now(), id,
	)
	if err != nil {
		return fmt.Errorf("update run intent: %w", err)
	}
	return nil
}

// SetRunAwaitingAgent marks a run as parked awaiting the driving agent,
// stamping awaiting_agent_since with the current time. Called by the executor
// when a step enters a gate (awaiting_approval / fix_review). This is a pollable
// observability signal only; it does not change gate resolution.
func (d *DB) SetRunAwaitingAgent(id string) error {
	ts := now()
	_, err := d.sql.Exec(`UPDATE runs SET awaiting_agent_since = ?, updated_at = ? WHERE id = ?`, ts, ts, id)
	if err != nil {
		return fmt.Errorf("set run awaiting agent: %w", err)
	}
	return nil
}

// ClearRunAwaitingAgent clears the awaiting-agent marker on a run. Called by the
// executor the moment the agent responds (or the approval wait is cancelled) and
// the run resumes, so awaiting_agent_since is non-nil exactly while a gate is
// actually parked.
func (d *DB) ClearRunAwaitingAgent(id string) error {
	_, err := d.sql.Exec(`UPDATE runs SET awaiting_agent_since = NULL, updated_at = ? WHERE id = ?`, now(), id)
	if err != nil {
		return fmt.Errorf("clear run awaiting agent: %w", err)
	}
	return nil
}

// AddRunParkedDuration accumulates parked-at-gate wall time onto a run's
// total. Called by the executor when a gate wait ends.
func (d *DB) AddRunParkedDuration(id string, ms int64) error {
	if ms <= 0 {
		return nil
	}
	_, err := d.sql.Exec(`UPDATE runs SET parked_ms = COALESCE(parked_ms, 0) + ?, updated_at = ? WHERE id = ?`, ms, now(), id)
	if err != nil {
		return fmt.Errorf("add run parked duration: %w", err)
	}
	return nil
}

func (d *DB) CompleteRunAwaitingAgent(id string, ms int64) error {
	if ms < 0 {
		ms = 0
	}
	_, err := d.sql.Exec(
		`UPDATE runs SET awaiting_agent_since = NULL, parked_ms = COALESCE(parked_ms, 0) + ?, updated_at = ? WHERE id = ?`,
		ms, now(), id,
	)
	if err != nil {
		return fmt.Errorf("complete run awaiting agent: %w", err)
	}
	return nil
}

// RecoverStaleRuns marks any runs stuck in pending/running status as failed
// and fails any in-progress steps. This is called at daemon startup to clean
// up after a previous crash. Returns the number of recovered runs.
func (d *DB) RecoverStaleRuns(errMsg string) (int, error) {
	return d.RecoverStaleRunsExcept(errMsg, nil)
}

// RecoverStaleRunsExcept marks active runs as failed unless their IDs appear
// in preserved. Callers use preserved only after independently proving a run
// can be reconstructed safely.
func (d *DB) RecoverStaleRunsExcept(errMsg string, preserved map[string]struct{}) (int, error) {
	ts := now()

	tx, err := d.sql.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders, args := recoveryExclusionClause(preserved)
	stepArgs := []any{
		types.StepStatusFailed, errMsg, ts,
		types.StepStatusRunning, types.StepStatusAwaitingApproval, types.StepStatusFixing, types.StepStatusFixReview,
		types.RunPending, types.RunRunning,
	}
	stepArgs = append(stepArgs, args...)
	_, err = tx.Exec(
		`UPDATE step_results SET status = ?, error = ?, completed_at = ?
		 WHERE status IN (?, ?, ?, ?) AND run_id IN (
			SELECT id FROM runs WHERE status IN (?, ?)`+placeholders+`
		 )`,
		stepArgs...,
	)
	if err != nil {
		return 0, fmt.Errorf("recover stale steps: %w", err)
	}

	// Fail stale runs. Clear any awaiting-agent marker so a recovered (now
	// failed) run is never reported as still parked awaiting the agent,
	// accumulating the marker's elapsed time into the run's parked total so
	// the parked evidence survives the crash.
	runArgs := []any{types.RunFailed, errMsg, ts, ts, ts, types.RunPending, types.RunRunning}
	runArgs = append(runArgs, args...)
	result, err := tx.Exec(
		`UPDATE runs SET status = ?, error = ?, push_active = 0,
			parked_ms = COALESCE(parked_ms, 0) + CASE
				WHEN awaiting_agent_since IS NOT NULL AND ? > awaiting_agent_since
				THEN (? - awaiting_agent_since) * 1000 ELSE 0 END,
			awaiting_agent_since = NULL, updated_at = ? WHERE status IN (?, ?)`+placeholders,
		runArgs...,
	)
	if err != nil {
		return 0, fmt.Errorf("recover stale runs: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return int(count), nil
}

func recoveryExclusionClause(preserved map[string]struct{}) (string, []any) {
	if len(preserved) == 0 {
		return "", nil
	}
	args := make([]any, 0, len(preserved))
	placeholders := make([]string, 0, len(preserved))
	for id := range preserved {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	return " AND id NOT IN (" + strings.Join(placeholders, ", ") + ")", args
}
