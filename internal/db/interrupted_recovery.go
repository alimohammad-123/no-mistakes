package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// LegacyDaemonShutdownError is the exact error persisted by older daemons when
// graceful shutdown interrupted an approval wait.
const LegacyDaemonShutdownError = "daemon shutting down"

// InterruptedGateRestore is the exact run and gate transaction restored from a
// legacy graceful-shutdown footprint.
type InterruptedGateRestore struct {
	Run  *Run
	Step *StepResult
}

// InspectLegacyInterruptedGate validates the complete legacy database footprint
// without changing it. Callers use this before performing trust and workspace
// checks, then RestoreLegacyInterruptedGate repeats the same checks in its
// claiming transaction.
func (d *DB) InspectLegacyInterruptedGate(runID, repoID, branch, headSHA, submittedHead, intent, sourceRef string, expected []types.StepName) (*InterruptedGateRestore, error) {
	if runID == "" || repoID == "" || branch == "" || headSHA == "" || submittedHead == "" || intent == "" || sourceRef == "" || len(expected) == 0 {
		return nil, fmt.Errorf("inspect interrupted gate: incomplete recovery identity")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("inspect interrupted gate: begin transaction: %w", err)
	}
	defer tx.Rollback()
	run, gate, gateStatus, err := inspectLegacyInterruptedGateTx(tx, runID, repoID, branch, headSHA, submittedHead, intent, sourceRef, expected)
	if err != nil {
		return nil, fmt.Errorf("inspect interrupted gate: %w", err)
	}
	gateCopy := *gate
	gateCopy.Status = gateStatus
	gateCopy.Error = nil
	gateCopy.CompletedAt = nil
	return &InterruptedGateRestore{Run: run, Step: &gateCopy}, nil
}

// RestoreLegacyInterruptedGate is the sole database owner of the compatibility
// transition from the old failed-shutdown footprint back to an ordinary parked
// run. The caller must independently verify repository and worktree state
// before invoking it. Every mutable database invariant is re-read and checked
// in this transaction, and any mismatch leaves the run untouched.
func (d *DB) RestoreLegacyInterruptedGate(runID, repoID, branch, headSHA, submittedHead, intent, sourceRef string, expected []types.StepName) (*InterruptedGateRestore, error) {
	if runID == "" || repoID == "" || branch == "" || headSHA == "" || submittedHead == "" || intent == "" || sourceRef == "" || len(expected) == 0 {
		return nil, fmt.Errorf("restore interrupted gate: incomplete recovery identity")
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("restore interrupted gate: begin transaction: %w", err)
	}
	defer tx.Rollback()

	run, gate, gateStatus, err := inspectLegacyInterruptedGateTx(tx, runID, repoID, branch, headSHA, submittedHead, intent, sourceRef, expected)
	if err != nil {
		return nil, fmt.Errorf("restore interrupted gate: %w", err)
	}

	ts := now()
	result, err := tx.Exec(
		`UPDATE runs SET status = ?, error = NULL, awaiting_agent_since = ?, source_ref = COALESCE(source_ref, ?), updated_at = ?
		 WHERE id = ? AND repo_id = ? AND branch = ? AND head_sha = ? AND submitted_head_sha = ?
		 AND intent = ? AND intent_source = ? AND intent_score = 1 AND status = ? AND error = ?
		 AND awaiting_agent_since IS NULL AND custody_returned_at IS NULL`,
		types.RunRunning, ts, sourceRef, ts, run.ID, repoID, branch, headSHA, submittedHead,
		intent, RunIntentSourceAgent, types.RunFailed, LegacyDaemonShutdownError,
	)
	if err != nil {
		return nil, fmt.Errorf("restore interrupted gate: update run: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return nil, fmt.Errorf("restore interrupted gate: run changed before claim")
	}

	result, err = tx.Exec(
		`UPDATE step_results SET status = ?, error = NULL, completed_at = NULL, last_activity_at = ?, last_activity = ?, agent_pid = NULL
		 WHERE id = ? AND run_id = ? AND status = ? AND error = ? AND completed_at IS NOT NULL`,
		gateStatus, ts, "status: "+string(gateStatus), gate.ID, run.ID, types.StepStatusFailed, LegacyDaemonShutdownError,
	)
	if err != nil {
		return nil, fmt.Errorf("restore interrupted gate: update step: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return nil, fmt.Errorf("restore interrupted gate: interrupted step changed before claim")
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("restore interrupted gate: commit transaction: %w", err)
	}
	// Build the claimed snapshots from the values committed above. A database
	// read after commit could fail after the state is already active, leaving the
	// caller unable to register its executor.
	restoredRun := *run
	restoredRun.Status = types.RunRunning
	restoredRun.Error = nil
	restoredRun.AwaitingAgentSince = &ts
	restoredRun.SourceRef = &sourceRef
	restoredRun.UpdatedAt = ts
	restoredStep := *gate
	restoredStep.Status = gateStatus
	restoredStep.Error = nil
	restoredStep.CompletedAt = nil
	restoredStep.LastActivityAt = &ts
	activity := "status: " + string(gateStatus)
	restoredStep.LastActivity = &activity
	restoredStep.AgentPID = nil
	return &InterruptedGateRestore{Run: &restoredRun, Step: &restoredStep}, nil
}

func inspectLegacyInterruptedGateTx(tx *sql.Tx, runID, repoID, branch, headSHA, submittedHead, intent, sourceRef string, expected []types.StepName) (*Run, *StepResult, types.StepStatus, error) {
	run := &Run{}
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), run); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, "", fmt.Errorf("run is missing")
		}
		return nil, nil, "", fmt.Errorf("read run: %w", err)
	}
	if err := validateLegacyInterruptedRun(tx, run, repoID, branch, headSHA, submittedHead, intent, sourceRef); err != nil {
		return nil, nil, "", err
	}
	steps, err := getStepsByRunTx(tx, run.ID)
	if err != nil {
		return nil, nil, "", err
	}
	gate, gateStatus, err := validateLegacyInterruptedSteps(tx, steps, expected)
	if err != nil {
		return nil, nil, "", err
	}
	return run, gate, gateStatus, nil
}

// FailClaimedInterruptedGate atomically terminalizes a compatibility claim
// that failed its post-claim Git integrity checks. It clears the parked marker
// because no executor will be registered for the run.
func (d *DB) FailClaimedInterruptedGate(runID, stepID, errMsg string, durationMS int64) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("fail claimed interrupted gate: begin transaction: %w", err)
	}
	defer tx.Rollback()
	ts := now()
	result, err := tx.Exec(
		`UPDATE runs SET status = ?, error = ?, awaiting_agent_since = NULL, push_active = 0, updated_at = ?
		 WHERE id = ? AND status = ? AND awaiting_agent_since IS NOT NULL`,
		types.RunFailed, errMsg, ts, runID, types.RunRunning,
	)
	if err != nil {
		return fmt.Errorf("fail claimed interrupted gate: update run: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("fail claimed interrupted gate: run changed")
	}
	result, err = tx.Exec(
		`UPDATE step_results SET status = ?, error = ?, duration_ms = ?, completed_at = ?, last_activity_at = ?, last_activity = ?, agent_pid = NULL
		 WHERE id = ? AND run_id = ? AND status IN (?, ?)`,
		types.StepStatusFailed, errMsg, durationMS, ts, ts, "step failed: "+errMsg, stepID, runID,
		types.StepStatusAwaitingApproval, types.StepStatusFixReview,
	)
	if err != nil {
		return fmt.Errorf("fail claimed interrupted gate: update step: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("fail claimed interrupted gate: step changed")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fail claimed interrupted gate: commit: %w", err)
	}
	return nil
}

func validateLegacyInterruptedRun(tx *sql.Tx, run *Run, repoID, branch, headSHA, submittedHead, intent, sourceRef string) error {
	if run.RepoID != repoID || run.Branch != branch || run.HeadSHA != headSHA {
		return fmt.Errorf("repository, branch, or pipeline head mismatch")
	}
	if run.Status != types.RunFailed || run.Error == nil || *run.Error != LegacyDaemonShutdownError || run.AwaitingAgentSince != nil {
		return fmt.Errorf("run does not have the exact legacy graceful-shutdown signature")
	}
	if run.SubmittedHeadSHA == nil || *run.SubmittedHeadSHA != submittedHead || run.Intent == nil || *run.Intent != intent ||
		run.IntentSource == nil || *run.IntentSource != RunIntentSourceAgent || run.IntentScore == nil || *run.IntentScore != 1 {
		return fmt.Errorf("submitted head or authoritative intent does not match")
	}
	if run.CustodyReturnedAt != nil {
		return fmt.Errorf("pipeline custody was already returned")
	}
	if run.LastPushedSHA != nil || run.PushTargetKind != nil || run.PushTargetFingerprint != nil || run.PushRef != nil || run.LastPushedAt != nil || run.PushGeneration != nil || run.PushActive {
		return fmt.Errorf("run has push provenance")
	}
	if run.PRURL != nil || (run.PRState != nil && *run.PRState != "" && *run.PRState != "none") || run.PRStateObservedAt != nil || run.CIReadyAt != nil {
		return fmt.Errorf("run has pull request or CI provenance")
	}
	if run.SourceRef != nil && *run.SourceRef != sourceRef {
		return fmt.Errorf("stored source ref does not match durable branch identity")
	}
	var newest string
	if err := tx.QueryRow(`SELECT id FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC LIMIT 1`, repoID, branch).Scan(&newest); err != nil {
		return fmt.Errorf("read newest branch run: %w", err)
	}
	if newest != run.ID {
		return fmt.Errorf("a newer run exists for the branch")
	}
	var active int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM runs WHERE repo_id = ? AND branch = ? AND status IN (?, ?)`, repoID, branch, types.RunPending, types.RunRunning).Scan(&active); err != nil {
		return fmt.Errorf("check active branch runs: %w", err)
	}
	if active != 0 {
		return fmt.Errorf("an active run already exists for the branch")
	}
	return nil
}

func getStepsByRunTx(tx *sql.Tx, runID string) ([]*StepResult, error) {
	rows, err := tx.Query(`SELECT `+stepResultColumns+` FROM step_results WHERE run_id = ? ORDER BY step_order`, runID)
	if err != nil {
		return nil, fmt.Errorf("read step topology: %w", err)
	}
	defer rows.Close()
	var steps []*StepResult
	for rows.Next() {
		step := &StepResult{}
		if err := rows.Scan(&step.ID, &step.RunID, &step.StepName, &step.StepOrder, &step.Status, &step.ExitCode, &step.DurationMS, &step.LogPath, &step.FindingsJSON, &step.Error, &step.StartedAt, &step.CompletedAt, &step.LastActivityAt, &step.LastActivity, &step.AgentPID, &step.AutoFixLimit); err != nil {
			return nil, fmt.Errorf("scan step topology: %w", err)
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func validateLegacyInterruptedSteps(tx *sql.Tx, steps []*StepResult, expected []types.StepName) (*StepResult, types.StepStatus, error) {
	if len(steps) != len(expected) {
		return nil, "", fmt.Errorf("ambiguous step topology: got %d steps, want %d", len(steps), len(expected))
	}
	gateIndex := -1
	var gate *StepResult
	for i, step := range steps {
		if step.StepName != expected[i] || step.StepOrder != expected[i].Order() {
			return nil, "", fmt.Errorf("ambiguous step topology at position %d", i)
		}
		if step.Status == types.StepStatusFailed && step.Error != nil && *step.Error == LegacyDaemonShutdownError {
			if gate != nil {
				return nil, "", fmt.Errorf("ambiguous step topology: multiple interrupted steps")
			}
			gate, gateIndex = step, i
		}
	}
	if gate == nil {
		return nil, "", fmt.Errorf("no interrupted approval step")
	}
	for i, step := range steps {
		switch {
		case i < gateIndex:
			if step.Status != types.StepStatusCompleted || step.CompletedAt == nil || step.Error != nil {
				return nil, "", fmt.Errorf("step %s before interrupted gate is not completed", step.StepName)
			}
		case i == gateIndex:
			if step.StartedAt == nil || step.CompletedAt == nil || step.DurationMS == nil || step.AgentPID != nil || step.FindingsJSON == nil || strings.TrimSpace(*step.FindingsJSON) == "" {
				return nil, "", fmt.Errorf("interrupted approval step is incomplete")
			}
			findings, err := types.ParseFindingsJSON(*step.FindingsJSON)
			if err != nil || len(findings.Items) == 0 {
				return nil, "", fmt.Errorf("interrupted approval step has no preserved finding payload")
			}
		case i > gateIndex:
			if step.Status != types.StepStatusPending || step.StartedAt != nil || step.CompletedAt != nil || step.Error != nil || step.FindingsJSON != nil || step.AgentPID != nil {
				return nil, "", fmt.Errorf("step %s after interrupted gate is not pristine pending", step.StepName)
			}
		}
	}

	rounds, err := getRoundsByStepTx(tx, gate.ID)
	if err != nil {
		return nil, "", err
	}
	if len(rounds) == 0 {
		return nil, "", fmt.Errorf("interrupted approval step has no preserved round")
	}
	for i, round := range rounds {
		if round.Round != i+1 {
			return nil, "", fmt.Errorf("interrupted approval step has ambiguous rounds")
		}
	}
	latest := rounds[len(rounds)-1]
	if latest.FindingsJSON == nil || *latest.FindingsJSON != *gate.FindingsJSON {
		return nil, "", fmt.Errorf("interrupted approval step findings do not match its latest round")
	}
	status := types.StepStatusAwaitingApproval
	if latest.IsFixRound() {
		status = types.StepStatusFixReview
	} else if latest.Trigger != "initial" {
		return nil, "", fmt.Errorf("interrupted approval step has unknown round trigger %q", latest.Trigger)
	}
	return gate, status, nil
}

func getRoundsByStepTx(tx *sql.Tx, stepID string) ([]*StepRound, error) {
	rows, err := tx.Query(`SELECT id, step_result_id, round, trigger_type, findings_json, user_findings_json, selected_finding_ids, selection_source, fix_summary, duration_ms, created_at FROM step_rounds WHERE step_result_id = ? ORDER BY round`, stepID)
	if err != nil {
		return nil, fmt.Errorf("read interrupted step rounds: %w", err)
	}
	defer rows.Close()
	var rounds []*StepRound
	for rows.Next() {
		round := &StepRound{}
		if err := rows.Scan(&round.ID, &round.StepResultID, &round.Round, &round.Trigger, &round.FindingsJSON, &round.UserFindingsJSON, &round.SelectedFindingIDs, &round.SelectionSource, &round.FixSummary, &round.DurationMS, &round.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan interrupted step rounds: %w", err)
		}
		rounds = append(rounds, round)
	}
	return rounds, rows.Err()
}
