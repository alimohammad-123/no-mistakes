package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	RunRecoveryExactFinalHeadCapacity = "exact_final_head_capacity"
	ExactRecoveryDeliveryProtocol     = 2
)

func ExactFinalHeadCapacityStepError(maxReplays int) string {
	return fmt.Sprintf("%s: final-head validation did not converge after %d replay attempts", ErrHeadValidationMutationExhausted, maxReplays)
}

func ExactFinalHeadCapacityRunError(maxReplays int) string {
	return fmt.Sprintf("step %s failed: %s", types.StepDocument, ExactFinalHeadCapacityStepError(maxReplays))
}

// RunRecoveryEvent is an append-only audit record for an intentional revival
// of one terminal run. The snapshot preserves the terminal state that existed
// immediately before the claim so the transition cannot look like a silent
// status rewrite.
type RunRecoveryEvent struct {
	ID                    string
	RunID                 string
	Kind                  string
	RecoveredAt           int64
	EvidenceToken         string
	PriorStatus           types.RunStatus
	PriorError            string
	HeadSHA               string
	TestHeadSHA           string
	ValidationTargetSHA   string
	ReplayCount           int
	SourceRef             string
	PRURL                 string
	LastPushedSHA         string
	PushTargetKind        string
	PushTargetFingerprint string
	PushGeneration        int64
	LastPushedAt          int64
	DocumentStepID        string
	PriorStepStatus       types.StepStatus
	PriorStepError        string
	DeliveryProtocol      int
	AnchorRef             string
}

// ExactFinalHeadCapacityFailure is a read-only admission snapshot. Its token
// binds the later transactional claim to the complete durable run and step
// evidence inspected before external Git and PR checks.
type ExactFinalHeadCapacityFailure struct {
	Run           *Run
	Document      *StepResult
	EvidenceToken string
	LastPushedSHA string
	StoredPRURL   string
	CanonicalRef  string
}

func (d *DB) GetRunRecoveryEvent(runID, kind string) (*RunRecoveryEvent, error) {
	return getRunRecoveryEvent(d.sql, runID, kind)
}

type queryRower interface {
	QueryRow(query string, args ...any) *sql.Row
}

func getRunRecoveryEvent(q queryRower, runID, kind string) (*RunRecoveryEvent, error) {
	var event RunRecoveryEvent
	err := q.QueryRow(
		`SELECT id, run_id, kind, recovered_at, evidence_token, prior_status, prior_error,
		        head_sha, test_head_sha, validation_target_sha, replay_count, source_ref,
		        pr_url, last_pushed_sha, push_target_kind, push_target_fingerprint,
		        push_generation, last_pushed_at, document_step_id, prior_step_status, prior_step_error,
		        COALESCE(delivery_protocol_version, 0), COALESCE(anchor_ref, '')
		 FROM run_recovery_events WHERE run_id = ? AND kind = ?`,
		runID, kind,
	).Scan(
		&event.ID, &event.RunID, &event.Kind, &event.RecoveredAt, &event.EvidenceToken,
		&event.PriorStatus, &event.PriorError, &event.HeadSHA, &event.TestHeadSHA,
		&event.ValidationTargetSHA, &event.ReplayCount, &event.SourceRef, &event.PRURL,
		&event.LastPushedSHA, &event.PushTargetKind, &event.PushTargetFingerprint,
		&event.PushGeneration, &event.LastPushedAt, &event.DocumentStepID,
		&event.PriorStepStatus, &event.PriorStepError, &event.DeliveryProtocol, &event.AnchorRef,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get run recovery event: %w", err)
	}
	return &event, nil
}

func (d *DB) InspectExactFinalHeadCapacityFailure(runID string, maxReplays int, expected []types.StepName) (*ExactFinalHeadCapacityFailure, error) {
	return inspectExactFinalHeadCapacityFailure(d.sql, runID, maxReplays, expected)
}

type exactRecoveryQuerier interface {
	queryRower
	Query(query string, args ...any) (*sql.Rows, error)
}

func inspectExactFinalHeadCapacityFailure(q exactRecoveryQuerier, runID string, maxReplays int, expected []types.StepName) (*ExactFinalHeadCapacityFailure, error) {
	if strings.TrimSpace(runID) == "" || maxReplays <= 0 || len(expected) == 0 {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: run, replay bound, and topology are required")
	}
	var run Run
	if err := scanRun(q.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("inspect exact final-head capacity failure: run is missing")
		}
		return nil, fmt.Errorf("inspect exact final-head capacity failure: %w", err)
	}
	canonicalRef, err := validateExactFinalHeadCapacityRun(&run, maxReplays)
	if err != nil {
		return nil, err
	}
	var latestRunID string
	if err := q.QueryRow(
		`SELECT id FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC LIMIT 1`,
		run.RepoID, run.Branch,
	).Scan(&latestRunID); err != nil {
		return nil, fmt.Errorf("inspect exact final-head capacity failure latest branch run: %w", err)
	}
	if latestRunID != run.ID {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: run is historical and no longer the latest branch run")
	}
	if event, err := getRunRecoveryEvent(q, runID, RunRecoveryExactFinalHeadCapacity); err != nil {
		return nil, err
	} else if event != nil {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: run already has an explicit recovery event")
	}
	var pendingTransition bool
	if err := q.QueryRow(`SELECT EXISTS(SELECT 1 FROM run_head_transitions WHERE run_id = ?)`, runID).Scan(&pendingTransition); err != nil {
		return nil, fmt.Errorf("inspect exact final-head capacity failure transition: %w", err)
	}
	if pendingTransition {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: a head transition is pending")
	}
	steps, err := exactRecoverySteps(q, runID)
	if err != nil {
		return nil, err
	}
	document, err := validateExactFinalHeadCapacitySteps(q, runID, steps, expected, maxReplays, false)
	if err != nil {
		return nil, err
	}
	token, err := exactFinalHeadRecoveryEvidenceToken(&run, steps, document)
	if err != nil {
		return nil, err
	}
	return &ExactFinalHeadCapacityFailure{
		Run:           &run,
		Document:      document,
		EvidenceToken: token,
		LastPushedSHA: *run.LastPushedSHA,
		StoredPRURL:   *run.PRURL,
		CanonicalRef:  canonicalRef,
	}, nil
}

func validateExactFinalHeadCapacityRun(run *Run, maxReplays int) (string, error) {
	refuse := func(reason string) (string, error) {
		return "", fmt.Errorf("inspect exact final-head capacity failure: %s", reason)
	}
	if run == nil || run.Status != types.RunFailed || run.Error == nil || *run.Error != ExactFinalHeadCapacityRunError(maxReplays) {
		return refuse("terminal status or capacity error does not match")
	}
	if run.TestHeadSHA == nil || run.ValidationTargetSHA == nil || *run.TestHeadSHA != run.HeadSHA ||
		*run.ValidationTargetSHA != run.HeadSHA || run.ValidationReplayCount != maxReplays {
		return refuse("exact Test proof and replay target do not match the capacity boundary")
	}
	if run.PushActive || run.CustodyReturnedAt != nil || run.AwaitingAgentSince != nil || run.CIReadyAt != nil {
		return refuse("run is publishing, custody-returned, parked, or already CI-ready")
	}
	if run.PRURL == nil || strings.TrimSpace(*run.PRURL) == "" {
		return refuse("stored PR identity is missing")
	}
	if run.LastPushedSHA == nil || strings.TrimSpace(*run.LastPushedSHA) == "" || *run.LastPushedSHA == run.HeadSHA ||
		run.PushTargetKind == nil || strings.TrimSpace(*run.PushTargetKind) == "" ||
		run.PushTargetFingerprint == nil || strings.TrimSpace(*run.PushTargetFingerprint) == "" ||
		run.PushRef == nil || strings.TrimSpace(*run.PushRef) == "" ||
		run.LastPushedAt == nil || run.PushGeneration == nil || *run.PushGeneration <= 0 {
		return refuse("earlier published-head provenance is missing or already at the final head")
	}
	if run.SourceRef == nil || strings.TrimSpace(*run.SourceRef) == "" {
		return refuse("frozen source-ref provenance is missing")
	}
	canonicalRef, err := run.FrozenSourceRef()
	if err != nil {
		return refuse(err.Error())
	}
	if canonicalRef != *run.SourceRef {
		return refuse("frozen source-ref provenance is inconsistent")
	}
	if *run.PushRef != canonicalRef {
		return refuse("push ref does not match the frozen source ref")
	}
	return canonicalRef, nil
}

func exactRecoverySteps(q exactRecoveryQuerier, runID string) ([]*StepResult, error) {
	rows, err := q.Query(
		`SELECT id, run_id, step_name, step_order, status, exit_code, duration_ms, log_path,
		        findings_json, error, started_at, completed_at, last_activity_at,
		        last_activity, agent_pid, auto_fix_limit
		 FROM step_results WHERE run_id = ? ORDER BY step_order ASC`, runID,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect exact final-head capacity steps: %w", err)
	}
	defer rows.Close()
	var steps []*StepResult
	for rows.Next() {
		var step StepResult
		if err := rows.Scan(
			&step.ID, &step.RunID, &step.StepName, &step.StepOrder, &step.Status,
			&step.ExitCode, &step.DurationMS, &step.LogPath, &step.FindingsJSON, &step.Error,
			&step.StartedAt, &step.CompletedAt, &step.LastActivityAt, &step.LastActivity,
			&step.AgentPID, &step.AutoFixLimit,
		); err != nil {
			return nil, fmt.Errorf("inspect exact final-head capacity step: %w", err)
		}
		steps = append(steps, &step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inspect exact final-head capacity steps: %w", err)
	}
	return steps, nil
}

func validateExactFinalHeadCapacitySteps(q queryRower, runID string, steps []*StepResult, expected []types.StepName, maxReplays int, active bool) (*StepResult, error) {
	if len(steps) != len(expected) {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: topology has %d steps, want %d", len(steps), len(expected))
	}
	documentIndex := -1
	for index, step := range steps {
		if step.StepName != expected[index] || step.StepOrder != expected[index].Order() {
			return nil, fmt.Errorf("inspect exact final-head capacity failure: topology changed at step %d", index)
		}
		if step.StepName == types.StepDocument {
			documentIndex = index
		}
	}
	if documentIndex < 0 {
		return nil, fmt.Errorf("inspect exact final-head capacity failure: Document step is missing")
	}
	if active {
		interruptedSeen := false
		activeSeen := false
		for index, step := range steps {
			if index < documentIndex {
				if step.StepName == types.StepTest {
					if step.Status != types.StepStatusCompleted || step.Error != nil {
						return nil, fmt.Errorf("inspect exact final-head capacity recovery: Test is not complete")
					}
				} else if step.Status != types.StepStatusCompleted && step.Status != types.StepStatusSkipped {
					return nil, fmt.Errorf("inspect exact final-head capacity recovery: predecessor %s is %s", step.StepName, step.Status)
				}
				continue
			}
			switch step.Status {
			case types.StepStatusCompleted, types.StepStatusSkipped:
				if interruptedSeen {
					return nil, fmt.Errorf("inspect exact final-head capacity recovery: completed step %s follows the interrupted suffix", step.StepName)
				}
			case types.StepStatusRunning, types.StepStatusFixing:
				if interruptedSeen || activeSeen {
					return nil, fmt.Errorf("inspect exact final-head capacity recovery: multiple suffix steps are active")
				}
				activeSeen = true
				interruptedSeen = true
			case types.StepStatusPending:
				interruptedSeen = true
			default:
				return nil, fmt.Errorf("inspect exact final-head capacity recovery: suffix step %s is %s", step.StepName, step.Status)
			}
		}
		return steps[documentIndex], nil
	}
	for index, step := range steps {
		switch {
		case index < documentIndex:
			if step.StepName == types.StepTest {
				if step.Status != types.StepStatusCompleted || step.Error != nil {
					return nil, fmt.Errorf("inspect exact final-head capacity failure: Test is not complete")
				}
			} else if step.Status != types.StepStatusCompleted && step.Status != types.StepStatusSkipped {
				return nil, fmt.Errorf("inspect exact final-head capacity failure: predecessor %s is %s", step.StepName, step.Status)
			}
		case index == documentIndex:
			if step.Status != types.StepStatusFailed || step.Error == nil || *step.Error != ExactFinalHeadCapacityStepError(maxReplays) ||
				step.StartedAt == nil || step.CompletedAt == nil || step.DurationMS == nil || step.ExitCode != nil ||
				step.FindingsJSON != nil || step.AgentPID != nil {
				return nil, fmt.Errorf("inspect exact final-head capacity failure: Document failure is not the exact pre-round capacity boundary")
			}
			var newRounds int
			if err := q.QueryRow(
				`SELECT COUNT(*) FROM step_rounds WHERE step_result_id = ? AND created_at >= ?`,
				step.ID, *step.StartedAt,
			).Scan(&newRounds); err != nil {
				return nil, fmt.Errorf("inspect exact final-head capacity Document rounds: %w", err)
			}
			var newInvocations int
			if err := q.QueryRow(
				`SELECT COUNT(*) FROM agent_invocations WHERE run_id = ? AND step_name = ? AND started_at >= ?`,
				runID, string(types.StepDocument), *step.StartedAt,
			).Scan(&newInvocations); err != nil {
				return nil, fmt.Errorf("inspect exact final-head capacity Document invocations: %w", err)
			}
			if newRounds != 0 || newInvocations != 0 {
				return nil, fmt.Errorf("inspect exact final-head capacity failure: Document failure advanced into a round")
			}
		case index > documentIndex:
			if step.Status != types.StepStatusPending || step.StartedAt != nil || step.CompletedAt != nil ||
				step.ExitCode != nil || step.DurationMS != nil || step.LogPath != nil || step.FindingsJSON != nil ||
				step.Error != nil || step.AgentPID != nil {
				return nil, fmt.Errorf("inspect exact final-head capacity failure: successor %s is not pristine pending", step.StepName)
			}
		}
	}
	return steps[documentIndex], nil
}

func exactFinalHeadRecoveryEvidenceToken(run *Run, steps []*StepResult, document *StepResult) (string, error) {
	payload := struct {
		Run      *Run
		Steps    []*StepResult
		Document *StepResult
	}{Run: run, Steps: steps, Document: document}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode exact final-head recovery evidence: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (d *DB) RestoreExactFinalHeadCapacityFailure(runID, evidenceToken string, maxReplays int, expected []types.StepName) (*Run, error) {
	if strings.TrimSpace(evidenceToken) == "" {
		return nil, fmt.Errorf("restore exact final-head capacity failure: evidence token is required")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: begin: %w", err)
	}
	defer tx.Rollback()
	failure, err := inspectExactFinalHeadCapacityFailure(tx, runID, maxReplays, expected)
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: %w", err)
	}
	if failure.EvidenceToken != evidenceToken {
		return nil, fmt.Errorf("restore exact final-head capacity failure: durable evidence changed before claim")
	}
	anchorRef, err := sourceprovenance.ExactRecoveryAnchorRef(runID)
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: %w", err)
	}
	ts := now()
	result, err := tx.Exec(
		`INSERT INTO run_recovery_events (
			id, run_id, kind, recovered_at, evidence_token, prior_status, prior_error,
			head_sha, test_head_sha, validation_target_sha, replay_count, source_ref,
			pr_url, last_pushed_sha, push_target_kind, push_target_fingerprint,
			push_generation, last_pushed_at, document_step_id, prior_step_status, prior_step_error,
			delivery_protocol_version, anchor_ref
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newID(), runID, RunRecoveryExactFinalHeadCapacity, ts, failure.EvidenceToken,
		failure.Run.Status, *failure.Run.Error, failure.Run.HeadSHA, *failure.Run.TestHeadSHA,
		*failure.Run.ValidationTargetSHA, failure.Run.ValidationReplayCount, failure.CanonicalRef,
		failure.StoredPRURL, failure.LastPushedSHA, *failure.Run.PushTargetKind,
		*failure.Run.PushTargetFingerprint, *failure.Run.PushGeneration, *failure.Run.LastPushedAt,
		failure.Document.ID, failure.Document.Status, *failure.Document.Error,
		ExactRecoveryDeliveryProtocol, anchorRef,
	)
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: append recovery provenance: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return nil, fmt.Errorf("restore exact final-head capacity failure: recovery provenance was not appended")
	}
	result, err = tx.Exec(
		`UPDATE runs SET status = ?, error = NULL, awaiting_agent_since = NULL, ci_ready_at = NULL, updated_at = ?
		 WHERE id = ? AND status = ? AND error = ? AND head_sha = ? AND test_head_sha = ?
		   AND validation_target_sha = ? AND validation_replay_count = ?
		   AND custody_returned_at IS NULL AND COALESCE(push_active, 0) = 0`,
		types.RunRunning, ts, runID, types.RunFailed, ExactFinalHeadCapacityRunError(maxReplays),
		failure.Run.HeadSHA, failure.Run.HeadSHA, failure.Run.HeadSHA, maxReplays,
	)
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: revive run: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return nil, fmt.Errorf("restore exact final-head capacity failure: terminal run changed before claim")
	}
	result, err = tx.Exec(
		`UPDATE step_results
		 SET status = ?, exit_code = NULL, duration_ms = NULL, log_path = NULL,
		     findings_json = NULL, error = NULL, started_at = NULL, completed_at = NULL,
		     last_activity_at = NULL, last_activity = NULL, agent_pid = NULL, auto_fix_limit = NULL
		 WHERE run_id = ? AND step_order >= ?`,
		types.StepStatusPending, runID, types.StepDocument.Order(),
	)
	if err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: reset delivery suffix: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != int64(len(expected)-types.StepDocument.Order()+1) {
		return nil, fmt.Errorf("restore exact final-head capacity failure: delivery suffix topology changed")
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("restore exact final-head capacity failure: commit: %w", err)
	}
	restored, err := d.GetRun(runID)
	if err != nil {
		return nil, err
	}
	return restored, nil
}

func (d *DB) ValidateActiveExactFinalHeadCapacityRecovery(runID string, maxReplays int, expected []types.StepName) error {
	event, err := d.GetRunRecoveryEvent(runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		return fmt.Errorf("validate active exact final-head capacity recovery: recovery provenance is missing")
	}
	run, err := d.GetRun(runID)
	if err != nil {
		return err
	}
	targetValid := run != nil && (run.ValidationTargetSHA == nil || *run.ValidationTargetSHA == event.ValidationTargetSHA)
	lastPushValid := run != nil && run.LastPushedSHA != nil && (*run.LastPushedSHA == event.LastPushedSHA || *run.LastPushedSHA == event.HeadSHA)
	if run == nil || run.Status != types.RunRunning || run.Error != nil || run.AwaitingAgentSince != nil ||
		run.CustodyReturnedAt != nil || run.PushActive || run.HeadSHA != event.HeadSHA ||
		run.TestHeadSHA == nil || *run.TestHeadSHA != event.TestHeadSHA || !targetValid ||
		run.ValidationReplayCount != maxReplays || event.ReplayCount != maxReplays ||
		run.PRURL == nil || *run.PRURL != event.PRURL || !lastPushValid {
		return fmt.Errorf("validate active exact final-head capacity recovery: run proof or delivery identity changed")
	}
	expectedAnchor, anchorErr := sourceprovenance.ExactRecoveryAnchorRef(runID)
	if event.PriorStatus != types.RunFailed || event.PriorError != ExactFinalHeadCapacityRunError(maxReplays) ||
		event.PriorStepStatus != types.StepStatusFailed || event.PriorStepError != ExactFinalHeadCapacityStepError(maxReplays) ||
		anchorErr != nil || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol || event.AnchorRef != expectedAnchor ||
		event.PushTargetKind == "" || event.PushTargetFingerprint == "" || event.PushGeneration <= 0 || event.LastPushedAt <= 0 ||
		run.PushTargetKind == nil || *run.PushTargetKind != event.PushTargetKind ||
		run.PushTargetFingerprint == nil || *run.PushTargetFingerprint != event.PushTargetFingerprint ||
		run.PushRef == nil || *run.PushRef != event.SourceRef || run.PushGeneration == nil || run.LastPushedAt == nil {
		return fmt.Errorf("validate active exact final-head capacity recovery: audit provenance is inconsistent")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil || ref != event.SourceRef {
		return fmt.Errorf("validate active exact final-head capacity recovery: source ref changed")
	}
	if transition, err := d.GetRunHeadTransition(runID); err != nil || transition != nil {
		return fmt.Errorf("validate active exact final-head capacity recovery: head transition is pending")
	}
	steps, err := exactRecoverySteps(d.sql, runID)
	if err != nil {
		return err
	}
	document, err := validateExactFinalHeadCapacitySteps(d.sql, runID, steps, expected, maxReplays, true)
	if err != nil {
		return err
	}
	if document.ID != event.DocumentStepID {
		return fmt.Errorf("validate active exact final-head capacity recovery: Document identity changed")
	}
	var documentStatus, lintStatus, pushStatus, prStatus types.StepStatus
	var prStepID string
	for _, step := range steps {
		switch step.StepName {
		case types.StepDocument:
			documentStatus = step.Status
		case types.StepLint:
			lintStatus = step.Status
		case types.StepPush:
			pushStatus = step.Status
		case types.StepPR:
			prStatus = step.Status
			prStepID = step.ID
		}
	}
	completed := func(status types.StepStatus) bool {
		return status == types.StepStatusCompleted || status == types.StepStatusSkipped
	}
	if run.ValidationTargetSHA == nil && (!completed(documentStatus) || !completed(lintStatus)) {
		return fmt.Errorf("validate active exact final-head capacity recovery: proof closed before Document and Lint completed")
	}
	if run.ValidationTargetSHA != nil && *run.LastPushedSHA != event.LastPushedSHA {
		return fmt.Errorf("validate active exact final-head capacity recovery: exact head published before proof closure")
	}
	if *run.LastPushedSHA == event.LastPushedSHA {
		if completed(pushStatus) || *run.PushGeneration != event.PushGeneration || *run.LastPushedAt != event.LastPushedAt {
			return fmt.Errorf("validate active exact final-head capacity recovery: Push advanced without publishing the exact head")
		}
	} else {
		pushDurable := completed(pushStatus) || pushStatus == types.StepStatusRunning
		if *run.LastPushedSHA != run.HeadSHA || !pushDurable ||
			*run.PushGeneration != event.PushGeneration+1 || *run.LastPushedAt < event.LastPushedAt {
			return fmt.Errorf("validate active exact final-head capacity recovery: exact published-head provenance is incomplete")
		}
	}
	refObservation, err := d.GetExactRecoveryRefObservation(runID)
	if err != nil {
		return err
	}
	if refObservation != nil {
		if refObservation.Provider == "" || refObservation.SourceRef != event.SourceRef ||
			refObservation.StaleOID != event.LastPushedSHA || refObservation.ExpectedOID != event.HeadSHA ||
			refObservation.DeadlineAt <= 0 {
			return fmt.Errorf("validate active exact final-head capacity recovery: ref observation identity changed")
		}
		switch refObservation.State {
		case ExactRecoveryRefObservationStale, ExactRecoveryRefObservationExpected:
		case ExactRecoveryRefObservationAmbiguous:
			return fmt.Errorf("validate active exact final-head capacity recovery: ref observation is ambiguous")
		default:
			return fmt.Errorf("validate active exact final-head capacity recovery: ref observation state is invalid")
		}
	}
	prUpdate, err := d.GetExactRecoveryPRUpdate(runID)
	if err != nil {
		return err
	}
	if prUpdate != nil {
		if prUpdate.StepResultID != prStepID || prUpdate.TargetURL != event.PRURL ||
			prUpdate.HeadSHA != event.HeadSHA || prUpdate.IntendedContentHash == "" ||
			prUpdate.PriorContentHash == "" || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol {
			return fmt.Errorf("validate active exact final-head capacity recovery: PR update provenance is inconsistent")
		}
		switch prStatus {
		case types.StepStatusRunning:
			if prUpdate.State != ExactRecoveryPRUpdatePrepared && prUpdate.State != ExactRecoveryPRUpdateApplied {
				return fmt.Errorf("validate active exact final-head capacity recovery: PR update phase is invalid")
			}
		case types.StepStatusCompleted:
			if prUpdate.State != ExactRecoveryPRUpdateApplied || prUpdate.AppliedAt == nil {
				return fmt.Errorf("validate active exact final-head capacity recovery: completed PR lacks applied provenance")
			}
		default:
			return fmt.Errorf("validate active exact final-head capacity recovery: PR update exists outside its step")
		}
	} else {
		if prStatus == types.StepStatusRunning && event.DeliveryProtocol != ExactRecoveryDeliveryProtocol {
			return fmt.Errorf("validate active exact final-head capacity recovery: interrupted PR update has ambiguous provenance")
		}
		if prStatus == types.StepStatusCompleted && event.DeliveryProtocol == ExactRecoveryDeliveryProtocol {
			return fmt.Errorf("validate active exact final-head capacity recovery: completed PR update lacks durable provenance")
		}
	}
	return nil
}
