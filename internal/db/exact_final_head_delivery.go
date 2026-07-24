package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	ExactRecoveryPRUpdatePrepared = "prepared"
	ExactRecoveryPRUpdateApplied  = "applied"
)

type ExactRecoveryPRUpdate struct {
	RunID               string
	StepResultID        string
	TargetURL           string
	HeadSHA             string
	PriorContentHash    string
	IntendedContentHash string
	IntendedTitle       string
	IntendedBody        string
	State               string
	PreparedAt          int64
	AppliedAt           *int64
}

func ExactRecoveryPRContentHash(title, body string) string {
	title = strings.TrimSpace(title)
	body = strings.ReplaceAll(body, "\r\n", "\n")
	body = strings.TrimRight(body, "\n")
	payload, _ := json.Marshal(struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}{Title: title, Body: body})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (d *DB) GetExactRecoveryPRUpdate(runID string) (*ExactRecoveryPRUpdate, error) {
	return getExactRecoveryPRUpdate(d.sql, runID)
}

func getExactRecoveryPRUpdate(q queryRower, runID string) (*ExactRecoveryPRUpdate, error) {
	var update ExactRecoveryPRUpdate
	err := q.QueryRow(
		`SELECT run_id, step_result_id, target_url, head_sha, prior_content_hash,
		        intended_content_hash, intended_title, intended_body, state, prepared_at, applied_at
		 FROM run_recovery_pr_updates WHERE run_id = ?`,
		runID,
	).Scan(
		&update.RunID, &update.StepResultID, &update.TargetURL, &update.HeadSHA,
		&update.PriorContentHash, &update.IntendedContentHash, &update.IntendedTitle,
		&update.IntendedBody, &update.State, &update.PreparedAt, &update.AppliedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get exact recovery PR update: %w", err)
	}
	return &update, nil
}

func (d *DB) PrepareExactRecoveryPRUpdate(runID, stepResultID, targetURL, headSHA, priorTitle, priorBody, intendedTitle, intendedBody string) (*ExactRecoveryPRUpdate, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(stepResultID) == "" ||
		strings.TrimSpace(targetURL) == "" || strings.TrimSpace(headSHA) == "" {
		return nil, fmt.Errorf("prepare exact recovery PR update: identity is incomplete")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("prepare exact recovery PR update: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return nil, err
	}
	if event == nil || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol || event.HeadSHA != headSHA || event.PRURL != targetURL {
		return nil, fmt.Errorf("prepare exact recovery PR update: recovery identity is inconsistent")
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return nil, fmt.Errorf("prepare exact recovery PR update: read run: %w", err)
	}
	if run.Status != types.RunRunning || run.HeadSHA != headSHA || run.TestHeadSHA == nil || *run.TestHeadSHA != headSHA ||
		run.ValidationTargetSHA != nil || run.LastPushedSHA == nil || *run.LastPushedSHA != headSHA ||
		run.PRURL == nil || *run.PRURL != targetURL || run.PushActive || run.CustodyReturnedAt != nil {
		return nil, fmt.Errorf("prepare exact recovery PR update: delivery proof is inconsistent")
	}
	var stepStatus types.StepStatus
	if err := tx.QueryRow(
		`SELECT status FROM step_results WHERE id = ? AND run_id = ? AND step_name = ?`,
		stepResultID, runID, types.StepPR,
	).Scan(&stepStatus); err != nil || stepStatus != types.StepStatusRunning {
		return nil, fmt.Errorf("prepare exact recovery PR update: PR step is not running")
	}
	var pushStatus types.StepStatus
	if err := tx.QueryRow(
		`SELECT status FROM step_results WHERE run_id = ? AND step_name = ?`,
		runID, types.StepPush,
	).Scan(&pushStatus); err != nil || pushStatus != types.StepStatusCompleted {
		return nil, fmt.Errorf("prepare exact recovery PR update: Push is not complete")
	}
	priorHash := ExactRecoveryPRContentHash(priorTitle, priorBody)
	intendedHash := ExactRecoveryPRContentHash(intendedTitle, intendedBody)
	existing, err := getExactRecoveryPRUpdate(tx, runID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.StepResultID != stepResultID || existing.TargetURL != targetURL || existing.HeadSHA != headSHA ||
			existing.PriorContentHash != priorHash || existing.IntendedContentHash != intendedHash ||
			existing.IntendedTitle != intendedTitle || existing.IntendedBody != intendedBody {
			return nil, fmt.Errorf("prepare exact recovery PR update: durable intent changed")
		}
		return existing, nil
	}
	ts := now()
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_pr_updates (
			run_id, step_result_id, target_url, head_sha, prior_content_hash,
			intended_content_hash, intended_title, intended_body, state, prepared_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runID, stepResultID, targetURL, headSHA, priorHash, intendedHash,
		intendedTitle, intendedBody, ExactRecoveryPRUpdatePrepared, ts,
	); err != nil {
		return nil, fmt.Errorf("prepare exact recovery PR update: persist intent: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("prepare exact recovery PR update: commit: %w", err)
	}
	return d.GetExactRecoveryPRUpdate(runID)
}

func (d *DB) MarkExactRecoveryPRUpdateApplied(runID, observedTitle, observedBody string) error {
	observedHash := ExactRecoveryPRContentHash(observedTitle, observedBody)
	result, err := d.sql.Exec(
		`UPDATE run_recovery_pr_updates
		 SET state = ?, applied_at = COALESCE(applied_at, ?)
		 WHERE run_id = ? AND intended_content_hash = ? AND state IN (?, ?)`,
		ExactRecoveryPRUpdateApplied, now(), runID, observedHash,
		ExactRecoveryPRUpdatePrepared, ExactRecoveryPRUpdateApplied,
	)
	if err != nil {
		return fmt.Errorf("mark exact recovery PR update applied: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("mark exact recovery PR update applied: durable intent or observed content changed")
	}
	return nil
}

func (d *DB) ExactRecoveryPushAlreadyBound(runID, headSHA string) (bool, error) {
	event, err := d.GetRunRecoveryEvent(runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil || event == nil {
		return false, err
	}
	if event.DeliveryProtocol != ExactRecoveryDeliveryProtocol || strings.TrimSpace(event.AnchorRef) == "" {
		return false, fmt.Errorf("exact recovery push binding provenance is incompatible")
	}
	run, err := d.GetRun(runID)
	if err != nil {
		return false, err
	}
	if run == nil || run.Status != types.RunRunning || run.HeadSHA != event.HeadSHA || headSHA != event.HeadSHA ||
		run.TestHeadSHA == nil || *run.TestHeadSHA != headSHA || run.ValidationTargetSHA != nil ||
		run.PushGeneration == nil || run.LastPushedAt == nil || run.PushTargetKind == nil ||
		run.PushTargetFingerprint == nil || run.PushRef == nil ||
		*run.PushTargetKind != event.PushTargetKind ||
		*run.PushTargetFingerprint != event.PushTargetFingerprint || *run.PushRef != event.SourceRef {
		return false, fmt.Errorf("exact recovery push binding proof is inconsistent")
	}
	if run.LastPushedSHA == nil {
		return false, fmt.Errorf("exact recovery push binding proof is incomplete")
	}
	if *run.LastPushedSHA == event.LastPushedSHA {
		if *run.PushGeneration != event.PushGeneration || *run.LastPushedAt != event.LastPushedAt {
			return false, fmt.Errorf("exact recovery prior push binding changed")
		}
		return false, nil
	}
	if *run.LastPushedSHA != headSHA || *run.PushGeneration != event.PushGeneration+1 ||
		*run.LastPushedAt < event.LastPushedAt {
		return false, fmt.Errorf("exact recovery push binding changed")
	}
	observation, err := d.GetExactRecoveryRefObservation(runID)
	if err != nil {
		return false, err
	}
	operation, err := d.GetExactRecoveryPushOperation(runID)
	if err != nil {
		return false, err
	}
	if (observation == nil) != (operation == nil) {
		return false, fmt.Errorf("exact recovery Push operation journal is incomplete")
	}
	if operation != nil {
		if err := validateExactRecoveryPushOperationIdentity(operation, observation, event); err != nil {
			return false, err
		}
		if err := validateExactRecoveryPushOperationPhase(operation, observation, run); err != nil {
			return false, err
		}
		if operation.Phase != ExactRecoveryPushBound {
			return false, fmt.Errorf("exact recovery push binding lacks bound operation")
		}
	}
	var pushStatus types.StepStatus
	if err := d.sql.QueryRow(
		`SELECT status FROM step_results WHERE run_id = ? AND step_name = ?`,
		runID, types.StepPush,
	).Scan(&pushStatus); err != nil || pushStatus != types.StepStatusRunning {
		return false, fmt.Errorf("exact recovery Push step is not running")
	}
	return true, nil
}

func (d *DB) ReconcileStaleExactRecoveryPushCustody(runID, remoteHead, sourceRef, sourceHead string, maxReplays int, expected []types.StepName) (bool, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(remoteHead) == "" ||
		strings.TrimSpace(sourceRef) == "" || strings.TrimSpace(sourceHead) == "" ||
		maxReplays <= 0 || len(expected) == 0 {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: identities, remote head, replay bound, and topology are required")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return false, err
	}
	if event == nil || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol ||
		event.PriorStatus != types.RunFailed || event.PriorError != ExactFinalHeadCapacityRunError(maxReplays) ||
		event.PriorStepStatus != types.StepStatusFailed || event.PriorStepError != ExactFinalHeadCapacityStepError(maxReplays) ||
		event.HeadSHA == "" || event.TestHeadSHA != event.HeadSHA || event.ValidationTargetSHA != event.HeadSHA ||
		event.ReplayCount != maxReplays || event.SourceRef == "" || event.PRURL == "" ||
		event.LastPushedSHA == "" || event.LastPushedSHA == event.HeadSHA ||
		event.PushTargetKind == "" || event.PushTargetFingerprint == "" ||
		event.PushGeneration <= 0 || event.LastPushedAt <= 0 || event.DocumentStepID == "" {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: recovery provenance is missing or incompatible")
	}
	if sourceRef != event.SourceRef || sourceHead != event.HeadSHA {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: observed source ownership changed")
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: read run: %w", err)
	}
	if !run.PushActive {
		return false, nil
	}
	if run.Status != types.RunRunning || run.Error != nil || run.AwaitingAgentSince != nil ||
		run.CustodyReturnedAt != nil || run.CIReadyAt != nil ||
		run.HeadSHA != event.HeadSHA || run.TestHeadSHA == nil || *run.TestHeadSHA != event.TestHeadSHA ||
		run.ValidationTargetSHA != nil || run.ValidationReplayCount != maxReplays || event.ReplayCount != maxReplays ||
		run.PRURL == nil || *run.PRURL != event.PRURL || run.LastPushedSHA == nil ||
		run.PushTargetKind == nil || *run.PushTargetKind != event.PushTargetKind ||
		run.PushTargetFingerprint == nil || *run.PushTargetFingerprint != event.PushTargetFingerprint ||
		run.PushRef == nil || *run.PushRef != event.SourceRef ||
		run.PushGeneration == nil || run.LastPushedAt == nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: run proof or delivery identity changed")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil || ref != event.SourceRef {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: source ref changed")
	}
	var pendingTransition bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM run_head_transitions WHERE run_id = ?)`, runID).Scan(&pendingTransition); err != nil || pendingTransition {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: head transition is pending")
	}
	var latestRunID string
	if err := tx.QueryRow(
		`SELECT id FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC LIMIT 1`,
		run.RepoID, run.Branch,
	).Scan(&latestRunID); err != nil || latestRunID != run.ID {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: run is superseded")
	}
	steps, err := exactRecoverySteps(tx, runID)
	if err != nil {
		return false, err
	}
	document, err := validateExactFinalHeadCapacitySteps(tx, runID, steps, expected, maxReplays, true)
	if err != nil {
		return false, err
	}
	if document.ID != event.DocumentStepID {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: Document identity changed")
	}
	var pushStatus types.StepStatus
	for _, step := range steps {
		if step.StepName == types.StepPush {
			pushStatus = step.Status
			break
		}
	}
	if pushStatus != types.StepStatusRunning && pushStatus != types.StepStatusCompleted {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: Push step phase is invalid")
	}
	if update, err := getExactRecoveryPRUpdate(tx, runID); err != nil {
		return false, err
	} else if update != nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: PR delivery already started")
	}
	observation, err := getExactRecoveryRefObservation(tx, runID)
	if err != nil {
		return false, err
	}
	operation, err := getExactRecoveryPushOperation(tx, runID)
	if err != nil {
		return false, err
	}
	if (observation == nil) != (operation == nil) {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: operation journal is incomplete")
	}
	if observation != nil {
		if err := validateExactRecoveryPushOperationIdentity(operation, observation, event); err != nil {
			return false, err
		}
		if observation.State != ExactRecoveryRefObservationStale {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: observation state is not stale")
		}
	}

	priorBinding := *run.LastPushedSHA == event.LastPushedSHA &&
		*run.PushGeneration == event.PushGeneration && *run.LastPushedAt == event.LastPushedAt
	exactBinding := *run.LastPushedSHA == event.HeadSHA &&
		*run.PushGeneration == event.PushGeneration+1 && *run.LastPushedAt >= event.LastPushedAt
	if operation != nil && remoteHead != operation.StaleOID && remoteHead != operation.TargetOID {
		recordErr := recordExactRecoveryRefObservation(tx, observation, operation, &run, remoteHead)
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("%v; commit remote ambiguity: %w", recordErr, commitErr)
		}
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: remote head is ambiguous")
	}
	switch {
	case priorBinding && pushStatus == types.StepStatusRunning && remoteHead == event.LastPushedSHA:
		if operation != nil {
			switch operation.Phase {
			case ExactRecoveryPushPrepared:
			case ExactRecoveryPushInvoked:
				if err := rotateExactRecoveryPushOperation(tx, operation); err != nil {
					return false, err
				}
			default:
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: prior binding has invalid operation phase")
			}
		}
	case priorBinding && pushStatus == types.StepStatusRunning && remoteHead == event.HeadSHA:
		if operation == nil {
			ts := now()
			result, err := tx.Exec(
				`UPDATE runs
				 SET last_pushed_sha = ?, push_generation = ?, last_pushed_at = ?, push_active = 0, updated_at = ?
				 WHERE id = ? AND status = ? AND head_sha = ? AND test_head_sha = ?
				   AND validation_target_sha IS NULL AND validation_replay_count = ?
				   AND last_pushed_sha = ? AND push_generation = ? AND last_pushed_at = ?
				   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
				   AND COALESCE(push_active, 0) = 1 AND custody_returned_at IS NULL`,
				event.HeadSHA, event.PushGeneration+1, ts, ts, runID, types.RunRunning,
				event.HeadSHA, event.TestHeadSHA, maxReplays, event.LastPushedSHA,
				event.PushGeneration, event.LastPushedAt, event.PushTargetKind,
				event.PushTargetFingerprint, event.SourceRef,
			)
			if err != nil {
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: bind observed exact head: %w", err)
			}
			if changed, err := result.RowsAffected(); err != nil || changed != 1 {
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: durable state changed before exact-head binding")
			}
			if err := tx.Commit(); err != nil {
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: commit exact-head binding: %w", err)
			}
			return true, nil
		}
		if operation.Phase == ExactRecoveryPushPrepared {
			recordErr := recordExactRecoveryRefObservation(tx, observation, operation, &run, remoteHead)
			if commitErr := tx.Commit(); commitErr != nil {
				return false, fmt.Errorf("%v; commit pre-invocation delivery ambiguity: %w", recordErr, commitErr)
			}
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: exact head predates Push invocation")
		}
		if operation.Phase != ExactRecoveryPushInvoked {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: exact head lacks matching invocation")
		}
		if err := bindExactRecoveryPushOperation(tx, &run, operation, event); err != nil {
			return false, err
		}
		result, err := tx.Exec(
			`UPDATE runs SET push_active = 0, updated_at = ?
			 WHERE id = ? AND status = ? AND last_pushed_sha = ? AND push_generation = ?
			   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
			   AND COALESCE(push_active, 0) = 1 AND custody_returned_at IS NULL`,
			now(), runID, types.RunRunning, operation.TargetOID, operation.TargetGeneration,
			operation.TargetKind, operation.TargetFingerprint, operation.SourceRef,
		)
		if err != nil {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: release exact binding: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: exact binding custody changed")
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: commit exact-head binding: %w", err)
		}
		return true, nil
	case exactBinding && remoteHead == event.HeadSHA:
		if operation != nil && operation.Phase != ExactRecoveryPushBound {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: exact binding lacks bound operation")
		}
	default:
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: remote head and durable binding are inconsistent")
	}
	result, err := tx.Exec(
		`UPDATE runs SET push_active = 0, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND test_head_sha = ?
		   AND validation_target_sha IS NULL AND validation_replay_count = ?
		   AND last_pushed_sha = ? AND push_generation = ? AND last_pushed_at = ?
		   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
		   AND COALESCE(push_active, 0) = 1 AND custody_returned_at IS NULL`,
		now(), runID, types.RunRunning, event.HeadSHA, event.TestHeadSHA, maxReplays,
		*run.LastPushedSHA, *run.PushGeneration, *run.LastPushedAt,
		event.PushTargetKind, event.PushTargetFingerprint, event.SourceRef,
	)
	if err != nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: release: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: durable state changed before release")
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: commit release: %w", err)
	}
	return true, nil
}

func (d *DB) CancelExactRecoveryAsSuperseded(runID string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("cancel superseded exact recovery: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		return fmt.Errorf("cancel superseded exact recovery: recovery provenance is missing")
	}
	if event.DeliveryProtocol != ExactRecoveryDeliveryProtocol || strings.TrimSpace(event.AnchorRef) == "" {
		return fmt.Errorf("cancel superseded exact recovery: recovery provenance is incompatible")
	}
	var status types.RunStatus
	var runError *string
	if err := tx.QueryRow(`SELECT status, error FROM runs WHERE id = ?`, runID).Scan(&status, &runError); err != nil {
		return fmt.Errorf("cancel superseded exact recovery: read run: %w", err)
	}
	if status == types.RunCancelled && runError != nil && *runError == types.RunCancelReasonSuperseded {
		return nil
	}
	if status != types.RunRunning {
		return fmt.Errorf("cancel superseded exact recovery: run is not active")
	}
	ts := now()
	if _, err := tx.Exec(
		`UPDATE step_results SET status = ?, error = ?, completed_at = ?, last_activity_at = ?, last_activity = ?
		 WHERE run_id = ? AND status IN (?, ?)`,
		types.StepStatusFailed, types.RunCancelReasonSuperseded, ts, ts,
		"step failed: "+types.RunCancelReasonSuperseded, runID,
		types.StepStatusRunning, types.StepStatusFixing,
	); err != nil {
		return fmt.Errorf("cancel superseded exact recovery: terminalize active step: %w", err)
	}
	result, err := tx.Exec(
		`UPDATE runs SET status = ?, error = ?, push_active = 0, updated_at = ?
		 WHERE id = ? AND status = ? AND EXISTS (
			SELECT 1 FROM run_recovery_events WHERE run_id = runs.id AND kind = ?
		 )`,
		types.RunCancelled, types.RunCancelReasonSuperseded, ts,
		runID, types.RunRunning, RunRecoveryExactFinalHeadCapacity,
	)
	if err != nil {
		return fmt.Errorf("cancel superseded exact recovery: terminalize run: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("cancel superseded exact recovery: run changed before terminalization")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cancel superseded exact recovery: commit: %w", err)
	}
	return nil
}
