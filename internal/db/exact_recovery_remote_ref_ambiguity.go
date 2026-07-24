package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	ExactRecoveryRemoteRefMissing          = "missing"
	ExactRecoveryRemoteRefDuplicate        = "duplicate"
	ExactRecoveryRemoteRefPeeled           = "peeled"
	ExactRecoveryRemoteRefIdentityMismatch = "identity-mismatch"
	ExactRecoveryRemoteRefMalformed        = "malformed"
	ExactRecoveryRemoteRefUnexpectedOID    = "unexpected-oid"
)

type ExactRecoveryRemoteRefAmbiguity struct {
	RunID                  string
	RecoveryEventID        string
	SourceRef              string
	StaleOID               string
	TargetOID              string
	TargetKind             string
	TargetFingerprint      string
	ObservedLastPushedSHA  string
	ObservedPushGeneration int64
	Classification         string
	ObservedOID            string
	ObservedPushActive     bool
	ObservedPushStepStatus types.StepStatus
	ObservedOperationID    string
	ObservedOperationPhase string
	ObservedAt             int64
}

func validExactRecoveryRemoteRefClassification(classification string) bool {
	switch classification {
	case ExactRecoveryRemoteRefMissing,
		ExactRecoveryRemoteRefDuplicate,
		ExactRecoveryRemoteRefPeeled,
		ExactRecoveryRemoteRefIdentityMismatch,
		ExactRecoveryRemoteRefMalformed,
		ExactRecoveryRemoteRefUnexpectedOID:
		return true
	default:
		return false
	}
}

func getExactRecoveryRemoteRefAmbiguity(q queryRower, runID string) (*ExactRecoveryRemoteRefAmbiguity, error) {
	var ambiguity ExactRecoveryRemoteRefAmbiguity
	err := q.QueryRow(
		`SELECT run_id, recovery_event_id, source_ref, stale_oid, target_oid,
		        target_kind, target_fingerprint, observed_last_pushed_sha,
		        observed_push_generation, classification, observed_oid,
		        observed_push_active, observed_push_step_status,
		        observed_operation_id, observed_operation_phase, observed_at
		 FROM run_recovery_remote_ref_ambiguities WHERE run_id = ?`,
		runID,
	).Scan(
		&ambiguity.RunID, &ambiguity.RecoveryEventID, &ambiguity.SourceRef,
		&ambiguity.StaleOID, &ambiguity.TargetOID, &ambiguity.TargetKind,
		&ambiguity.TargetFingerprint, &ambiguity.ObservedLastPushedSHA,
		&ambiguity.ObservedPushGeneration, &ambiguity.Classification,
		&ambiguity.ObservedOID, &ambiguity.ObservedPushActive,
		&ambiguity.ObservedPushStepStatus, &ambiguity.ObservedOperationID,
		&ambiguity.ObservedOperationPhase, &ambiguity.ObservedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get exact recovery remote-ref ambiguity: %w", err)
	}
	return &ambiguity, nil
}

func (d *DB) GetExactRecoveryRemoteRefAmbiguity(runID string) (*ExactRecoveryRemoteRefAmbiguity, error) {
	return getExactRecoveryRemoteRefAmbiguity(d.sql, runID)
}

func ensureNoExactRecoveryRemoteRefAmbiguity(q queryRower, runID string) error {
	ambiguity, err := getExactRecoveryRemoteRefAmbiguity(q, runID)
	if err != nil {
		return err
	}
	if ambiguity != nil {
		return fmt.Errorf("exact recovery remote ref is terminally ambiguous: %s", ambiguity.Classification)
	}
	return nil
}

func (d *DB) CheckExactRecoveryRemoteRefAmbiguity(runID string) error {
	return ensureNoExactRecoveryRemoteRefAmbiguity(d.sql, runID)
}

func validExactRecoveryObservedOID(observedOID, expectedOID string) bool {
	if len(observedOID) != 40 && len(observedOID) != 64 {
		return false
	}
	if (len(expectedOID) == 40 || len(expectedOID) == 64) && len(observedOID) != len(expectedOID) {
		return false
	}
	for _, char := range observedOID {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func recordExactRecoveryRemoteRefAmbiguity(tx *sql.Tx, event *RunRecoveryEvent, run *Run, sourceRef, classification, observedOID string) error {
	sourceRef = strings.TrimSpace(sourceRef)
	classification = strings.TrimSpace(classification)
	observedOID = strings.TrimSpace(observedOID)
	if event == nil || run == nil || sourceRef != event.SourceRef ||
		!validExactRecoveryRemoteRefClassification(classification) {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: identity or classification is invalid")
	}
	if classification == ExactRecoveryRemoteRefUnexpectedOID {
		if !validExactRecoveryObservedOID(observedOID, event.HeadSHA) {
			return fmt.Errorf("record exact recovery remote-ref ambiguity: unexpected OID is invalid")
		}
	} else if observedOID != "" {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: structural observation includes an OID")
	}
	existing, err := getExactRecoveryRemoteRefAmbiguity(tx, run.ID)
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("exact recovery remote ref is terminally ambiguous: %s", existing.Classification)
	}
	if event.DeliveryProtocol != ExactRecoveryDeliveryProtocol ||
		run.Status != types.RunRunning || run.Error != nil || run.CustodyReturnedAt != nil ||
		run.HeadSHA != event.HeadSHA || run.TestHeadSHA == nil || *run.TestHeadSHA != event.TestHeadSHA ||
		run.ValidationReplayCount != event.ReplayCount || run.PRURL == nil || *run.PRURL != event.PRURL ||
		run.LastPushedSHA == nil || run.PushGeneration == nil || run.LastPushedAt == nil ||
		run.PushTargetKind == nil || *run.PushTargetKind != event.PushTargetKind ||
		run.PushTargetFingerprint == nil || *run.PushTargetFingerprint != event.PushTargetFingerprint ||
		run.PushRef == nil || *run.PushRef != event.SourceRef {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: run or binding proof changed")
	}
	priorBinding := *run.LastPushedSHA == event.LastPushedSHA &&
		*run.PushGeneration == event.PushGeneration && *run.LastPushedAt == event.LastPushedAt
	targetBinding := *run.LastPushedSHA == event.HeadSHA &&
		*run.PushGeneration == event.PushGeneration+1 && *run.LastPushedAt >= event.LastPushedAt
	if !priorBinding && !targetBinding {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: delivery binding changed")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil || ref != event.SourceRef {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: source ref changed")
	}
	var latestRunID string
	if err := tx.QueryRow(
		`SELECT id FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC LIMIT 1`,
		run.RepoID, run.Branch,
	).Scan(&latestRunID); err != nil || latestRunID != run.ID {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: run is superseded")
	}
	var pendingTransition bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM run_head_transitions WHERE run_id = ?)`, run.ID).Scan(&pendingTransition); err != nil || pendingTransition {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: head transition is pending")
	}
	var pushStatus types.StepStatus
	if err := tx.QueryRow(
		`SELECT status FROM step_results WHERE run_id = ? AND step_name = ?`,
		run.ID, types.StepPush,
	).Scan(&pushStatus); err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: Push phase is missing")
	}
	operation, err := getExactRecoveryPushOperation(tx, run.ID)
	if err != nil {
		return err
	}
	operationID := ""
	operationPhase := ""
	if operation != nil {
		if operation.SourceRef != event.SourceRef ||
			operation.StaleOID != event.LastPushedSHA || operation.TargetOID != event.HeadSHA ||
			operation.TargetKind != event.PushTargetKind ||
			operation.TargetFingerprint != event.PushTargetFingerprint ||
			operation.PriorGeneration != event.PushGeneration ||
			operation.TargetGeneration != event.PushGeneration+1 ||
			operation.PriorPushedAt != event.LastPushedAt {
			return fmt.Errorf("record exact recovery remote-ref ambiguity: Push operation identity changed")
		}
		operationID = operation.OperationID
		operationPhase = operation.Phase
	}
	operationCAS := `NOT EXISTS (SELECT 1 FROM run_recovery_push_operations WHERE run_id = ?)`
	operationArgs := []any{run.ID}
	if operation != nil {
		operationCAS = `EXISTS (
			SELECT 1 FROM run_recovery_push_operations
			 WHERE run_id = ? AND operation_id = ? AND phase = ?
			   AND source_ref = ? AND stale_oid = ? AND target_oid = ?
			   AND target_kind = ? AND target_fingerprint = ?
			   AND prior_generation = ? AND target_generation = ? AND prior_pushed_at = ?
		)`
		operationArgs = []any{
			run.ID, operation.OperationID, operation.Phase, operation.SourceRef,
			operation.StaleOID, operation.TargetOID, operation.TargetKind,
			operation.TargetFingerprint, operation.PriorGeneration,
			operation.TargetGeneration, operation.PriorPushedAt,
		}
	}
	query := `INSERT INTO run_recovery_remote_ref_ambiguities (
			run_id, recovery_event_id, source_ref, stale_oid, target_oid,
			target_kind, target_fingerprint, observed_last_pushed_sha,
			observed_push_generation, classification, observed_oid,
			observed_push_active, observed_push_step_status,
			observed_operation_id, observed_operation_phase, observed_at
		) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		  WHERE EXISTS (
			SELECT 1 FROM runs
			 WHERE id = ? AND status = ? AND error IS NULL AND head_sha = ? AND test_head_sha = ?
			   AND validation_replay_count = ? AND custody_returned_at IS NULL
			   AND last_pushed_sha = ? AND push_generation = ?
			   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
			   AND COALESCE(push_active, 0) = ?
		  )
		    AND EXISTS (
			SELECT 1 FROM step_results
			 WHERE run_id = ? AND step_name = ? AND status = ?
		    )
		    AND ` + operationCAS
	args := []any{
		run.ID, event.ID, event.SourceRef, event.LastPushedSHA, event.HeadSHA,
		event.PushTargetKind, event.PushTargetFingerprint, *run.LastPushedSHA,
		*run.PushGeneration, classification, observedOID, run.PushActive,
		pushStatus, operationID, operationPhase, now(),
		run.ID, types.RunRunning, event.HeadSHA, event.TestHeadSHA,
		event.ReplayCount, *run.LastPushedSHA, *run.PushGeneration,
		event.PushTargetKind, event.PushTargetFingerprint, event.SourceRef,
		run.PushActive, run.ID, types.StepPush, pushStatus,
	}
	args = append(args, operationArgs...)
	result, err := tx.Exec(
		query,
		args...,
	)
	if err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: persist: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: durable state changed")
	}
	return nil
}

func (d *DB) recordExactRecoveryRemoteRefAmbiguity(runID, sourceRef, classification, observedOID string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: recovery provenance is missing")
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: read run: %w", err)
	}
	if err := recordExactRecoveryRemoteRefAmbiguity(tx, event, &run, sourceRef, classification, observedOID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: commit: %w", err)
	}
	return nil
}

func (d *DB) RecordExactRecoveryRemoteRefAmbiguity(runID, classification string) error {
	event, err := d.GetRunRecoveryEvent(runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: recovery provenance is missing")
	}
	return d.recordExactRecoveryRemoteRefAmbiguity(runID, event.SourceRef, classification, "")
}

func (d *DB) RecordExactRecoveryUnexpectedRemoteOID(runID, sourceRef, observedOID string) error {
	return d.recordExactRecoveryRemoteRefAmbiguity(
		runID, sourceRef, ExactRecoveryRemoteRefUnexpectedOID, observedOID,
	)
}
