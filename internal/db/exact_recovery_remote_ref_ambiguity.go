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
	ObservedAt             int64
}

func validExactRecoveryRemoteRefClassification(classification string) bool {
	switch classification {
	case ExactRecoveryRemoteRefMissing,
		ExactRecoveryRemoteRefDuplicate,
		ExactRecoveryRemoteRefPeeled,
		ExactRecoveryRemoteRefIdentityMismatch,
		ExactRecoveryRemoteRefMalformed:
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
		        observed_push_generation, classification, observed_at
		 FROM run_recovery_remote_ref_ambiguities WHERE run_id = ?`,
		runID,
	).Scan(
		&ambiguity.RunID, &ambiguity.RecoveryEventID, &ambiguity.SourceRef,
		&ambiguity.StaleOID, &ambiguity.TargetOID, &ambiguity.TargetKind,
		&ambiguity.TargetFingerprint, &ambiguity.ObservedLastPushedSHA,
		&ambiguity.ObservedPushGeneration, &ambiguity.Classification,
		&ambiguity.ObservedAt,
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

func recordExactRecoveryRemoteRefAmbiguity(tx *sql.Tx, event *RunRecoveryEvent, run *Run, classification string) error {
	classification = strings.TrimSpace(classification)
	if event == nil || run == nil || !validExactRecoveryRemoteRefClassification(classification) {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: identity or classification is invalid")
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
	result, err := tx.Exec(
		`INSERT INTO run_recovery_remote_ref_ambiguities (
			run_id, recovery_event_id, source_ref, stale_oid, target_oid,
			target_kind, target_fingerprint, observed_last_pushed_sha,
			observed_push_generation, classification, observed_at
		) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		  WHERE EXISTS (
			SELECT 1 FROM runs
			 WHERE id = ? AND status = ? AND error IS NULL AND head_sha = ? AND test_head_sha = ?
			   AND validation_replay_count = ? AND custody_returned_at IS NULL
			   AND last_pushed_sha = ? AND push_generation = ?
			   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
		  )`,
		run.ID, event.ID, event.SourceRef, event.LastPushedSHA, event.HeadSHA,
		event.PushTargetKind, event.PushTargetFingerprint, *run.LastPushedSHA,
		*run.PushGeneration, classification, now(),
		run.ID, types.RunRunning, event.HeadSHA, event.TestHeadSHA,
		event.ReplayCount, *run.LastPushedSHA, *run.PushGeneration,
		event.PushTargetKind, event.PushTargetFingerprint, event.SourceRef,
	)
	if err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: persist: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: durable state changed")
	}
	return nil
}

func (d *DB) RecordExactRecoveryRemoteRefAmbiguity(runID, classification string) error {
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
	if err := recordExactRecoveryRemoteRefAmbiguity(tx, event, &run, classification); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record exact recovery remote-ref ambiguity: commit: %w", err)
	}
	return nil
}
