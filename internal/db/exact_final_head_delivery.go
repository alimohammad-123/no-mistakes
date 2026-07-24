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
	if event == nil || event.DeliveryProtocol != 1 || event.HeadSHA != headSHA || event.PRURL != targetURL {
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
	var pushStatus types.StepStatus
	if err := d.sql.QueryRow(
		`SELECT status FROM step_results WHERE run_id = ? AND step_name = ?`,
		runID, types.StepPush,
	).Scan(&pushStatus); err != nil || pushStatus != types.StepStatusRunning {
		return false, fmt.Errorf("exact recovery Push step is not running")
	}
	return true, nil
}
