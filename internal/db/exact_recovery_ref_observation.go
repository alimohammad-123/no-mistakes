package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	ExactRecoveryRefObservationStale     = "stale"
	ExactRecoveryRefObservationExpected  = "expected"
	ExactRecoveryRefObservationAmbiguous = "ambiguous"
)

type ExactRecoveryRefObservation struct {
	RunID           string
	Provider        string
	SourceRef       string
	StaleOID        string
	ExpectedOID     string
	DeadlineAt      int64
	Attempts        int
	State           string
	LastObservation string
	CreatedAt       int64
	UpdatedAt       int64
}

type ExactRecoveryRefObservationEvent struct {
	RunID       string
	Attempt     int
	Observation string
	PriorState  string
	State       string
	ObservedAt  int64
}

func (d *DB) GetExactRecoveryRefObservation(runID string) (*ExactRecoveryRefObservation, error) {
	observation, err := getExactRecoveryRefObservation(d.sql, runID)
	if err != nil || observation == nil {
		return observation, err
	}
	events, err := d.ListExactRecoveryRefObservationEvents(runID)
	if err != nil {
		return nil, err
	}
	if err := validateExactRecoveryRefObservationEvents(observation, events); err != nil {
		return nil, err
	}
	return observation, nil
}

func (d *DB) ListExactRecoveryRefObservationEvents(runID string) ([]ExactRecoveryRefObservationEvent, error) {
	rows, err := d.sql.Query(
		`SELECT run_id, attempt, observation, prior_state, state, observed_at
		 FROM run_recovery_ref_observation_events WHERE run_id = ? ORDER BY attempt`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list exact recovery ref observation events: %w", err)
	}
	defer rows.Close()
	var events []ExactRecoveryRefObservationEvent
	for rows.Next() {
		var event ExactRecoveryRefObservationEvent
		if err := rows.Scan(
			&event.RunID, &event.Attempt, &event.Observation,
			&event.PriorState, &event.State, &event.ObservedAt,
		); err != nil {
			return nil, fmt.Errorf("list exact recovery ref observation events: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list exact recovery ref observation events: %w", err)
	}
	return events, nil
}

func validateExactRecoveryRefObservationEvents(observation *ExactRecoveryRefObservation, events []ExactRecoveryRefObservationEvent) error {
	if observation == nil || len(events) != observation.Attempts || len(events) == 0 {
		return fmt.Errorf("exact recovery ref observation event history is incomplete")
	}
	priorState := ""
	for index, event := range events {
		if event.RunID != observation.RunID || event.Attempt != index+1 ||
			event.PriorState != priorState || event.Observation == "" {
			return fmt.Errorf("exact recovery ref observation event history is inconsistent")
		}
		valid := false
		switch priorState {
		case "":
			valid = event.State == ExactRecoveryRefObservationStale ||
				event.State == ExactRecoveryRefObservationAmbiguous
		case ExactRecoveryRefObservationStale:
			valid = event.State == ExactRecoveryRefObservationStale ||
				event.State == ExactRecoveryRefObservationExpected ||
				event.State == ExactRecoveryRefObservationAmbiguous
		case ExactRecoveryRefObservationExpected:
			valid = event.State == ExactRecoveryRefObservationExpected ||
				event.State == ExactRecoveryRefObservationAmbiguous
		case ExactRecoveryRefObservationAmbiguous:
			valid = event.State == ExactRecoveryRefObservationAmbiguous
		}
		if !valid {
			return fmt.Errorf("exact recovery ref observation event transition is invalid")
		}
		priorState = event.State
	}
	last := events[len(events)-1]
	if last.State != observation.State || last.Observation != observation.LastObservation {
		return fmt.Errorf("exact recovery ref observation event tail is inconsistent")
	}
	return nil
}

func getExactRecoveryRefObservation(q queryRower, runID string) (*ExactRecoveryRefObservation, error) {
	var observation ExactRecoveryRefObservation
	err := q.QueryRow(
		`SELECT run_id, provider, source_ref, stale_oid, expected_oid, deadline_at,
		        attempts, state, last_observation, created_at, updated_at
		 FROM run_recovery_ref_observations WHERE run_id = ?`,
		runID,
	).Scan(
		&observation.RunID, &observation.Provider, &observation.SourceRef,
		&observation.StaleOID, &observation.ExpectedOID, &observation.DeadlineAt,
		&observation.Attempts, &observation.State, &observation.LastObservation,
		&observation.CreatedAt, &observation.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get exact recovery ref observation: %w", err)
	}
	return &observation, nil
}

func (d *DB) PrepareExactRecoveryRefObservation(runID, provider, sourceRef, expectedOID, observedOID string, deadlineAt int64) (*ExactRecoveryRefObservation, error) {
	runID = strings.TrimSpace(runID)
	provider = strings.TrimSpace(provider)
	sourceRef = strings.TrimSpace(sourceRef)
	expectedOID = strings.TrimSpace(expectedOID)
	observedOID = strings.TrimSpace(observedOID)
	if runID == "" || provider == "" || sourceRef == "" || expectedOID == "" || observedOID == "" || deadlineAt <= now() {
		return nil, fmt.Errorf("prepare exact recovery ref observation: identity or deadline is incomplete")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("prepare exact recovery ref observation: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return nil, err
	}
	if event == nil || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol ||
		event.HeadSHA != expectedOID || event.SourceRef != sourceRef {
		return nil, fmt.Errorf("prepare exact recovery ref observation: recovery identity is inconsistent")
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return nil, fmt.Errorf("prepare exact recovery ref observation: read run: %w", err)
	}
	if run.Status != types.RunRunning || run.HeadSHA != expectedOID ||
		run.TestHeadSHA == nil || *run.TestHeadSHA != expectedOID ||
		run.ValidationTargetSHA != nil || !run.PushActive ||
		run.LastPushedSHA == nil || *run.LastPushedSHA != event.LastPushedSHA ||
		run.PushGeneration == nil || *run.PushGeneration != event.PushGeneration {
		return nil, fmt.Errorf("prepare exact recovery ref observation: delivery proof is inconsistent")
	}
	var pushStatus types.StepStatus
	if err := tx.QueryRow(
		`SELECT status FROM step_results WHERE run_id = ? AND step_name = ?`,
		runID, types.StepPush,
	).Scan(&pushStatus); err != nil || pushStatus != types.StepStatusRunning {
		return nil, fmt.Errorf("prepare exact recovery ref observation: Push step is not running")
	}
	existing, err := getExactRecoveryRefObservation(tx, runID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		operation, err := getExactRecoveryPushOperation(tx, runID)
		if err != nil {
			return nil, err
		}
		if err := validateExactRecoveryPushOperationIdentity(operation, existing, event); err != nil {
			return nil, err
		}
		if existing.Provider != provider || existing.SourceRef != sourceRef ||
			existing.StaleOID != event.LastPushedSHA || existing.ExpectedOID != expectedOID {
			return nil, fmt.Errorf("prepare exact recovery ref observation: durable identity changed")
		}
		if observedOID == existing.StaleOID && existing.DeadlineAt <= now() {
			err := recordExactRecoveryRefObservation(tx, existing, operation, &run, "timeout")
			if commitErr := tx.Commit(); commitErr != nil {
				return nil, fmt.Errorf("%v; commit ambiguity: %w", err, commitErr)
			}
			observation, getErr := d.GetExactRecoveryRefObservation(runID)
			if getErr != nil {
				return nil, getErr
			}
			return observation, fmt.Errorf("prepare exact recovery ref observation: visibility deadline expired")
		}
		if err := recordExactRecoveryRefObservation(tx, existing, operation, &run, observedOID); err != nil {
			if commitErr := tx.Commit(); commitErr != nil {
				return nil, fmt.Errorf("%v; commit ambiguity: %w", err, commitErr)
			}
			observation, getErr := d.GetExactRecoveryRefObservation(runID)
			if getErr != nil {
				return nil, getErr
			}
			return observation, err
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("prepare exact recovery ref observation: commit: %w", err)
		}
		return d.GetExactRecoveryRefObservation(runID)
	}
	state := ExactRecoveryRefObservationStale
	if observedOID != event.LastPushedSHA {
		state = ExactRecoveryRefObservationAmbiguous
	}
	ts := now()
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_ref_observations (
			run_id, provider, source_ref, stale_oid, expected_oid, deadline_at,
			attempts, state, last_observation, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)`,
		runID, provider, sourceRef, event.LastPushedSHA, expectedOID, deadlineAt,
		state, observedOID, ts, ts,
	); err != nil {
		return nil, fmt.Errorf("prepare exact recovery ref observation: persist: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_ref_observation_events (
			run_id, attempt, observation, prior_state, state, observed_at
		) VALUES (?, 1, ?, '', ?, ?)`,
		runID, observedOID, state, ts,
	); err != nil {
		return nil, fmt.Errorf("prepare exact recovery ref observation: persist event: %w", err)
	}
	operation, err := createExactRecoveryPushOperation(tx, event, deadlineAt)
	if err != nil {
		return nil, err
	}
	if err := appendExactRecoveryPushAttemptObservation(tx, operation, observedOID, state); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("prepare exact recovery ref observation: commit: %w", err)
	}
	observation, getErr := d.GetExactRecoveryRefObservation(runID)
	if getErr != nil {
		return nil, getErr
	}
	if state == ExactRecoveryRefObservationAmbiguous {
		return observation, fmt.Errorf("prepare exact recovery ref observation: source ref is %s, want sole stale OID %s", observedOID, event.LastPushedSHA)
	}
	return observation, nil
}

func (d *DB) RecordExactRecoveryRefObservation(runID, observed string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("record exact recovery ref observation: begin: %w", err)
	}
	defer tx.Rollback()
	observation, err := getExactRecoveryRefObservation(tx, runID)
	if err != nil {
		return err
	}
	if observation == nil {
		return fmt.Errorf("record exact recovery ref observation: durable journal is missing")
	}
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	operation, err := getExactRecoveryPushOperation(tx, runID)
	if err != nil {
		return err
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return fmt.Errorf("record exact recovery ref observation: read run: %w", err)
	}
	if err := validateExactRecoveryPushOperationIdentity(operation, observation, event); err != nil {
		return err
	}
	observed = strings.TrimSpace(observed)
	if observed == observation.StaleOID && observation.DeadlineAt <= now() {
		observed = "timeout"
	}
	recordErr := recordExactRecoveryRefObservation(tx, observation, operation, &run, observed)
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record exact recovery ref observation: commit: %w", err)
	}
	return recordErr
}

func (d *DB) RecordExactRecoveryPendingPushObservation(runID, observed string) (bool, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return false, fmt.Errorf("record exact recovery pending Push observation: begin: %w", err)
	}
	defer tx.Rollback()
	_, operation, observation, run, err := exactRecoveryPushOperationState(tx, runID)
	if err != nil {
		return false, err
	}
	observed = strings.TrimSpace(observed)
	if operation.Phase != ExactRecoveryPushInvoked ||
		observation.State != ExactRecoveryRefObservationStale ||
		observed != operation.StaleOID ||
		!exactRecoveryRunHasPriorPushBinding(run, operation) {
		return false, fmt.Errorf("record exact recovery pending Push observation: invocation identity changed")
	}
	if observation.DeadlineAt <= now() {
		return false, nil
	}
	if err := recordExactRecoveryRefObservation(tx, observation, operation, run, observed); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("record exact recovery pending Push observation: commit: %w", err)
	}
	return true, nil
}

func recordExactRecoveryRefObservation(tx *sql.Tx, observation *ExactRecoveryRefObservation, operation *ExactRecoveryPushOperation, run *Run, observed string) error {
	state := observation.State
	if observation.State != ExactRecoveryRefObservationAmbiguous {
		priorBinding := exactRecoveryRunHasPriorPushBinding(run, operation)
		targetBinding := exactRecoveryRunHasTargetPushBinding(run, operation)
		switch {
		case operation.Phase == ExactRecoveryPushPrepared && !priorBinding:
			state = ExactRecoveryRefObservationAmbiguous
		case operation.Phase == ExactRecoveryPushInvoked && !priorBinding:
			state = ExactRecoveryRefObservationAmbiguous
		case operation.Phase == ExactRecoveryPushBound && !targetBinding:
			state = ExactRecoveryRefObservationAmbiguous
		case observed == observation.ExpectedOID:
			if operation.Phase == ExactRecoveryPushBound && targetBinding {
				state = ExactRecoveryRefObservationExpected
			} else {
				state = ExactRecoveryRefObservationAmbiguous
			}
		case observed == observation.StaleOID:
			if observation.State == ExactRecoveryRefObservationExpected {
				state = ExactRecoveryRefObservationAmbiguous
			} else {
				state = ExactRecoveryRefObservationStale
			}
		default:
			state = ExactRecoveryRefObservationAmbiguous
		}
	}
	result, err := tx.Exec(
		`UPDATE run_recovery_ref_observations
		 SET attempts = attempts + 1, state = ?, last_observation = ?, updated_at = ?
		 WHERE run_id = ? AND attempts = ? AND state = ?`,
		state, observed, now(), observation.RunID, observation.Attempts, observation.State,
	)
	if err != nil {
		return fmt.Errorf("record exact recovery ref observation: update: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("record exact recovery ref observation: durable state changed")
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_ref_observation_events (
			run_id, attempt, observation, prior_state, state, observed_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		observation.RunID, observation.Attempts+1, observed, observation.State, state, now(),
	); err != nil {
		return fmt.Errorf("record exact recovery ref observation event: %w", err)
	}
	if err := appendExactRecoveryPushAttemptObservation(tx, operation, observed, state); err != nil {
		return err
	}
	if state == ExactRecoveryRefObservationAmbiguous {
		return fmt.Errorf("exact recovery ref observation %q is ambiguous after %s", observed, observation.State)
	}
	return nil
}
