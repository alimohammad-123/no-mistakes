package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	ExactRecoveryPushPrepared = "prepared"
	ExactRecoveryPushInvoked  = "invoked"
	ExactRecoveryPushBound    = "bound"

	ExactRecoveryPushNotApplied = "not_applied"
	ExactRecoveryPushApplied    = "applied"
)

type ExactRecoveryPushOperation struct {
	RunID             string
	OperationID       string
	Attempt           int
	Phase             string
	SourceRef         string
	StaleOID          string
	TargetOID         string
	TargetKind        string
	TargetFingerprint string
	PriorGeneration   int64
	TargetGeneration  int64
	PriorPushedAt     int64
	CreatedAt         int64
	InvokedAt         *int64
	BoundAt           *int64
	UpdatedAt         int64
}

type ExactRecoveryPushOperationEvent struct {
	RunID       string
	Sequence    int
	OperationID string
	Attempt     int
	Phase       string
	OccurredAt  int64
}

type ExactRecoveryPushAttempt struct {
	RunID             string
	Attempt           int
	OperationID       string
	Phase             string
	SourceRef         string
	StaleOID          string
	TargetOID         string
	TargetKind        string
	TargetFingerprint string
	PriorGeneration   int64
	TargetGeneration  int64
	PriorPushedAt     int64
	DeadlineAt        int64
	Disposition       *string
	PreparedAt        int64
	InvokedAt         *int64
	ClosedAt          *int64
}

type ExactRecoveryPushAttemptObservation struct {
	RunID       string
	Attempt     int
	Sequence    int
	OperationID string
	Observation string
	State       string
	ObservedAt  int64
}

func (d *DB) GetExactRecoveryPushOperation(runID string) (*ExactRecoveryPushOperation, error) {
	operation, err := getExactRecoveryPushOperation(d.sql, runID)
	if err != nil || operation == nil {
		return operation, err
	}
	events, err := d.ListExactRecoveryPushOperationEvents(runID)
	if err != nil {
		return nil, err
	}
	if err := validateExactRecoveryPushOperationEvents(operation, events); err != nil {
		return nil, err
	}
	attempts, err := d.ListExactRecoveryPushAttempts(runID)
	if err != nil {
		return nil, err
	}
	observations, err := d.ListExactRecoveryPushAttemptObservations(runID)
	if err != nil {
		return nil, err
	}
	if err := validateExactRecoveryPushAttempts(operation, attempts, observations); err != nil {
		return nil, err
	}
	return operation, nil
}

func getExactRecoveryPushOperation(q queryRower, runID string) (*ExactRecoveryPushOperation, error) {
	var operation ExactRecoveryPushOperation
	err := q.QueryRow(
		`SELECT run_id, operation_id, attempt, phase, source_ref, stale_oid, target_oid,
		        target_kind, target_fingerprint, prior_generation, target_generation,
		        prior_pushed_at, created_at, invoked_at, bound_at, updated_at
		 FROM run_recovery_push_operations WHERE run_id = ?`,
		runID,
	).Scan(
		&operation.RunID, &operation.OperationID, &operation.Attempt, &operation.Phase,
		&operation.SourceRef, &operation.StaleOID, &operation.TargetOID,
		&operation.TargetKind, &operation.TargetFingerprint,
		&operation.PriorGeneration, &operation.TargetGeneration,
		&operation.PriorPushedAt,
		&operation.CreatedAt, &operation.InvokedAt, &operation.BoundAt, &operation.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get exact recovery Push operation: %w", err)
	}
	return &operation, nil
}

func (d *DB) ListExactRecoveryPushOperationEvents(runID string) ([]ExactRecoveryPushOperationEvent, error) {
	rows, err := d.sql.Query(
		`SELECT run_id, sequence, operation_id, attempt, phase, occurred_at
		 FROM run_recovery_push_operation_events WHERE run_id = ? ORDER BY sequence`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list exact recovery Push operation events: %w", err)
	}
	defer rows.Close()
	var events []ExactRecoveryPushOperationEvent
	for rows.Next() {
		var event ExactRecoveryPushOperationEvent
		if err := rows.Scan(
			&event.RunID, &event.Sequence, &event.OperationID,
			&event.Attempt, &event.Phase, &event.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("list exact recovery Push operation events: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list exact recovery Push operation events: %w", err)
	}
	return events, nil
}

func validateExactRecoveryPushOperationEvents(operation *ExactRecoveryPushOperation, events []ExactRecoveryPushOperationEvent) error {
	if operation == nil || len(events) == 0 {
		return fmt.Errorf("exact recovery Push operation event history is incomplete")
	}
	var prior *ExactRecoveryPushOperationEvent
	for index := range events {
		event := &events[index]
		if event.RunID != operation.RunID || event.Sequence != index+1 ||
			event.OperationID == "" || event.Attempt <= 0 {
			return fmt.Errorf("exact recovery Push operation event history is inconsistent")
		}
		if prior == nil {
			if event.Attempt != 1 || event.Phase != ExactRecoveryPushPrepared {
				return fmt.Errorf("exact recovery Push operation history lacks initial preparation")
			}
		} else if event.OperationID == prior.OperationID {
			if event.Attempt != prior.Attempt ||
				(prior.Phase == ExactRecoveryPushPrepared && event.Phase != ExactRecoveryPushInvoked) ||
				(prior.Phase == ExactRecoveryPushInvoked && event.Phase != ExactRecoveryPushBound) ||
				prior.Phase == ExactRecoveryPushBound {
				return fmt.Errorf("exact recovery Push operation phase transition is invalid")
			}
		} else if (prior.Phase != ExactRecoveryPushPrepared && prior.Phase != ExactRecoveryPushInvoked) ||
			event.Attempt != prior.Attempt+1 || event.Phase != ExactRecoveryPushPrepared {
			return fmt.Errorf("exact recovery Push operation attempt transition is invalid")
		}
		prior = event
	}
	last := events[len(events)-1]
	if last.OperationID != operation.OperationID || last.Attempt != operation.Attempt ||
		last.Phase != operation.Phase {
		return fmt.Errorf("exact recovery Push operation event tail is inconsistent")
	}
	return nil
}

func validateExactRecoveryPushAttempts(operation *ExactRecoveryPushOperation, attempts []ExactRecoveryPushAttempt, observations []ExactRecoveryPushAttemptObservation) error {
	if operation == nil || len(attempts) != operation.Attempt || len(observations) < len(attempts) {
		return fmt.Errorf("exact recovery Push attempt history is incomplete")
	}
	observationsByAttempt := make(map[int][]ExactRecoveryPushAttemptObservation, len(attempts))
	for _, observation := range observations {
		observationsByAttempt[observation.Attempt] = append(observationsByAttempt[observation.Attempt], observation)
	}
	for index := range attempts {
		attempt := &attempts[index]
		number := index + 1
		if attempt.RunID != operation.RunID || attempt.Attempt != number ||
			attempt.OperationID == "" || attempt.SourceRef != operation.SourceRef ||
			attempt.StaleOID != operation.StaleOID || attempt.TargetOID != operation.TargetOID ||
			attempt.TargetKind != operation.TargetKind ||
			attempt.TargetFingerprint != operation.TargetFingerprint ||
			attempt.PriorGeneration != operation.PriorGeneration ||
			attempt.TargetGeneration != operation.TargetGeneration ||
			attempt.PriorPushedAt != operation.PriorPushedAt ||
			attempt.DeadlineAt <= attempt.PreparedAt {
			return fmt.Errorf("exact recovery Push attempt identity is inconsistent")
		}
		history := observationsByAttempt[number]
		for observationIndex, observation := range history {
			if observation.RunID != operation.RunID || observation.Attempt != number ||
				observation.Sequence != observationIndex+1 ||
				observation.OperationID != attempt.OperationID ||
				observation.Observation == "" || observation.State == "" {
				return fmt.Errorf("exact recovery Push attempt observation history is inconsistent")
			}
		}
		if len(history) == 0 {
			return fmt.Errorf("exact recovery Push attempt observation history is incomplete")
		}
		if number < operation.Attempt {
			if (attempt.Phase != ExactRecoveryPushPrepared && attempt.Phase != ExactRecoveryPushInvoked) ||
				attempt.Disposition == nil || *attempt.Disposition != ExactRecoveryPushNotApplied ||
				attempt.ClosedAt == nil {
				return fmt.Errorf("exact recovery Push prior attempt is not terminal")
			}
			if (attempt.Phase == ExactRecoveryPushPrepared && attempt.InvokedAt != nil) ||
				(attempt.Phase == ExactRecoveryPushInvoked && attempt.InvokedAt == nil) {
				return fmt.Errorf("exact recovery Push prior attempt invocation is inconsistent")
			}
			continue
		}
		if attempt.OperationID != operation.OperationID || attempt.Phase != operation.Phase ||
			attempt.PreparedAt != operation.CreatedAt {
			return fmt.Errorf("exact recovery Push current attempt is inconsistent")
		}
		switch operation.Phase {
		case ExactRecoveryPushPrepared:
			if attempt.InvokedAt != nil || attempt.Disposition != nil || attempt.ClosedAt != nil {
				return fmt.Errorf("exact recovery Push prepared attempt is terminal")
			}
		case ExactRecoveryPushInvoked:
			if attempt.InvokedAt == nil || attempt.Disposition != nil || attempt.ClosedAt != nil {
				return fmt.Errorf("exact recovery Push invoked attempt is inconsistent")
			}
		case ExactRecoveryPushBound:
			if attempt.InvokedAt == nil || attempt.Disposition == nil ||
				*attempt.Disposition != ExactRecoveryPushApplied || attempt.ClosedAt == nil {
				return fmt.Errorf("exact recovery Push bound attempt is incomplete")
			}
		default:
			return fmt.Errorf("exact recovery Push current attempt phase is invalid")
		}
	}
	return nil
}

func createExactRecoveryPushOperation(tx *sql.Tx, event *RunRecoveryEvent, deadlineAt int64) (*ExactRecoveryPushOperation, error) {
	if event == nil {
		return nil, fmt.Errorf("create exact recovery Push operation: recovery provenance is missing")
	}
	ts := now()
	operation := &ExactRecoveryPushOperation{
		RunID:             event.RunID,
		OperationID:       newID(),
		Attempt:           1,
		Phase:             ExactRecoveryPushPrepared,
		SourceRef:         event.SourceRef,
		StaleOID:          event.LastPushedSHA,
		TargetOID:         event.HeadSHA,
		TargetKind:        event.PushTargetKind,
		TargetFingerprint: event.PushTargetFingerprint,
		PriorGeneration:   event.PushGeneration,
		TargetGeneration:  event.PushGeneration + 1,
		PriorPushedAt:     event.LastPushedAt,
		CreatedAt:         ts,
		UpdatedAt:         ts,
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_push_operations (
			run_id, operation_id, attempt, phase, source_ref, stale_oid, target_oid,
			target_kind, target_fingerprint, prior_generation, target_generation,
			prior_pushed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.RunID, operation.OperationID, operation.Attempt, operation.Phase,
		operation.SourceRef, operation.StaleOID, operation.TargetOID,
		operation.TargetKind, operation.TargetFingerprint,
		operation.PriorGeneration, operation.TargetGeneration,
		operation.PriorPushedAt,
		operation.CreatedAt, operation.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("create exact recovery Push operation: persist: %w", err)
	}
	if err := appendExactRecoveryPushOperationEvent(tx, operation); err != nil {
		return nil, err
	}
	if err := insertExactRecoveryPushAttempt(tx, operation, deadlineAt); err != nil {
		return nil, err
	}
	return operation, nil
}

func insertExactRecoveryPushAttempt(tx *sql.Tx, operation *ExactRecoveryPushOperation, deadlineAt int64) error {
	if operation == nil || deadlineAt <= operation.CreatedAt {
		return fmt.Errorf("create exact recovery Push attempt: deadline is invalid")
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_push_attempts (
			run_id, attempt, operation_id, phase, source_ref, stale_oid, target_oid,
			target_kind, target_fingerprint, prior_generation, target_generation,
			prior_pushed_at, deadline_at, prepared_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.RunID, operation.Attempt, operation.OperationID, operation.Phase,
		operation.SourceRef, operation.StaleOID, operation.TargetOID,
		operation.TargetKind, operation.TargetFingerprint,
		operation.PriorGeneration, operation.TargetGeneration,
		operation.PriorPushedAt, deadlineAt, operation.CreatedAt,
	); err != nil {
		return fmt.Errorf("create exact recovery Push attempt: %w", err)
	}
	return nil
}

func (d *DB) ListExactRecoveryPushAttempts(runID string) ([]ExactRecoveryPushAttempt, error) {
	rows, err := d.sql.Query(
		`SELECT run_id, attempt, operation_id, phase, source_ref, stale_oid, target_oid,
		        target_kind, target_fingerprint, prior_generation, target_generation,
		        prior_pushed_at, deadline_at, disposition, prepared_at, invoked_at, closed_at
		 FROM run_recovery_push_attempts WHERE run_id = ? ORDER BY attempt`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list exact recovery Push attempts: %w", err)
	}
	defer rows.Close()
	var attempts []ExactRecoveryPushAttempt
	for rows.Next() {
		var attempt ExactRecoveryPushAttempt
		if err := rows.Scan(
			&attempt.RunID, &attempt.Attempt, &attempt.OperationID, &attempt.Phase,
			&attempt.SourceRef, &attempt.StaleOID, &attempt.TargetOID,
			&attempt.TargetKind, &attempt.TargetFingerprint,
			&attempt.PriorGeneration, &attempt.TargetGeneration, &attempt.PriorPushedAt,
			&attempt.DeadlineAt, &attempt.Disposition, &attempt.PreparedAt,
			&attempt.InvokedAt, &attempt.ClosedAt,
		); err != nil {
			return nil, fmt.Errorf("list exact recovery Push attempts: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list exact recovery Push attempts: %w", err)
	}
	return attempts, nil
}

func (d *DB) ListExactRecoveryPushAttemptObservations(runID string) ([]ExactRecoveryPushAttemptObservation, error) {
	rows, err := d.sql.Query(
		`SELECT run_id, attempt, sequence, operation_id, observation, state, observed_at
		 FROM run_recovery_push_attempt_observations
		 WHERE run_id = ? ORDER BY attempt, sequence`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list exact recovery Push attempt observations: %w", err)
	}
	defer rows.Close()
	var observations []ExactRecoveryPushAttemptObservation
	for rows.Next() {
		var observation ExactRecoveryPushAttemptObservation
		if err := rows.Scan(
			&observation.RunID, &observation.Attempt, &observation.Sequence,
			&observation.OperationID, &observation.Observation,
			&observation.State, &observation.ObservedAt,
		); err != nil {
			return nil, fmt.Errorf("list exact recovery Push attempt observations: %w", err)
		}
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list exact recovery Push attempt observations: %w", err)
	}
	return observations, nil
}

func appendExactRecoveryPushAttemptObservation(tx *sql.Tx, operation *ExactRecoveryPushOperation, observed, state string) error {
	var sequence int
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(sequence), 0) + 1
		 FROM run_recovery_push_attempt_observations WHERE run_id = ? AND attempt = ?`,
		operation.RunID, operation.Attempt,
	).Scan(&sequence); err != nil {
		return fmt.Errorf("append exact recovery Push attempt observation: sequence: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_push_attempt_observations (
			run_id, attempt, sequence, operation_id, observation, state, observed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		operation.RunID, operation.Attempt, sequence, operation.OperationID,
		observed, state, now(),
	); err != nil {
		return fmt.Errorf("append exact recovery Push attempt observation: %w", err)
	}
	return nil
}

func appendExactRecoveryPushOperationEvent(tx *sql.Tx, operation *ExactRecoveryPushOperation) error {
	var sequence int
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(sequence), 0) + 1
		 FROM run_recovery_push_operation_events WHERE run_id = ?`,
		operation.RunID,
	).Scan(&sequence); err != nil {
		return fmt.Errorf("append exact recovery Push operation event: sequence: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO run_recovery_push_operation_events (
			run_id, sequence, operation_id, attempt, phase, occurred_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		operation.RunID, sequence, operation.OperationID,
		operation.Attempt, operation.Phase, now(),
	); err != nil {
		return fmt.Errorf("append exact recovery Push operation event: %w", err)
	}
	return nil
}

func rotateExactRecoveryPushOperation(tx *sql.Tx, operation *ExactRecoveryPushOperation, observation *ExactRecoveryRefObservation, deadlineAt int64, maxAttempts int) error {
	if operation == nil ||
		(operation.Phase != ExactRecoveryPushPrepared && operation.Phase != ExactRecoveryPushInvoked) {
		return fmt.Errorf("rotate exact recovery Push operation: phase is not retryable")
	}
	ts := now()
	if observation == nil || observation.State != ExactRecoveryRefObservationStale ||
		observation.LastObservation != operation.StaleOID || deadlineAt <= ts {
		return fmt.Errorf("rotate exact recovery Push operation: exact no-change proof or fresh deadline is missing")
	}
	if operation.Phase == ExactRecoveryPushPrepared &&
		(observation.DeadlineAt > ts || operation.InvokedAt != nil) {
		return fmt.Errorf("rotate exact recovery Push operation: prepared attempt is not expired and uninvoked")
	}
	if operation.Phase == ExactRecoveryPushInvoked && operation.InvokedAt == nil {
		return fmt.Errorf("rotate exact recovery Push operation: invocation provenance is missing")
	}
	if operation.Attempt >= maxAttempts {
		return fmt.Errorf("rotate exact recovery Push operation: attempt budget exhausted")
	}
	priorPhase := operation.Phase
	disposition := ExactRecoveryPushNotApplied
	result, err := tx.Exec(
		`UPDATE run_recovery_push_attempts
		 SET disposition = ?, closed_at = ?
		 WHERE run_id = ? AND attempt = ? AND operation_id = ? AND phase = ?
		   AND disposition IS NULL AND closed_at IS NULL
		   AND ((? = ? AND invoked_at IS NULL) OR (? = ? AND invoked_at IS NOT NULL))`,
		disposition, ts, operation.RunID, operation.Attempt, operation.OperationID,
		priorPhase, priorPhase, ExactRecoveryPushPrepared,
		priorPhase, ExactRecoveryPushInvoked,
	)
	if err != nil {
		return fmt.Errorf("rotate exact recovery Push operation: close attempt: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("rotate exact recovery Push operation: immutable attempt changed")
	}
	nextID := newID()
	result, err = tx.Exec(
		`UPDATE run_recovery_push_operations
		 SET operation_id = ?, attempt = attempt + 1, phase = ?,
		     created_at = ?, invoked_at = NULL, bound_at = NULL, updated_at = ?
		 WHERE run_id = ? AND operation_id = ? AND attempt = ? AND phase = ?
		   AND bound_at IS NULL
		   AND ((? = ? AND invoked_at IS NULL) OR (? = ? AND invoked_at IS NOT NULL))`,
		nextID, ExactRecoveryPushPrepared, ts, ts, operation.RunID,
		operation.OperationID, operation.Attempt, priorPhase,
		priorPhase, ExactRecoveryPushPrepared,
		priorPhase, ExactRecoveryPushInvoked,
	)
	if err != nil {
		return fmt.Errorf("rotate exact recovery Push operation: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("rotate exact recovery Push operation: durable invocation changed")
	}
	operation.OperationID = nextID
	operation.Attempt++
	operation.Phase = ExactRecoveryPushPrepared
	operation.CreatedAt = ts
	operation.InvokedAt = nil
	operation.BoundAt = nil
	operation.UpdatedAt = ts
	if err := insertExactRecoveryPushAttempt(tx, operation, deadlineAt); err != nil {
		return err
	}
	result, err = tx.Exec(
		`UPDATE run_recovery_ref_observations
		 SET deadline_at = ?, state = ?, last_observation = ?, updated_at = ?
		 WHERE run_id = ? AND deadline_at = ? AND state = ? AND last_observation = ?`,
		deadlineAt, ExactRecoveryRefObservationStale, operation.StaleOID, ts,
		operation.RunID, observation.DeadlineAt,
		ExactRecoveryRefObservationStale, operation.StaleOID,
	)
	if err != nil {
		return fmt.Errorf("rotate exact recovery Push operation: refresh deadline: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("rotate exact recovery Push operation: observation deadline changed")
	}
	if err := appendExactRecoveryPushOperationEvent(tx, operation); err != nil {
		return err
	}
	return appendExactRecoveryPushAttemptObservation(tx, operation, operation.StaleOID, ExactRecoveryRefObservationStale)
}

func validateExactRecoveryPushOperationIdentity(operation *ExactRecoveryPushOperation, observation *ExactRecoveryRefObservation, event *RunRecoveryEvent) error {
	if operation == nil || observation == nil || event == nil ||
		operation.RunID != event.RunID || operation.OperationID == "" || operation.Attempt <= 0 ||
		operation.SourceRef != event.SourceRef || operation.StaleOID != event.LastPushedSHA ||
		operation.TargetOID != event.HeadSHA || operation.TargetKind != event.PushTargetKind ||
		operation.TargetFingerprint != event.PushTargetFingerprint ||
		operation.PriorGeneration != event.PushGeneration ||
		operation.TargetGeneration != event.PushGeneration+1 ||
		operation.PriorPushedAt != event.LastPushedAt ||
		observation.RunID != operation.RunID || observation.SourceRef != operation.SourceRef ||
		observation.StaleOID != operation.StaleOID || observation.ExpectedOID != operation.TargetOID {
		return fmt.Errorf("exact recovery Push operation identity is inconsistent")
	}
	switch operation.Phase {
	case ExactRecoveryPushPrepared:
		if operation.InvokedAt != nil || operation.BoundAt != nil {
			return fmt.Errorf("exact recovery Push prepared phase has later timestamps")
		}
	case ExactRecoveryPushInvoked:
		if operation.InvokedAt == nil || operation.BoundAt != nil {
			return fmt.Errorf("exact recovery Push invoked phase is incomplete")
		}
	case ExactRecoveryPushBound:
		if operation.InvokedAt == nil || operation.BoundAt == nil {
			return fmt.Errorf("exact recovery Push bound phase is incomplete")
		}
	default:
		return fmt.Errorf("exact recovery Push operation phase is invalid")
	}
	return nil
}

func validateExactRecoveryPushOperationPhase(operation *ExactRecoveryPushOperation, observation *ExactRecoveryRefObservation, run *Run) error {
	if operation == nil || observation == nil || run == nil {
		return fmt.Errorf("exact recovery Push operation phase is incomplete")
	}
	switch operation.Phase {
	case ExactRecoveryPushPrepared, ExactRecoveryPushInvoked:
		if !exactRecoveryRunHasPriorPushBinding(run, operation) ||
			observation.State != ExactRecoveryRefObservationStale {
			return fmt.Errorf("exact recovery Push pre-binding phase is inconsistent")
		}
	case ExactRecoveryPushBound:
		if !exactRecoveryRunHasTargetPushBinding(run, operation) ||
			(observation.State != ExactRecoveryRefObservationStale &&
				observation.State != ExactRecoveryRefObservationExpected) {
			return fmt.Errorf("exact recovery Push bound phase is inconsistent")
		}
	default:
		return fmt.Errorf("exact recovery Push operation phase is invalid")
	}
	return nil
}

func (d *DB) MarkExactRecoveryPushInvoked(runID, operationID, observedRemote string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("mark exact recovery Push invoked: begin: %w", err)
	}
	defer tx.Rollback()
	_, operation, observation, run, err := exactRecoveryPushOperationState(tx, runID)
	if err != nil {
		return err
	}
	if operation.OperationID != strings.TrimSpace(operationID) ||
		operation.Phase != ExactRecoveryPushPrepared || observation.State != ExactRecoveryRefObservationStale ||
		!exactRecoveryRunHasPriorPushBinding(run, operation) {
		return fmt.Errorf("mark exact recovery Push invoked: prepared operation or prior binding changed")
	}
	observedRemote = strings.TrimSpace(observedRemote)
	if observedRemote != operation.StaleOID {
		recordErr := recordExactRecoveryRefObservation(tx, observation, operation, run, observedRemote)
		if commitErr := tx.Commit(); commitErr != nil {
			return fmt.Errorf("%v; commit pre-invocation ambiguity: %w", recordErr, commitErr)
		}
		return fmt.Errorf("mark exact recovery Push invoked: remote is %s, want %s", observedRemote, operation.StaleOID)
	}
	ts := now()
	result, err := tx.Exec(
		`UPDATE run_recovery_push_operations
		 SET phase = ?, invoked_at = ?, updated_at = ?
		 WHERE run_id = ? AND operation_id = ? AND attempt = ? AND phase = ?
		   AND invoked_at IS NULL AND bound_at IS NULL`,
		ExactRecoveryPushInvoked, ts, ts, runID, operation.OperationID,
		operation.Attempt, ExactRecoveryPushPrepared,
	)
	if err != nil {
		return fmt.Errorf("mark exact recovery Push invoked: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("mark exact recovery Push invoked: durable operation changed")
	}
	result, err = tx.Exec(
		`UPDATE run_recovery_push_attempts
		 SET phase = ?, invoked_at = ?
		 WHERE run_id = ? AND attempt = ? AND operation_id = ? AND phase = ?
		   AND disposition IS NULL AND invoked_at IS NULL AND closed_at IS NULL`,
		ExactRecoveryPushInvoked, ts, runID, operation.Attempt,
		operation.OperationID, ExactRecoveryPushPrepared,
	)
	if err != nil {
		return fmt.Errorf("mark exact recovery Push invoked: update attempt: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("mark exact recovery Push invoked: immutable attempt changed")
	}
	operation.Phase = ExactRecoveryPushInvoked
	operation.InvokedAt = &ts
	operation.UpdatedAt = ts
	if err := appendExactRecoveryPushOperationEvent(tx, operation); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark exact recovery Push invoked: commit: %w", err)
	}
	return nil
}

func (d *DB) BindExactRecoveryPushOperation(runID, operationID string, binding PushBinding) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("bind exact recovery Push operation: begin: %w", err)
	}
	defer tx.Rollback()
	event, operation, observation, run, err := exactRecoveryPushOperationState(tx, runID)
	if err != nil {
		return err
	}
	if operation.OperationID != strings.TrimSpace(operationID) ||
		operation.Phase != ExactRecoveryPushInvoked || observation.State != ExactRecoveryRefObservationStale ||
		!exactRecoveryRunHasPriorPushBinding(run, operation) ||
		binding.HeadSHA != operation.TargetOID || binding.Ref != operation.SourceRef ||
		binding.TargetKind != operation.TargetKind ||
		binding.TargetFingerprint != operation.TargetFingerprint {
		return fmt.Errorf("bind exact recovery Push operation: invocation or binding identity changed")
	}
	if err := bindExactRecoveryPushOperation(tx, run, operation, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bind exact recovery Push operation: commit: %w", err)
	}
	return nil
}

func bindExactRecoveryPushOperation(tx *sql.Tx, run *Run, operation *ExactRecoveryPushOperation, event *RunRecoveryEvent) error {
	ts := now()
	result, err := tx.Exec(
		`UPDATE runs
		 SET last_pushed_sha = ?, push_generation = ?, last_pushed_at = ?, updated_at = ?
		 WHERE id = ? AND status = ? AND head_sha = ? AND test_head_sha = ?
		   AND last_pushed_sha = ? AND push_generation = ? AND last_pushed_at = ?
		   AND push_target_kind = ? AND push_target_fingerprint = ? AND push_ref = ?
		   AND COALESCE(push_active, 0) = 1 AND custody_returned_at IS NULL`,
		operation.TargetOID, operation.TargetGeneration, ts, ts,
		run.ID, types.RunRunning, operation.TargetOID, operation.TargetOID,
		operation.StaleOID, operation.PriorGeneration, event.LastPushedAt,
		operation.TargetKind, operation.TargetFingerprint, operation.SourceRef,
	)
	if err != nil {
		return fmt.Errorf("bind exact recovery Push operation: update run: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("bind exact recovery Push operation: run binding changed")
	}
	result, err = tx.Exec(
		`UPDATE run_recovery_push_operations
		 SET phase = ?, bound_at = ?, updated_at = ?
		 WHERE run_id = ? AND operation_id = ? AND attempt = ? AND phase = ?
		   AND invoked_at IS NOT NULL AND bound_at IS NULL`,
		ExactRecoveryPushBound, ts, ts, run.ID, operation.OperationID,
		operation.Attempt, ExactRecoveryPushInvoked,
	)
	if err != nil {
		return fmt.Errorf("bind exact recovery Push operation: update operation: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("bind exact recovery Push operation: operation changed")
	}
	result, err = tx.Exec(
		`UPDATE run_recovery_push_attempts
		 SET phase = ?, disposition = ?, closed_at = ?
		 WHERE run_id = ? AND attempt = ? AND operation_id = ? AND phase = ?
		   AND disposition IS NULL AND invoked_at IS NOT NULL AND closed_at IS NULL`,
		ExactRecoveryPushBound, ExactRecoveryPushApplied, ts, run.ID,
		operation.Attempt, operation.OperationID, ExactRecoveryPushInvoked,
	)
	if err != nil {
		return fmt.Errorf("bind exact recovery Push operation: update attempt: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fmt.Errorf("bind exact recovery Push operation: immutable attempt changed")
	}
	operation.Phase = ExactRecoveryPushBound
	operation.BoundAt = &ts
	operation.UpdatedAt = ts
	return appendExactRecoveryPushOperationEvent(tx, operation)
}

func exactRecoveryPushOperationState(tx *sql.Tx, runID string) (*RunRecoveryEvent, *ExactRecoveryPushOperation, *ExactRecoveryRefObservation, *Run, error) {
	event, err := getRunRecoveryEvent(tx, runID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	operation, err := getExactRecoveryPushOperation(tx, runID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	observation, err := getExactRecoveryRefObservation(tx, runID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var run Run
	if err := scanRun(tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID), &run); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read exact recovery Push operation run: %w", err)
	}
	if err := validateExactRecoveryPushOperationIdentity(operation, observation, event); err != nil {
		return nil, nil, nil, nil, err
	}
	if run.Status != types.RunRunning || !run.PushActive || run.HeadSHA != operation.TargetOID ||
		run.TestHeadSHA == nil || *run.TestHeadSHA != operation.TargetOID ||
		run.ValidationTargetSHA != nil || run.PushTargetKind == nil ||
		*run.PushTargetKind != operation.TargetKind || run.PushTargetFingerprint == nil ||
		*run.PushTargetFingerprint != operation.TargetFingerprint ||
		run.PushRef == nil || *run.PushRef != operation.SourceRef {
		return nil, nil, nil, nil, fmt.Errorf("exact recovery Push operation custody or proof changed")
	}
	if err := validateExactRecoveryPushOperationPhase(operation, observation, &run); err != nil {
		return nil, nil, nil, nil, err
	}
	return event, operation, observation, &run, nil
}

func exactRecoveryRunHasPriorPushBinding(run *Run, operation *ExactRecoveryPushOperation) bool {
	return run != nil && operation != nil && run.LastPushedSHA != nil &&
		*run.LastPushedSHA == operation.StaleOID && run.PushGeneration != nil &&
		*run.PushGeneration == operation.PriorGeneration && run.LastPushedAt != nil &&
		*run.LastPushedAt == operation.PriorPushedAt
}

func exactRecoveryRunHasTargetPushBinding(run *Run, operation *ExactRecoveryPushOperation) bool {
	return run != nil && operation != nil && run.LastPushedSHA != nil &&
		*run.LastPushedSHA == operation.TargetOID && run.PushGeneration != nil &&
		*run.PushGeneration == operation.TargetGeneration && run.LastPushedAt != nil &&
		operation.BoundAt != nil && *run.LastPushedAt == *operation.BoundAt
}
