package db

import (
	"fmt"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

type exactFinalHeadRecoveryFixture struct {
	t        *testing.T
	d        *DB
	run      *Run
	document *StepResult
	head     string
	pushed   string
}

func newExactFinalHeadRecoveryFixture(t *testing.T) *exactFinalHeadRecoveryFixture {
	t.Helper()
	d := openTestDB(t)
	repo, err := d.InsertRepo("/home/user/exact-final-head-recovery", "git@github.com:user/project.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	for index, head := range []string{"head-1", "head-2", "head-3"} {
		if index > 0 {
			if err := d.UpdateRunHeadSHA(run.ID, head); err != nil {
				t.Fatal(err)
			}
		}
		count, err := d.ScheduleHeadValidationReplay(run.ID, 3)
		if err != nil || count != index+1 {
			t.Fatalf("schedule target %d: count=%d err=%v", index+1, count, err)
		}
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "head-3"); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPRURL(run.ID, "https://github.com/user/project/pull/42"); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPushBinding(run.ID, PushBinding{
		HeadSHA:           "published-head",
		TargetKind:        "upstream",
		TargetFingerprint: "target-fingerprint",
		Ref:               "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}

	var document *StepResult
	for _, name := range types.AllSteps() {
		step, err := d.InsertStepResult(run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case name.Order() < types.StepTest.Order():
			if err := d.CompleteStep(step.ID, 0, 10, string(name)+".log"); err != nil {
				t.Fatal(err)
			}
		case name == types.StepTest:
			if err := d.CompleteStep(step.ID, 0, 10, "test.log"); err != nil {
				t.Fatal(err)
			}
		case name == types.StepDocument:
			document = step
			if err := d.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			if err := d.FailStep(step.ID, ExactFinalHeadCapacityStepError(3), 50); err != nil {
				t.Fatal(err)
			}
		}
	}
	if document == nil {
		t.Fatal("Document step missing")
	}
	if err := d.UpdateRunErrorStatus(run.ID, ExactFinalHeadCapacityRunError(3), types.RunFailed); err != nil {
		t.Fatal(err)
	}
	run, err = d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return &exactFinalHeadRecoveryFixture{t: t, d: d, run: run, document: document, head: "head-3", pushed: "published-head"}
}

func (f *exactFinalHeadRecoveryFixture) inspect() *ExactFinalHeadCapacityFailure {
	f.t.Helper()
	failure, err := f.d.InspectExactFinalHeadCapacityFailure(f.run.ID, 3, types.AllSteps())
	if err != nil {
		f.t.Fatal(err)
	}
	return failure
}

func TestRestoreExactFinalHeadCapacityFailureAppendsProvenanceAndPreservesIdentity(t *testing.T) {
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	beforeRounds, err := f.d.GetRoundsByStep(f.document.ID)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps())
	if err != nil {
		t.Fatalf("restore exact capacity failure: %v", err)
	}
	if restored.ID != f.run.ID || restored.Status != types.RunRunning || restored.Error != nil {
		t.Fatalf("restored run identity/status = %#v", restored)
	}
	if restored.HeadSHA != f.head || restored.TestHeadSHA == nil || *restored.TestHeadSHA != f.head ||
		restored.ValidationTargetSHA == nil || *restored.ValidationTargetSHA != f.head || restored.ValidationReplayCount != 3 {
		t.Fatalf("restored exact proof changed = %#v", restored)
	}
	if restored.PRURL == nil || *restored.PRURL != "https://github.com/user/project/pull/42" ||
		restored.LastPushedSHA == nil || *restored.LastPushedSHA != f.pushed || restored.PushActive {
		t.Fatalf("restored delivery identity changed = %#v", restored)
	}
	steps, err := f.d.GetStepsByRun(f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		switch {
		case step.StepName.Order() <= types.StepTest.Order():
			if step.Status != types.StepStatusCompleted {
				t.Fatalf("predecessor %s = %s", step.StepName, step.Status)
			}
		case step.StepName.Order() >= types.StepDocument.Order():
			if step.Status != types.StepStatusPending {
				t.Fatalf("recovery suffix %s = %s", step.StepName, step.Status)
			}
		}
	}
	afterRounds, err := f.d.GetRoundsByStep(f.document.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterRounds) != len(beforeRounds) {
		t.Fatalf("recovery rewrote round history: %d -> %d", len(beforeRounds), len(afterRounds))
	}
	event, err := f.d.GetRunRecoveryEvent(f.run.ID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if event == nil || event.RunID != f.run.ID || event.HeadSHA != f.head || event.PriorStatus != types.RunFailed ||
		event.PriorError != ExactFinalHeadCapacityRunError(3) || event.PRURL != "https://github.com/user/project/pull/42" ||
		event.LastPushedSHA != f.pushed || event.DeliveryProtocol != ExactRecoveryDeliveryProtocol || event.AnchorRef == "" {
		t.Fatalf("recovery provenance = %#v", event)
	}
	if _, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps()); err == nil {
		t.Fatal("repeated recovery rewrote the already recovered run")
	}
}

func TestInspectExactFinalHeadCapacityFailureRejectsInconsistentHistory(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*exactFinalHeadRecoveryFixture)
	}{
		{name: "different run failure", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET error = 'other failure' WHERE id = ?`, f.run.ID)
		}},
		{name: "non-exact Test proof", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET test_head_sha = 'other' WHERE id = ?`, f.run.ID)
		}},
		{name: "non-exact validation target", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET validation_target_sha = 'other' WHERE id = ?`, f.run.ID)
		}},
		{name: "replay below capacity", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET validation_replay_count = 2 WHERE id = ?`, f.run.ID)
		}},
		{name: "missing source ref", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET source_ref = NULL WHERE id = ?`, f.run.ID)
		}},
		{name: "pending transition", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `INSERT INTO run_head_transitions (
				run_id, source_ref, previous_sha, candidate_sha, require_validation, phase,
				expected_push_active, prior_target_sha, next_target_sha, prior_replay_count,
				next_replay_count, ownership_generation, created_at
			) VALUES (?, 'refs/heads/feature', 'head-3', 'head-4', 1, 'pipeline', 0,
			          'head-3', 'head-4', 3, 4, 1, 1)`, f.run.ID)
		}},
		{name: "push active", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET push_active = 1 WHERE id = ?`, f.run.ID)
		}},
		{name: "missing PR identity", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET pr_url = NULL WHERE id = ?`, f.run.ID)
		}},
		{name: "already published final head", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET last_pushed_sha = head_sha WHERE id = ?`, f.run.ID)
		}},
		{name: "Test not complete", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE step_results SET status = 'failed' WHERE run_id = ? AND step_name = 'test'`, f.run.ID)
		}},
		{name: "Document has unrelated failure", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE step_results SET error = 'other failure' WHERE id = ?`, f.document.ID)
		}},
		{name: "Document round after failed attempt began", mutate: func(f *exactFinalHeadRecoveryFixture) {
			if _, err := f.d.InsertStepRound(f.document.ID, 1, "head_revalidation", nil, nil, 1); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "delivery successor completed", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE step_results SET status = 'completed' WHERE run_id = ? AND step_name = 'push'`, f.run.ID)
		}},
		{name: "historical completed status", mutate: func(f *exactFinalHeadRecoveryFixture) {
			mustExecRecoveryTest(t, f.d, `UPDATE runs SET status = 'completed' WHERE id = ?`, f.run.ID)
		}},
		{name: "newer terminal branch run", mutate: func(f *exactFinalHeadRecoveryFixture) {
			newer, err := f.d.InsertRun(f.run.RepoID, f.run.Branch, "newer-head", "base")
			if err != nil {
				t.Fatal(err)
			}
			if err := f.d.UpdateRunStatus(newer.ID, types.RunCompleted); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newExactFinalHeadRecoveryFixture(t)
			tc.mutate(f)
			if _, err := f.d.InspectExactFinalHeadCapacityFailure(f.run.ID, 3, types.AllSteps()); err == nil {
				t.Fatal("inconsistent terminal history was accepted")
			}
			got, err := f.d.GetRun(f.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status == types.RunRunning {
				t.Fatal("read-only inspection revived the run")
			}
		})
	}
}

func TestRestoreExactFinalHeadCapacityFailureUsesEvidenceTokenCAS(t *testing.T) {
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	mustExecRecoveryTest(t, f.d, `UPDATE runs SET pr_url = 'https://github.com/user/project/pull/99' WHERE id = ?`, f.run.ID)
	if _, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps()); err == nil {
		t.Fatal("recovery claimed changed durable evidence")
	}
	got, err := f.d.GetRun(f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunFailed || got.PRURL == nil || *got.PRURL != "https://github.com/user/project/pull/99" {
		t.Fatalf("failed CAS changed terminal run: %#v", got)
	}
	event, err := f.d.GetRunRecoveryEvent(f.run.ID, RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if event != nil {
		t.Fatalf("failed CAS appended provenance: %#v", event)
	}
}

func TestValidateActiveExactFinalHeadCapacityRecoverySpansProofClosureAndPush(t *testing.T) {
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	restored, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	steps, err := f.d.GetStepsByRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	var documentID, lintID, pushID string
	for _, step := range steps {
		switch step.StepName {
		case types.StepDocument:
			documentID = step.ID
		case types.StepLint:
			lintID = step.ID
		case types.StepPush:
			pushID = step.ID
		}
	}
	for _, id := range []string{documentID, lintID} {
		if err := f.d.CompleteStep(id, 0, 1, "step.log"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.d.CompleteHeadValidation(restored.ID, restored.HeadSHA); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate after exact proof closure: %v", err)
	}
	if err := f.d.StartStep(pushID); err != nil {
		t.Fatal(err)
	}
	if err := f.d.UpdateRunPushBinding(restored.ID, PushBinding{
		HeadSHA: restored.HeadSHA, TargetKind: "upstream", TargetFingerprint: "target-fingerprint", Ref: "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate exact binding before Push completion: %v", err)
	}
	if bound, err := f.d.ExactRecoveryPushAlreadyBound(restored.ID, restored.HeadSHA); err != nil || !bound {
		t.Fatalf("exact recovery Push binding = %v, %v", bound, err)
	}
	if err := f.d.CompleteStep(pushID, 0, 1, "push.log"); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate after exact push: %v", err)
	}
}

func prepareExactRecoveryPushCrash(t *testing.T) (*exactFinalHeadRecoveryFixture, *Run, string) {
	t.Helper()
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	restored, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	steps, err := f.d.GetStepsByRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[types.StepName]*StepResult, len(steps))
	for _, step := range steps {
		byName[step.StepName] = step
	}
	for _, name := range []types.StepName{types.StepDocument, types.StepLint} {
		if err := f.d.CompleteStep(byName[name].ID, 0, 1, string(name)+".log"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.d.CompleteHeadValidation(restored.ID, restored.HeadSHA); err != nil {
		t.Fatal(err)
	}
	if err := f.d.StartStep(byName[types.StepPush].ID); err != nil {
		t.Fatal(err)
	}
	if err := f.d.SetRunPushActive(restored.ID, true); err != nil {
		t.Fatal(err)
	}
	return f, restored, byName[types.StepPush].ID
}

func TestExactRecoveryRefObservationJournalRefusesRememberedAmbiguity(t *testing.T) {
	tests := []struct {
		name         string
		observations []string
		wantState    string
		wantErrorAt  int
		bind         bool
	}{
		{
			name:         "old to expected",
			observations: []string{"published-head", "head-3"},
			wantState:    ExactRecoveryRefObservationExpected,
			wantErrorAt:  -1,
			bind:         true,
		},
		{
			name:         "target before invocation",
			observations: []string{"published-head", "head-3"},
			wantState:    ExactRecoveryRefObservationAmbiguous,
			wantErrorAt:  1,
		},
		{
			name:         "old to third",
			observations: []string{"published-head", "third-head"},
			wantState:    ExactRecoveryRefObservationAmbiguous,
			wantErrorAt:  1,
		},
		{
			name:         "old to third to expected",
			observations: []string{"published-head", "third-head", "head-3"},
			wantState:    ExactRecoveryRefObservationAmbiguous,
			wantErrorAt:  1,
		},
		{
			name:         "rollback after expected",
			observations: []string{"published-head", "head-3", "published-head"},
			wantState:    ExactRecoveryRefObservationAmbiguous,
			wantErrorAt:  2,
			bind:         true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, restored, _ := prepareExactRecoveryPushCrash(t)
			journal, err := f.d.PrepareExactRecoveryRefObservation(
				restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
				tc.observations[0], now()+30,
			)
			if err != nil {
				t.Fatal(err)
			}
			if tc.bind {
				operation, err := f.d.GetExactRecoveryPushOperation(restored.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.d.MarkExactRecoveryPushInvoked(
					restored.ID, operation.OperationID, "published-head",
				); err != nil {
					t.Fatal(err)
				}
				if err := f.d.BindExactRecoveryPushOperation(
					restored.ID, operation.OperationID, PushBinding{
						HeadSHA: restored.HeadSHA, TargetKind: "upstream",
						TargetFingerprint: "target-fingerprint", Ref: "refs/heads/feature",
					},
				); err != nil {
					t.Fatal(err)
				}
			}
			for index, observed := range tc.observations[1:] {
				err := f.d.RecordExactRecoveryRefObservation(restored.ID, observed)
				observationIndex := index + 1
				if observationIndex == tc.wantErrorAt {
					if err == nil {
						t.Fatalf("observation %q was accepted", observed)
					}
					break
				}
				if err != nil {
					t.Fatalf("observation %q: %v", observed, err)
				}
			}
			journal, err = f.d.GetExactRecoveryRefObservation(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if journal == nil || journal.State != tc.wantState {
				t.Fatalf("journal = %#v, want state %s", journal, tc.wantState)
			}
			events, err := f.d.ListExactRecoveryRefObservationEvents(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != journal.Attempts || events[len(events)-1].State != tc.wantState ||
				events[len(events)-1].Observation != journal.LastObservation {
				t.Fatalf("journal events = %#v for journal %#v", events, journal)
			}
			if tc.wantState == ExactRecoveryRefObservationAmbiguous {
				if err := f.d.RecordExactRecoveryRefObservation(restored.ID, restored.HeadSHA); err == nil {
					t.Fatal("restart forgot durable ambiguity")
				}
			}
		})
	}
}

func TestExactRecoveryRefObservationJournalPersistsTimeout(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		"published-head", now()+30,
	); err != nil {
		t.Fatal(err)
	}
	mustExecRecoveryTest(t, f.d,
		`UPDATE run_recovery_ref_observations SET deadline_at = ? WHERE run_id = ?`,
		now()-1, restored.ID,
	)
	if err := f.d.RecordExactRecoveryRefObservation(restored.ID, "published-head"); err == nil {
		t.Fatal("expired stale observation was accepted")
	}
	journal, err := f.d.GetExactRecoveryRefObservation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if journal == nil || journal.State != ExactRecoveryRefObservationAmbiguous ||
		journal.LastObservation != "timeout" {
		t.Fatalf("timeout journal = %#v", journal)
	}
}

func TestExactRecoveryPushInvocationRefusesPreexistingTarget(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		"published-head", now()+30,
	); err != nil {
		t.Fatal(err)
	}
	operation, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(
		restored.ID, operation.OperationID, restored.HeadSHA,
	); err == nil {
		t.Fatal("preexisting target was admitted as this operation's Push")
	}
	journal, err := f.d.GetExactRecoveryRefObservation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != ExactRecoveryRefObservationAmbiguous ||
		operation.Phase != ExactRecoveryPushPrepared || operation.InvokedAt != nil {
		t.Fatalf("preexisting target state = journal %#v operation %#v", journal, operation)
	}
}

func TestValidateActiveExactRecoveryRejectsUnscopedAzureOperationStates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *exactFinalHeadRecoveryFixture, *Run)
	}{
		{
			name: "expected observation with prior binding",
			mutate: func(t *testing.T, f *exactFinalHeadRecoveryFixture, restored *Run) {
				ts := now()
				mustExecRecoveryTest(t, f.d,
					`UPDATE run_recovery_ref_observations
					 SET attempts = 2, state = ?, last_observation = ?, updated_at = ?
					 WHERE run_id = ?`,
					ExactRecoveryRefObservationExpected, restored.HeadSHA, ts, restored.ID,
				)
				mustExecRecoveryTest(t, f.d,
					`INSERT INTO run_recovery_ref_observation_events (
						run_id, attempt, observation, prior_state, state, observed_at
					 ) VALUES (?, 2, ?, ?, ?, ?)`,
					restored.ID, restored.HeadSHA, ExactRecoveryRefObservationStale,
					ExactRecoveryRefObservationExpected, ts,
				)
			},
		},
		{
			name: "bound phase without invocation",
			mutate: func(t *testing.T, f *exactFinalHeadRecoveryFixture, restored *Run) {
				mustExecRecoveryTest(t, f.d,
					`UPDATE run_recovery_push_operations
					 SET phase = ?, bound_at = ?, updated_at = ? WHERE run_id = ?`,
					ExactRecoveryPushBound, now(), now(), restored.ID,
				)
			},
		},
		{
			name: "exact binding with prepared operation",
			mutate: func(t *testing.T, f *exactFinalHeadRecoveryFixture, restored *Run) {
				if err := f.d.UpdateRunPushBinding(restored.ID, PushBinding{
					HeadSHA: restored.HeadSHA, TargetKind: "upstream",
					TargetFingerprint: "target-fingerprint", Ref: "refs/heads/feature",
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, restored, _ := prepareExactRecoveryPushCrash(t)
			if _, err := f.d.PrepareExactRecoveryRefObservation(
				restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
				"published-head", now()+30,
			); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, f, restored)
			if err := f.d.SetRunPushActive(restored.ID, false); err != nil {
				t.Fatal(err)
			}
			if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(
				restored.ID, 3, types.AllSteps(),
			); err == nil {
				t.Fatal("unscoped Azure operation state was admitted")
			}
		})
	}
}

func TestReconcileStaleExactRecoveryPushCustodyAcrossCrashBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		remoteHead   string
		invoke       bool
		bind         bool
		completePush bool
		wantError    bool
		wantBound    bool
		wantPhase    string
		wantAttempt  int
		wantState    string
	}{
		{name: "prepared before push", remoteHead: "published-head", wantPhase: ExactRecoveryPushPrepared, wantAttempt: 1, wantState: ExactRecoveryRefObservationStale},
		{name: "delivered before invocation", remoteHead: "head-3", wantError: true, wantPhase: ExactRecoveryPushPrepared, wantAttempt: 1, wantState: ExactRecoveryRefObservationAmbiguous},
		{name: "invoked before remote mutation", remoteHead: "published-head", invoke: true, wantPhase: ExactRecoveryPushPrepared, wantAttempt: 2, wantState: ExactRecoveryRefObservationStale},
		{name: "externally succeeded unverified", remoteHead: "head-3", invoke: true, wantBound: true, wantPhase: ExactRecoveryPushBound, wantAttempt: 1, wantState: ExactRecoveryRefObservationStale},
		{name: "after binding before completion", remoteHead: "head-3", invoke: true, bind: true, wantBound: true, wantPhase: ExactRecoveryPushBound, wantAttempt: 1, wantState: ExactRecoveryRefObservationStale},
		{name: "after completion before release", remoteHead: "head-3", invoke: true, bind: true, completePush: true, wantBound: true, wantPhase: ExactRecoveryPushBound, wantAttempt: 1, wantState: ExactRecoveryRefObservationStale},
		{name: "remote mismatch", remoteHead: "other-head", invoke: true, wantError: true, wantPhase: ExactRecoveryPushInvoked, wantAttempt: 1, wantState: ExactRecoveryRefObservationAmbiguous},
		{name: "completed without binding", remoteHead: "head-3", invoke: true, completePush: true, wantError: true, wantPhase: ExactRecoveryPushInvoked, wantAttempt: 1, wantState: ExactRecoveryRefObservationStale},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, restored, pushID := prepareExactRecoveryPushCrash(t)
			if _, err := f.d.PrepareExactRecoveryRefObservation(
				restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
				"published-head", now()+30,
			); err != nil {
				t.Fatal(err)
			}
			operation, err := f.d.GetExactRecoveryPushOperation(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.invoke {
				if err := f.d.MarkExactRecoveryPushInvoked(
					restored.ID, operation.OperationID, "published-head",
				); err != nil {
					t.Fatal(err)
				}
			}
			if tc.bind {
				if err := f.d.BindExactRecoveryPushOperation(restored.ID, operation.OperationID, PushBinding{
					HeadSHA: restored.HeadSHA, TargetKind: "upstream", TargetFingerprint: "target-fingerprint", Ref: "refs/heads/feature",
				}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.completePush {
				if err := f.d.CompleteStep(pushID, 0, 1, "push.log"); err != nil {
					t.Fatal(err)
				}
			}
			reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
				restored.ID, tc.remoteHead, "refs/heads/feature", restored.HeadSHA,
				now()+30, 3, types.AllSteps(),
			)
			if tc.wantError {
				if err == nil || reconciled {
					t.Fatalf("inconsistent crash state reconciled: reconciled=%v err=%v", reconciled, err)
				}
			} else if err != nil || !reconciled {
				t.Fatalf("reconcile crash state: reconciled=%v err=%v", reconciled, err)
			}
			got, err := f.d.GetRun(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.PushActive == !tc.wantError {
				t.Fatalf("push_active = %v after reconciliation error=%v", got.PushActive, tc.wantError)
			}
			if tc.wantBound && (got.LastPushedSHA == nil || *got.LastPushedSHA != restored.HeadSHA ||
				got.PushGeneration == nil || *got.PushGeneration != 2) {
				t.Fatalf("exact binding was not preserved: %#v", got)
			}
			journal, err := f.d.GetExactRecoveryRefObservation(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if journal == nil || journal.State != tc.wantState {
				t.Fatalf("reconciled journal = %#v, want state %s", journal, tc.wantState)
			}
			operation, err = f.d.GetExactRecoveryPushOperation(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if operation == nil || operation.Phase != tc.wantPhase || operation.Attempt != tc.wantAttempt {
				t.Fatalf("reconciled operation = %#v, want phase %s attempt %d", operation, tc.wantPhase, tc.wantAttempt)
			}
			if !tc.wantError {
				if again, err := f.d.ReconcileStaleExactRecoveryPushCustody(
					restored.ID, tc.remoteHead, "refs/heads/feature", restored.HeadSHA,
					now()+30, 3, types.AllSteps(),
				); err != nil || again {
					t.Fatalf("repeated reconciliation = %v, %v", again, err)
				}
			}
		})
	}
}

func TestReconcileStaleExactRecoveryPushCustodyRejectsChangedSourceClaim(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
		restored.ID, f.pushed, "refs/heads/feature", "superseding-head",
		now()+30, 3, types.AllSteps(),
	)
	if err == nil || reconciled {
		t.Fatalf("changed source claim reconciled: reconciled=%v err=%v", reconciled, err)
	}
	got, err := f.d.GetRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.PushActive || got.LastPushedSHA == nil || *got.LastPushedSHA != f.pushed {
		t.Fatalf("changed source claim mutated recovery: %#v", got)
	}
}

func TestReconcileStaleExactRecoveryPushCustodyRotatesFreshAttemptDeadline(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	firstDeadline := now() + 30
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, firstDeadline,
	); err != nil {
		t.Fatal(err)
	}
	first, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, first.OperationID, f.pushed); err != nil {
		t.Fatal(err)
	}
	expiredPreparedAt := now() - 60
	expiredDeadline := now() - 30
	if _, err := f.d.sql.Exec(
		`UPDATE run_recovery_ref_observations SET deadline_at = ? WHERE run_id = ?`,
		expiredDeadline, restored.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.d.sql.Exec(
		`UPDATE run_recovery_push_attempts
		 SET prepared_at = ?, deadline_at = ?
		 WHERE run_id = ? AND attempt = 1`,
		expiredPreparedAt, expiredDeadline, restored.ID,
	); err != nil {
		t.Fatal(err)
	}
	nextDeadline := now() + 90
	reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
		restored.ID, f.pushed, "refs/heads/feature", restored.HeadSHA,
		nextDeadline, 3, types.AllSteps(),
	)
	if err != nil || !reconciled {
		t.Fatalf("delayed restart reconciliation = %v, %v", reconciled, err)
	}
	attempts, err := f.d.ListExactRecoveryPushAttempts(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempt history length = %d, want 2", len(attempts))
	}
	if attempts[0].OperationID != first.OperationID ||
		attempts[0].DeadlineAt != expiredDeadline ||
		attempts[0].Disposition == nil ||
		*attempts[0].Disposition != ExactRecoveryPushNotApplied ||
		attempts[0].ClosedAt == nil {
		t.Fatalf("closed attempt history changed: %#v", attempts[0])
	}
	if attempts[1].OperationID == first.OperationID ||
		attempts[1].DeadlineAt != nextDeadline ||
		attempts[1].Phase != ExactRecoveryPushPrepared ||
		attempts[1].Disposition != nil {
		t.Fatalf("fresh attempt is inconsistent: %#v", attempts[1])
	}
	journal, err := f.d.GetExactRecoveryRefObservation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if journal.DeadlineAt != nextDeadline || journal.State != ExactRecoveryRefObservationStale {
		t.Fatalf("rotated observation journal = %#v", journal)
	}
	observations, err := f.d.ListExactRecoveryPushAttemptObservations(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) < 2 ||
		observations[0].OperationID != attempts[0].OperationID ||
		observations[len(observations)-1].OperationID != attempts[1].OperationID {
		t.Fatalf("attempt-scoped observations = %#v", observations)
	}
	if err := f.d.SetRunPushActive(restored.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, now()+10,
	); err != nil {
		t.Fatalf("fresh attempt reused expired deadline: %v", err)
	}
}

func TestReconcileStaleExactRecoveryPushCustodyRotatesExpiredPreparedAttempt(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, now()+30,
	); err != nil {
		t.Fatal(err)
	}
	first, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	expireExactRecoveryPushAttempt(t, f.d, restored.ID, 1)
	nextDeadline := now() + 90
	reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
		restored.ID, f.pushed, "refs/heads/feature", restored.HeadSHA,
		nextDeadline, 3, types.AllSteps(),
	)
	if err != nil || !reconciled {
		t.Fatalf("prepared crash reconciliation = %v, %v", reconciled, err)
	}
	attempts, err := f.d.ListExactRecoveryPushAttempts(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 ||
		attempts[0].OperationID != first.OperationID ||
		attempts[0].Phase != ExactRecoveryPushPrepared ||
		attempts[0].InvokedAt != nil ||
		attempts[0].Disposition == nil ||
		*attempts[0].Disposition != ExactRecoveryPushNotApplied ||
		attempts[0].ClosedAt == nil {
		t.Fatalf("prepared attempt history = %#v", attempts)
	}
	if attempts[1].OperationID == first.OperationID ||
		attempts[1].Phase != ExactRecoveryPushPrepared ||
		attempts[1].DeadlineAt != nextDeadline ||
		attempts[1].Disposition != nil {
		t.Fatalf("fresh prepared attempt = %#v", attempts[1])
	}
	run, err := f.d.GetRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PushActive || run.LastPushedSHA == nil || *run.LastPushedSHA != f.pushed {
		t.Fatalf("prepared recovery changed publication or retained stale custody: %#v", run)
	}
	if err := f.d.SetRunPushActive(restored.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, first.OperationID, f.pushed); err == nil {
		t.Fatal("expired prepared owner crossed the rotation CAS")
	}
	current, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Attempt != 2 || current.Phase != ExactRecoveryPushPrepared {
		t.Fatalf("prepared rotation created extra attempts: %#v", current)
	}
}

func TestReconcileStaleExactRecoveryPushCustodyRefusesUncertainPreparedInvocation(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, now()+30,
	); err != nil {
		t.Fatal(err)
	}
	expireExactRecoveryPushAttempt(t, f.d, restored.ID, 1)
	if _, err := f.d.sql.Exec(
		`UPDATE run_recovery_push_attempts
		 SET invoked_at = ?
		 WHERE run_id = ? AND attempt = 1`,
		now(), restored.ID,
	); err != nil {
		t.Fatal(err)
	}
	reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
		restored.ID, f.pushed, "refs/heads/feature", restored.HeadSHA,
		now()+90, 3, types.AllSteps(),
	)
	if err == nil || reconciled {
		t.Fatalf("uncertain prepared invocation reconciled: reconciled=%v err=%v", reconciled, err)
	}
	attempts, err := f.d.ListExactRecoveryPushAttempts(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Disposition != nil || attempts[0].ClosedAt != nil {
		t.Fatalf("uncertain prepared attempt mutated: %#v", attempts)
	}
	run, err := f.d.GetRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !run.PushActive || run.LastPushedSHA == nil || *run.LastPushedSHA != f.pushed {
		t.Fatalf("uncertain prepared attempt lost custody: %#v", run)
	}
}

func TestReconcileStaleExactRecoveryPushCustodyDoesNotRotateExpiredPreparedAttemptOnRemoteAmbiguity(t *testing.T) {
	tests := []struct {
		name       string
		remoteHead string
	}{
		{name: "expected target", remoteHead: "head-3"},
		{name: "third oid", remoteHead: "other-head"},
		{name: "malformed oid", remoteHead: "not an oid"},
		{name: "missing oid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, restored, _ := prepareExactRecoveryPushCrash(t)
			if _, err := f.d.PrepareExactRecoveryRefObservation(
				restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
				f.pushed, now()+30,
			); err != nil {
				t.Fatal(err)
			}
			expireExactRecoveryPushAttempt(t, f.d, restored.ID, 1)
			reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
				restored.ID, tc.remoteHead, "refs/heads/feature", restored.HeadSHA,
				now()+90, 3, types.AllSteps(),
			)
			if err == nil || reconciled {
				t.Fatalf("ambiguous remote reconciled: reconciled=%v err=%v", reconciled, err)
			}
			attempts, err := f.d.ListExactRecoveryPushAttempts(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 1 || attempts[0].Disposition != nil || attempts[0].ClosedAt != nil {
				t.Fatalf("ambiguous remote rotated prepared attempt: %#v", attempts)
			}
			run, err := f.d.GetRun(restored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !run.PushActive || run.LastPushedSHA == nil || *run.LastPushedSHA != f.pushed {
				t.Fatalf("ambiguous remote changed publication custody: %#v", run)
			}
		})
	}
}

func TestReconcileStaleExactRecoveryPushCustodyBoundsAttemptRotation(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, now()+30,
	); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		operation, err := f.d.GetExactRecoveryPushOperation(restored.ID)
		if err != nil {
			t.Fatal(err)
		}
		if operation.Attempt != attempt {
			t.Fatalf("operation attempt = %d, want %d", operation.Attempt, attempt)
		}
		if attempt%2 == 1 {
			expireExactRecoveryPushAttempt(t, f.d, restored.ID, attempt)
		} else if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, operation.OperationID, f.pushed); err != nil {
			t.Fatal(err)
		}
		reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
			restored.ID, f.pushed, "refs/heads/feature", restored.HeadSHA,
			now()+int64(60+attempt), 3, types.AllSteps(),
		)
		if attempt < 3 {
			if err != nil || !reconciled {
				t.Fatalf("attempt %d rotation = %v, %v", attempt, reconciled, err)
			}
			if err := f.d.SetRunPushActive(restored.ID, true); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil || reconciled {
			t.Fatalf("exhausted attempt rotated: reconciled=%v err=%v", reconciled, err)
		}
	}
	run, err := f.d.GetRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !run.PushActive {
		t.Fatal("attempt exhaustion released Push custody")
	}
	attempts, err := f.d.ListExactRecoveryPushAttempts(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 || attempts[2].Disposition != nil {
		t.Fatalf("attempt budget history = %#v", attempts)
	}
}

func expireExactRecoveryPushAttempt(t *testing.T, database *DB, runID string, attempt int) {
	t.Helper()
	preparedAt := now() - 60
	deadlineAt := now() - 30
	if _, err := database.sql.Exec(
		`UPDATE run_recovery_ref_observations
		 SET deadline_at = ?
		 WHERE run_id = ?`,
		deadlineAt, runID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(
		`UPDATE run_recovery_push_attempts
		 SET prepared_at = ?, deadline_at = ?
		 WHERE run_id = ? AND attempt = ?`,
		preparedAt, deadlineAt, runID, attempt,
	); err != nil {
		t.Fatal(err)
	}
}

func TestExactRecoveryPushOperationInvocationCASIsSingleOwner(t *testing.T) {
	f, restored, _ := prepareExactRecoveryPushCrash(t)
	if _, err := f.d.PrepareExactRecoveryRefObservation(
		restored.ID, "azuredevops", "refs/heads/feature", restored.HeadSHA,
		f.pushed, now()+30,
	); err != nil {
		t.Fatal(err)
	}
	first, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, first.OperationID, f.pushed); err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, first.OperationID, f.pushed); err == nil {
		t.Fatal("second owner marked the same operation invoked")
	}
	if reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(
		restored.ID, f.pushed, "refs/heads/feature", restored.HeadSHA,
		now()+60, 3, types.AllSteps(),
	); err != nil || !reconciled {
		t.Fatalf("rotate first owner = %v, %v", reconciled, err)
	}
	if err := f.d.SetRunPushActive(restored.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := f.d.MarkExactRecoveryPushInvoked(restored.ID, first.OperationID, f.pushed); err == nil {
		t.Fatal("stale operation owner crossed the rotation CAS")
	}
	current, err := f.d.GetExactRecoveryPushOperation(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Attempt != 2 || current.Phase != ExactRecoveryPushPrepared {
		t.Fatalf("competing owner changed current operation: %#v", current)
	}
}

func TestCancelExactRecoveryAsSupersededTerminalizesActivePhase(t *testing.T) {
	f, restored, pushID := prepareExactRecoveryPushCrash(t)
	if err := f.d.CancelExactRecoveryAsSuperseded(restored.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.d.CancelExactRecoveryAsSuperseded(restored.ID); err != nil {
		t.Fatalf("idempotent supersession: %v", err)
	}
	got, err := f.d.GetRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCancelled || got.Error == nil ||
		*got.Error != types.RunCancelReasonSuperseded || got.PushActive {
		t.Fatalf("superseded run = %#v", got)
	}
	steps, err := f.d.GetStepsByRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if step.ID == pushID && (step.Status != types.StepStatusFailed || step.Error == nil ||
			*step.Error != types.RunCancelReasonSuperseded) {
			t.Fatalf("superseded Push step = %#v", step)
		}
	}
}

func TestExactRecoveryPRUpdatePersistsEveryMutationBoundary(t *testing.T) {
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	restored, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	steps, err := f.d.GetStepsByRun(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[types.StepName]*StepResult, len(steps))
	for _, step := range steps {
		byName[step.StepName] = step
	}
	for _, name := range []types.StepName{types.StepDocument, types.StepLint} {
		if err := f.d.CompleteStep(byName[name].ID, 0, 1, string(name)+".log"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.d.CompleteHeadValidation(restored.ID, restored.HeadSHA); err != nil {
		t.Fatal(err)
	}
	if err := f.d.StartStep(byName[types.StepPush].ID); err != nil {
		t.Fatal(err)
	}
	if err := f.d.UpdateRunPushBinding(restored.ID, PushBinding{
		HeadSHA: restored.HeadSHA, TargetKind: "upstream", TargetFingerprint: "target-fingerprint", Ref: "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.d.CompleteStep(byName[types.StepPush].ID, 0, 1, "push.log"); err != nil {
		t.Fatal(err)
	}
	if err := f.d.StartStep(byName[types.StepPR].ID); err != nil {
		t.Fatal(err)
	}
	update, err := f.d.PrepareExactRecoveryPRUpdate(
		restored.ID, byName[types.StepPR].ID, *restored.PRURL, restored.HeadSHA,
		"prior title", "prior body", "intended title", "intended body",
	)
	if err != nil {
		t.Fatal(err)
	}
	if update.State != ExactRecoveryPRUpdatePrepared || update.AppliedAt != nil {
		t.Fatalf("prepared PR update = %#v", update)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate prepared PR update: %v", err)
	}
	if err := f.d.MarkExactRecoveryPRUpdateApplied(restored.ID, "intended title", "intended body"); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate applied running PR update: %v", err)
	}
	if err := f.d.CompleteStep(byName[types.StepPR].ID, 0, 1, "pr.log"); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate applied completed PR update: %v", err)
	}
	if err := f.d.MarkExactRecoveryPRUpdateApplied(restored.ID, "other", "content"); err == nil {
		t.Fatal("mismatched remote content marked applied")
	}
}

func TestValidateActiveExactFinalHeadCapacityRecoveryRejectsChangedProof(t *testing.T) {
	f := newExactFinalHeadRecoveryFixture(t)
	failure := f.inspect()
	if _, err := f.d.RestoreExactFinalHeadCapacityFailure(f.run.ID, failure.EvidenceToken, 3, types.AllSteps()); err != nil {
		t.Fatal(err)
	}
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(f.run.ID, 3, types.AllSteps()); err != nil {
		t.Fatalf("validate restored state: %v", err)
	}
	mustExecRecoveryTest(t, f.d, `UPDATE runs SET test_head_sha = 'other' WHERE id = ?`, f.run.ID)
	if err := f.d.ValidateActiveExactFinalHeadCapacityRecovery(f.run.ID, 3, types.AllSteps()); err == nil {
		t.Fatal("active recovery accepted changed exact proof")
	}
}

func mustExecRecoveryTest(t *testing.T, d *DB, query string, args ...any) {
	t.Helper()
	if _, err := d.sql.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", fmt.Sprintf("%.80s", query), err)
	}
}
