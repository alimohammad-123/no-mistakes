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
		event.PriorError != ExactFinalHeadCapacityRunError(3) || event.PRURL != "https://github.com/user/project/pull/42" || event.LastPushedSHA != f.pushed {
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

func TestReconcileStaleExactRecoveryPushCustodyAcrossCrashBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		remoteHead     string
		advanceBinding bool
		completePush   bool
		wantError      bool
		wantBound      bool
	}{
		{name: "before push", remoteHead: "published-head"},
		{name: "after remote mutation before binding", remoteHead: "head-3", wantBound: true},
		{name: "after binding before completion", remoteHead: "head-3", advanceBinding: true, wantBound: true},
		{name: "after completion before release", remoteHead: "head-3", advanceBinding: true, completePush: true, wantBound: true},
		{name: "remote mismatch", remoteHead: "other-head", wantError: true},
		{name: "completed without binding", remoteHead: "head-3", completePush: true, wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, restored, pushID := prepareExactRecoveryPushCrash(t)
			if tc.advanceBinding {
				if err := f.d.UpdateRunPushBinding(restored.ID, PushBinding{
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
			reconciled, err := f.d.ReconcileStaleExactRecoveryPushCustody(restored.ID, tc.remoteHead, 3, types.AllSteps())
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
			if !tc.wantError {
				if again, err := f.d.ReconcileStaleExactRecoveryPushCustody(restored.ID, tc.remoteHead, 3, types.AllSteps()); err != nil || again {
					t.Fatalf("repeated reconciliation = %v, %v", again, err)
				}
			}
		})
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
