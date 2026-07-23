package db

import (
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestRecordSuccessfulTestHeadRequiresExactActiveRunHead(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-proof", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}

	if err := d.BeginConfiguredTestAttempt(run.ID, "other-head"); err == nil {
		t.Fatal("began configured Test for a head other than runs.head_sha")
	}
	if err := d.BeginConfiguredTestAttempt(run.ID, "head-1"); err != nil {
		t.Fatalf("begin configured Test: %v", err)
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "other-head"); err == nil {
		t.Fatal("recorded Test proof for a head other than runs.head_sha")
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "head-1"); err != nil {
		t.Fatalf("record exact Test proof: %v", err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TestHeadSHA == nil || *got.TestHeadSHA != "head-1" {
		t.Fatalf("TestHeadSHA = %v, want head-1", got.TestHeadSHA)
	}
	if err := d.BeginConfiguredTestAttempt(run.ID, "head-1"); err != nil {
		t.Fatal(err)
	}
	got, err = d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TestHeadSHA != nil {
		t.Fatalf("new Test attempt retained contradictory old proof %v", *got.TestHeadSHA)
	}

	if err := d.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "head-1"); err == nil {
		t.Fatal("rewrote Test proof for terminal run")
	}
}

func TestScheduleHeadValidationReplayIsAtomicAndBounded(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-replay", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-2", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := d.SetRunCIReady(run.ID, true); err != nil {
		t.Fatal(err)
	}

	for _, name := range types.AllSteps() {
		step, err := d.InsertStepResult(run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		if name.Order() < types.StepTest.Order() {
			if err := d.CompleteStep(step.ID, 0, 1, "before.log"); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := d.StartStep(step.ID); err != nil {
			t.Fatal(err)
		}
		if err := d.CompleteStep(step.ID, 0, 1, "validation.log"); err != nil {
			t.Fatal(err)
		}
	}

	count, err := d.ScheduleHeadValidationReplay(run.ID, 2)
	if err != nil {
		t.Fatalf("schedule replay: %v", err)
	}
	if count != 1 {
		t.Fatalf("replay count = %d, want 1", count)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ValidationReplayCount != 1 || got.CIReadyAt != nil {
		t.Fatalf("run replay state = count %d ready %v", got.ValidationReplayCount, got.CIReadyAt)
	}
	steps, err := d.GetStepsByRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if step.StepName.Order() < types.StepTest.Order() {
			if step.Status != types.StepStatusCompleted {
				t.Fatalf("pre-validation step %s reset to %s", step.StepName, step.Status)
			}
			continue
		}
		if step.Status != types.StepStatusPending || step.StartedAt != nil || step.CompletedAt != nil || step.AgentPID != nil {
			t.Fatalf("step %s not reset safely: %#v", step.StepName, step)
		}
	}

	if count, err := d.ScheduleHeadValidationReplay(run.ID, 2); err != nil || count != 1 {
		t.Fatalf("idempotent same-target replay = count %d, err %v", count, err)
	}
	if err := d.UpdateRunHeadSHA(run.ID, "head-3"); err != nil {
		t.Fatal(err)
	}
	if count, err := d.ScheduleHeadValidationReplay(run.ID, 2); err != nil || count != 2 {
		t.Fatalf("schedule second distinct target = count %d, err %v", count, err)
	}
	if err := d.UpdateRunHeadSHA(run.ID, "head-4"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ScheduleHeadValidationReplay(run.ID, 2); err == nil {
		t.Fatal("scheduled distinct target beyond persisted bound")
	}
}

func TestScheduleHeadValidationReplayNeverRewritesTerminalHistory(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-terminal", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ScheduleHeadValidationReplay(run.ID, 3); err == nil {
		t.Fatal("scheduled final-head replay for terminal historical run")
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.ValidationReplayCount != 0 || got.TestHeadSHA != nil {
		t.Fatalf("terminal run was rewritten: %#v", got)
	}
}

func TestAdvanceRunHeadSHAPersistsReplayObligationIdempotently(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-advance-proof", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "head-1"); err != nil {
		t.Fatal(err)
	}
	if err := d.SetRunCIReady(run.ID, true); err != nil {
		t.Fatal(err)
	}

	count, err := d.AdvanceRunHeadSHA(run.ID, "head-1", "head-2", true, HeadAdvancePipeline)
	if err != nil || count != 1 {
		t.Fatalf("advance run head = count %d, err %v", count, err)
	}
	count, err = d.AdvanceRunHeadSHA(run.ID, "head-2", "head-2", true, HeadAdvancePipeline)
	if err != nil || count != 1 {
		t.Fatalf("idempotent advance = count %d, err %v", count, err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadSHA != "head-2" || got.ValidationTargetSHA == nil || *got.ValidationTargetSHA != "head-2" ||
		got.ValidationReplayCount != 1 || got.CIReadyAt != nil {
		t.Fatalf("advanced replay state = %#v", got)
	}
}

func TestAdvanceRunHeadSHARequiresMatchingPushCustody(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-push-advance", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AdvanceRunHeadSHA(run.ID, "head-1", "head-2", false, HeadAdvancePush); err == nil {
		t.Fatal("push-phase advance succeeded without push custody")
	}
	if err := d.SetRunPushActive(run.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AdvanceRunHeadSHA(run.ID, "head-1", "head-2", false, HeadAdvancePipeline); err == nil {
		t.Fatal("pipeline-phase advance stole push custody")
	}
	if _, err := d.AdvanceRunHeadSHA(run.ID, "head-1", "head-2", false, HeadAdvancePush); err != nil {
		t.Fatalf("push-phase advance with custody: %v", err)
	}
}

func TestRunHeadTransitionPersistsBeforeFinalizationAndIsIdempotent(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-head-transition", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordSuccessfulTestHead(run.ID, "head-1"); err != nil {
		t.Fatal(err)
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		t.Fatal(err)
	}

	transition, err := d.BeginRunHeadAdvance(run.ID, ref, "head-1", "head-2", true, HeadAdvancePipeline)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := d.BeginRunHeadAdvance(run.ID, ref, "head-1", "head-2", true, HeadAdvancePipeline)
	if err != nil {
		t.Fatalf("repeat begin: %v", err)
	}
	if !sameRunHeadTransition(transition, retry) {
		t.Fatalf("repeat begin changed transition: %#v != %#v", transition, retry)
	}
	before, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.HeadSHA != "head-1" || before.ValidationTargetSHA != nil ||
		before.HeadAdvanceGeneration != transition.OwnershipGeneration {
		t.Fatalf("pre-finalization run state = %#v", before)
	}

	count, err := d.FinalizeRunHeadAdvance(transition, false)
	if err != nil || count != 1 {
		t.Fatalf("finalize = count %d, err %v", count, err)
	}
	count, err = d.FinalizeRunHeadAdvance(transition, false)
	if err != nil || count != 1 {
		t.Fatalf("repeat finalize = count %d, err %v", count, err)
	}
	if pending, err := d.GetRunHeadTransition(run.ID); err != nil || pending != nil {
		t.Fatalf("transition after finalize = %#v, err %v", pending, err)
	}
}

func TestFinalizeRunHeadAdvanceRejectsCorruptDurableIntent(t *testing.T) {
	d := openTestDB(t)
	repo, _ := d.InsertRepo("/home/user/test-corrupt-head-transition", "git@github.com:user/project.git", "main")
	run, _ := d.InsertRun(repo.ID, "feature", "head-1", "base")
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		t.Fatal(err)
	}
	transition, err := d.BeginRunHeadAdvance(run.ID, ref, "head-1", "head-2", true, HeadAdvancePipeline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(
		`UPDATE run_head_transitions SET candidate_sha = 'corrupt-head' WHERE run_id = ?`, run.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := d.FinalizeRunHeadAdvance(transition, false); err == nil {
		t.Fatal("finalized a corrupt durable transition")
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadSHA != "head-1" || got.ValidationTargetSHA != nil {
		t.Fatalf("corrupt transition changed run proof state: %#v", got)
	}
	if pending, err := d.GetRunHeadTransition(run.ID); err != nil || pending == nil {
		t.Fatalf("corrupt transition was cleared: %#v, err %v", pending, err)
	}
}
