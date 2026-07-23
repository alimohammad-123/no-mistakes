package db

import (
	"reflect"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	legacyShutdownError       = "daemon shutting down"
	arenaEvidenceRunID        = "01KY4FMNYM4AX8PA9MY92QMHN4"
	arenaEvidenceBranch       = "fm/arena-no-mistakes-beta"
	arenaEvidencePipelineHead = "9d15bc97a6c93d0b8466f70704cf90b510ed2596"
	arenaEvidenceSubmitted    = "d1434c61a3eea0523785c6452d665d63c5997af8"
)

func seedLegacyInterruptedGate(t *testing.T, d *DB) (*Run, *StepResult, string) {
	t.Helper()
	repo, err := d.InsertRepo("/home/user/interrupted", "git@github.com:user/interrupted.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRunWithBaseBranch(repo.ID, arenaEvidenceBranch, arenaEvidencePipelineHead, "base", "staging")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(`UPDATE runs SET id = ?, submitted_head_sha = ? WHERE id = ?`, arenaEvidenceRunID, arenaEvidenceSubmitted, run.ID); err != nil {
		t.Fatal(err)
	}
	run.ID = arenaEvidenceRunID
	run.SubmittedHeadSHA = stringPtr(arenaEvidenceSubmitted)
	intent := "preserve the adoption canary"
	if err := d.UpdateRunIntent(run.ID, RunIntent{Summary: intent, Source: RunIntentSourceAgent, Score: 1}); err != nil {
		t.Fatal(err)
	}
	var interrupted *StepResult
	for _, name := range types.AllSteps() {
		step, err := d.InsertStepResult(run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		switch name {
		case types.StepIntent, types.StepRebase, types.StepReview:
			if err := d.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			if err := d.CompleteStep(step.ID, 0, 7, "preserved.log"); err != nil {
				t.Fatal(err)
			}
		case types.StepTest:
			interrupted = step
			if err := d.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			findings := `{"findings":[{"id":"test-1","severity":"error","description":"exact command summary","action":"auto-fix"}],"summary":"exact command summary"}`
			if err := d.SetStepFindings(step.ID, findings); err != nil {
				t.Fatal(err)
			}
			if _, err := d.InsertStepRound(step.ID, 1, "initial", &findings, nil, 123); err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateStepStatusWithDuration(step.ID, types.StepStatusAwaitingApproval, 123); err != nil {
				t.Fatal(err)
			}
		}
	}
	if interrupted == nil {
		t.Fatal("Test step not seeded")
	}
	if err := d.SetRunAwaitingAgent(run.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteRunAwaitingAgent(run.ID, 10); err != nil {
		t.Fatal(err)
	}
	if err := d.FailStep(interrupted.ID, legacyShutdownError, 123); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunErrorStatus(run.ID, legacyShutdownError, types.RunFailed); err != nil {
		t.Fatal(err)
	}
	run.Status = types.RunFailed
	run.Error = stringPtr(legacyShutdownError)
	return run, interrupted, intent
}

func stringPtr(value string) *string { return &value }

func TestRestoreLegacyInterruptedGateRestoresExactTransaction(t *testing.T) {
	d := openTestDB(t)
	run, interrupted, intent := seedLegacyInterruptedGate(t, d)
	if _, err := d.sql.Exec(`UPDATE runs SET source_ref = NULL WHERE id = ?`, run.ID); err != nil {
		t.Fatal(err)
	}

	before, err := d.GetStepResult(interrupted.ID)
	if err != nil {
		t.Fatal(err)
	}
	roundsBefore, err := d.GetRoundsByStep(interrupted.ID)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := d.RestoreLegacyInterruptedGate(run.ID, run.RepoID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, intent, "refs/heads/fm/arena-no-mistakes-beta", types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	if restored.Run.ID != run.ID || restored.Step.ID != interrupted.ID {
		t.Fatalf("restored wrong transaction: %#v", restored)
	}
	if restored.Run.Status != types.RunRunning || restored.Run.Error != nil || restored.Run.AwaitingAgentSince == nil {
		t.Fatalf("restored run = %#v", restored.Run)
	}
	if restored.Run.SourceRef == nil || *restored.Run.SourceRef != "refs/heads/fm/arena-no-mistakes-beta" {
		t.Fatalf("source ref = %v", restored.Run.SourceRef)
	}
	if restored.Step.Status != types.StepStatusAwaitingApproval || restored.Step.Error != nil || restored.Step.CompletedAt != nil {
		t.Fatalf("restored step = %#v", restored.Step)
	}
	if restored.Step.StartedAt == nil || *restored.Step.StartedAt != *before.StartedAt || restored.Step.DurationMS == nil || *restored.Step.DurationMS != *before.DurationMS || restored.Step.FindingsJSON == nil || *restored.Step.FindingsJSON != *before.FindingsJSON {
		t.Fatalf("preserved step timing/findings changed: before=%#v after=%#v", before, restored.Step)
	}
	roundsAfter, err := d.GetRoundsByStep(interrupted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundsAfter) != 1 || roundsAfter[0].ID != roundsBefore[0].ID || *roundsAfter[0].FindingsJSON != *roundsBefore[0].FindingsJSON {
		t.Fatalf("rounds changed: before=%#v after=%#v", roundsBefore, roundsAfter)
	}
	runs, err := d.GetRunsByRepo(run.RepoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("run count = %d, want 1", len(runs))
	}

	if _, err := d.RestoreLegacyInterruptedGate(run.ID, run.RepoID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, intent, "refs/heads/fm/arena-no-mistakes-beta", types.AllSteps()); err == nil {
		t.Fatal("second restore should be refused without changing the active run")
	}
	still, _ := d.GetRun(run.ID)
	if still.Status != types.RunRunning || still.AwaitingAgentSince == nil {
		t.Fatalf("second restore changed run: %#v", still)
	}
}

func TestFailClaimedInterruptedGateClearsParkedMarkerAtomically(t *testing.T) {
	d := openTestDB(t)
	run, interrupted, intent := seedLegacyInterruptedGate(t, d)
	restored, err := d.RestoreLegacyInterruptedGate(run.ID, run.RepoID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, intent, "refs/heads/fm/arena-no-mistakes-beta", types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.FailClaimedInterruptedGate(restored.Run.ID, restored.Step.ID, "integrity failure", 123); err != nil {
		t.Fatal(err)
	}
	failed, _ := d.GetRun(run.ID)
	failedStep, _ := d.GetStepResult(interrupted.ID)
	if failed.Status != types.RunFailed || failed.AwaitingAgentSince != nil || failed.Error == nil || *failed.Error != "integrity failure" {
		t.Fatalf("failed claim run = %#v", failed)
	}
	if failedStep.Status != types.StepStatusFailed || failedStep.Error == nil || *failedStep.Error != "integrity failure" {
		t.Fatalf("failed claim step = %#v", failedStep)
	}
}

func TestRestoreLegacyInterruptedGateRestoresFixReviewFromLatestRound(t *testing.T) {
	d := openTestDB(t)
	run, interrupted, intent := seedLegacyInterruptedGate(t, d)
	if _, err := d.sql.Exec(`UPDATE step_rounds SET trigger_type = 'auto_fix' WHERE step_result_id = ?`, interrupted.ID); err != nil {
		t.Fatal(err)
	}
	restored, err := d.RestoreLegacyInterruptedGate(run.ID, run.RepoID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, intent, "refs/heads/fm/arena-no-mistakes-beta", types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	if restored.Step.Status != types.StepStatusFixReview {
		t.Fatalf("restored status = %s, want fix_review", restored.Step.Status)
	}
}

func TestRestoreLegacyInterruptedGateRefusesAmbiguousShapes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *DB, *Run, *StepResult)
	}{
		{name: "different run error", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET error = 'real test failure' WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "different step error", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET error = 'real test failure' WHERE id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing findings", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET findings_json = NULL WHERE id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "empty findings", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET findings_json = '{"findings":[]}' WHERE id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed findings", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET findings_json = '{' WHERE id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing round", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`DELETE FROM step_rounds WHERE step_result_id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unknown round trigger", mutate: func(t *testing.T, d *DB, _ *Run, step *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_rounds SET trigger_type = 'mystery' WHERE step_result_id = ?`, step.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "pushed", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET last_pushed_sha = head_sha WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ci provenance", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET ci_ready_at = 1 WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "source ref mismatch", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET source_ref = 'refs/heads/other' WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "non-authoritative intent", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET intent_source = 'claude', intent_score = 0.5 WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "newer terminal run", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			if _, err := d.sql.Exec(`UPDATE runs SET created_at = 1 WHERE id = ?`, run.ID); err != nil {
				t.Fatal(err)
			}
			newer, err := d.InsertRun(run.RepoID, run.Branch, "new-head", "base")
			if err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateRunStatus(newer.ID, types.RunCompleted); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "active branch run", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			if _, err := d.sql.Exec(`UPDATE runs SET created_at = 1 WHERE id = ?`, run.ID); err != nil {
				t.Fatal(err)
			}
			active, err := d.InsertRun(run.RepoID, run.Branch, "active-head", "base")
			if err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateRunStatus(active.ID, types.RunRunning); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "pr", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET pr_url = 'https://example.invalid/pr/1' WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "custody returned", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE runs SET custody_returned_at = 1 WHERE id = ?`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "earlier pending", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET status = 'pending' WHERE run_id = ? AND step_name = 'review'`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "later completed", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET status = 'completed' WHERE run_id = ? AND step_name = 'document'`, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "second failed step", mutate: func(t *testing.T, d *DB, run *Run, _ *StepResult) {
			_, err := d.sql.Exec(`UPDATE step_results SET status = 'failed', error = ? WHERE run_id = ? AND step_name = 'document'`, legacyShutdownError, run.ID)
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := openTestDB(t)
			run, interrupted, intent := seedLegacyInterruptedGate(t, d)
			tc.mutate(t, d, run, interrupted)
			before, _ := d.GetRun(run.ID)
			beforeStep, _ := d.GetStepResult(interrupted.ID)
			if _, err := d.RestoreLegacyInterruptedGate(run.ID, run.RepoID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, intent, "refs/heads/fm/arena-no-mistakes-beta", types.AllSteps()); err == nil {
				t.Fatal("ambiguous legacy shape was restored")
			}
			after, _ := d.GetRun(run.ID)
			afterStep, _ := d.GetStepResult(interrupted.ID)
			if !reflect.DeepEqual(after, before) || !reflect.DeepEqual(afterStep, beforeStep) {
				t.Fatalf("refusal mutated state: run before=%#v after=%#v; step before=%#v after=%#v", before, after, beforeStep, afterStep)
			}
		})
	}
}
