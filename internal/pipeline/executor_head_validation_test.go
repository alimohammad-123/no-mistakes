package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type proofRecordingTestStep struct {
	calls int
	heads []string
}

func (s *proofRecordingTestStep) Name() types.StepName { return types.StepTest }

func (s *proofRecordingTestStep) Execute(sctx *StepContext) (*StepOutcome, error) {
	s.calls++
	s.heads = append(s.heads, sctx.Run.HeadSHA)
	if err := sctx.DB.RecordSuccessfulTestHead(sctx.Run.ID, sctx.Run.HeadSHA); err != nil {
		return nil, err
	}
	head := sctx.Run.HeadSHA
	sctx.Run.TestHeadSHA = &head
	return &StepOutcome{}, nil
}

type headMutatingStep struct {
	name        types.StepName
	calls       int
	mutateEvery bool
	mutateFirst bool
}

func (s *headMutatingStep) Name() types.StepName { return s.name }

func (s *headMutatingStep) Execute(sctx *StepContext) (*StepOutcome, error) {
	s.calls++
	if s.mutateEvery || (s.mutateFirst && s.calls == 1) {
		path := filepath.Join(sctx.WorkDir, fmt.Sprintf("head-mutation-%s-%d", s.name, s.calls))
		if err := os.WriteFile(path, []byte(s.name), 0o644); err != nil {
			return nil, err
		}
		if _, err := git.Run(sctx.Ctx, sctx.WorkDir, "add", filepath.Base(path)); err != nil {
			return nil, err
		}
		if _, err := git.Run(sctx.Ctx, sctx.WorkDir, "commit", "-m", fmt.Sprintf("mutate %s %d", s.name, s.calls)); err != nil {
			return nil, err
		}
		head, err := git.HeadSHA(sctx.Ctx, sctx.WorkDir)
		if err != nil {
			return nil, err
		}
		if err := sctx.AdvanceHeadSHA(head); err != nil {
			return nil, err
		}
	}
	return &StepOutcome{}, nil
}

func TestExecutor_DocumentCommitReplaysConfiguredTestAtFinalHead(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	lint := &headMutatingStep{name: types.StepLint}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint, push}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if testStep.calls != 2 || document.calls != 2 || lint.calls != 2 || push.callCount() != 1 {
		t.Fatalf("calls = test %d document %d lint %d push %d, want 2/2/2/1", testStep.calls, document.calls, lint.calls, push.callCount())
	}
	if got := testStep.heads[len(testStep.heads)-1]; got != run.HeadSHA {
		t.Fatalf("last tested head = %q, final head = %q", got, run.HeadSHA)
	}
	persisted, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.TestHeadSHA == nil || *persisted.TestHeadSHA != persisted.HeadSHA {
		t.Fatalf("persisted proof = %v, final head = %q", persisted.TestHeadSHA, persisted.HeadSHA)
	}
}

func TestExecutor_UnchangedPostTestHeadDoesNotReplay(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument}
	lint := &headMutatingStep{name: types.StepLint}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint, push}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if testStep.calls != 1 || document.calls != 1 || lint.calls != 1 || push.callCount() != 1 {
		t.Fatalf("calls = test %d document %d lint %d push %d, want 1/1/1/1", testStep.calls, document.calls, lint.calls, push.callCount())
	}
}

func TestExecutor_LintFixCommitInvalidatesConfiguredTest(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument}
	lint := &headMutatingStep{name: types.StepLint, mutateFirst: true}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint, push}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if testStep.calls != 2 || document.calls != 2 || lint.calls != 2 || push.callCount() != 1 {
		t.Fatalf("calls = test %d document %d lint %d push %d, want 2/2/2/1", testStep.calls, document.calls, lint.calls, push.callCount())
	}
}

func TestExecutor_HeadValidationReplayIsBounded(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument, mutateEvery: true}
	lint := &headMutatingStep{name: types.StepLint}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint, push}, nil)

	err := exec.Execute(context.Background(), run, repo, workDir)
	if err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("Execute() error = %v, want bounded non-convergence failure", err)
	}
	if push.callCount() != 0 {
		t.Fatalf("push executed %d times despite stale Test proof", push.callCount())
	}
	persisted, getErr := database.GetRun(run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if persisted.Status != types.RunFailed {
		t.Fatalf("run status = %s, want failed", persisted.Status)
	}
}

func TestExecutor_ResumeLegacyRunningCIReplaysMissingProofInSameRun(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	const prURL = "https://github.com/test/repo/pull/1477"
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPRURL(run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPushBinding(run.ID, db.PushBinding{
		HeadSHA:           run.HeadSHA,
		TargetKind:        "upstream",
		TargetFingerprint: "fingerprint",
		Ref:               "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}

	testStep := &proofRecordingTestStep{}
	prStep := &mockStep{name: types.StepPR, outcome: &StepOutcome{PRURL: prURL}}
	steps := []Step{
		newPassStep(types.StepIntent),
		newPassStep(types.StepRebase),
		newPassStep(types.StepReview),
		testStep,
		newPassStep(types.StepDocument),
		newPassStep(types.StepLint),
		newPassStep(types.StepPush),
		prStep,
		newPassStep(types.StepCI),
	}
	for _, step := range steps {
		result, err := database.InsertStepResult(run.ID, step.Name())
		if err != nil {
			t.Fatal(err)
		}
		if step.Name() == types.StepCI {
			if err := database.StartStep(result.ID); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := database.CompleteStep(result.ID, 0, 1, string(step.Name())+".log"); err != nil {
			t.Fatal(err)
		}
	}
	run, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.TestHeadSHA != nil {
		t.Fatal("legacy fixture unexpectedly has configured-Test provenance")
	}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, steps, nil)
	originalID := run.ID

	if err := exec.ResumeHeadValidation(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("ResumeHeadValidation() error = %v", err)
	}
	got, err := database.GetRun(originalID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != originalID || got.Status != types.RunCompleted {
		t.Fatalf("recovered run = id %s status %s, want same completed run", got.ID, got.Status)
	}
	if got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("recovered Test proof = %v, head = %s", got.TestHeadSHA, got.HeadSHA)
	}
	if got.PRURL == nil || *got.PRURL != prURL || prStep.callCount() != 1 {
		t.Fatalf("recovered PR identity = %v, PR executions = %d", got.PRURL, prStep.callCount())
	}
	if testStep.calls != 1 {
		t.Fatalf("configured Test replay calls = %d, want 1", testStep.calls)
	}
}

func TestExecutor_ResumePersistedReplayTargetIsIdempotent(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	testStep := &proofRecordingTestStep{}
	steps := []Step{
		newPassStep(types.StepIntent), newPassStep(types.StepRebase), newPassStep(types.StepReview), testStep,
		newPassStep(types.StepDocument), newPassStep(types.StepLint), newPassStep(types.StepPush),
		newPassStep(types.StepPR), newPassStep(types.StepCI),
	}
	for _, step := range steps {
		result, err := database.InsertStepResult(run.ID, step.Name())
		if err != nil {
			t.Fatal(err)
		}
		if step.Name().Order() < types.StepTest.Order() {
			if err := database.CompleteStep(result.ID, 0, 1, string(step.Name())+".log"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if count, err := database.ScheduleHeadValidationReplay(run.ID, maxHeadValidationReplays); err != nil || count != 1 {
		t.Fatalf("seed replay target = count %d, err %v", count, err)
	}
	run, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, steps, nil)
	if err := exec.ResumeHeadValidation(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("ResumeHeadValidation() error = %v", err)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ValidationReplayCount != 1 || got.Status != types.RunCompleted || testStep.calls != 1 {
		t.Fatalf("idempotent replay = count %d status %s Test calls %d", got.ValidationReplayCount, got.Status, testStep.calls)
	}
}

func TestExecutor_ResumeFinalizesCrashAfterCompletedCIWithoutReplay(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	const prURL = "https://github.com/test/repo/pull/1477"
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.BeginConfiguredTestAttempt(run.ID, run.HeadSHA); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, run.HeadSHA); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPRURL(run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPushBinding(run.ID, db.PushBinding{HeadSHA: run.HeadSHA, TargetKind: "upstream", TargetFingerprint: "fingerprint", Ref: "refs/heads/feature"}); err != nil {
		t.Fatal(err)
	}
	testStep := &proofRecordingTestStep{}
	steps := []Step{
		newPassStep(types.StepIntent), newPassStep(types.StepRebase), newPassStep(types.StepReview), testStep,
		newPassStep(types.StepDocument), newPassStep(types.StepLint), newPassStep(types.StepPush),
		&mockStep{name: types.StepPR, outcome: &StepOutcome{PRURL: prURL}}, newPassStep(types.StepCI),
	}
	for _, step := range steps {
		result, err := database.InsertStepResult(run.ID, step.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := database.CompleteStep(result.ID, 0, 1, string(step.Name())+".log"); err != nil {
			t.Fatal(err)
		}
	}
	run, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, steps, nil)
	if err := exec.ResumeHeadValidation(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("ResumeHeadValidation() error = %v", err)
	}
	if testStep.calls != 0 {
		t.Fatalf("completed exact-head run replayed Test %d times", testStep.calls)
	}
	got, err := database.GetRun(run.ID)
	if err != nil || got.Status != types.RunCompleted {
		t.Fatalf("finalized run = %#v, err %v", got, err)
	}
}

func TestExecutor_CancellationDuringReplayPreservesCandidateAndNeverPushes(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	ctx, cancel := context.WithCancelCause(context.Background())
	testCalls := 0
	testStep := &adaptiveCallStep{name: types.StepTest, fn: func(sctx *StepContext) (*StepOutcome, error) {
		testCalls++
		if testCalls == 1 {
			return &StepOutcome{TestedHeadSHA: sctx.Run.HeadSHA}, nil
		}
		cancel(fmt.Errorf(types.RunCancelReasonAbortedByUser))
		return nil, ctx.Err()
	}}
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, newPassStep(types.StepLint), push}, nil)

	err := exec.Execute(ctx, run, repo, workDir)
	if err == nil {
		t.Fatal("Execute() succeeded despite replay cancellation")
	}
	got, getErr := database.GetRun(run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.Status != types.RunCancelled || got.Error == nil || *got.Error != types.RunCancelReasonAbortedByUser {
		t.Fatalf("cancelled replay run = status %s error %v", got.Status, got.Error)
	}
	if got.HeadSHA == "abc123" || push.callCount() != 0 {
		t.Fatalf("candidate/push after cancellation = head %s push calls %d", got.HeadSHA, push.callCount())
	}
}

func TestExecutor_NoConfiguredTestDoesNotReplayPostTestCommit(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := newPassStep(types.StepTest)
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	lint := newPassStep(types.StepLint)
	push := newPassStep(types.StepPush)
	exec := NewExecutor(database, p, &config.Config{}, nil, []Step{testStep, document, lint, push}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if testStep.callCount() != 1 || document.calls != 1 || lint.callCount() != 1 || push.callCount() != 1 {
		t.Fatalf("unexpected replay without configured Test: test %d document %d lint %d push %d", testStep.callCount(), document.calls, lint.callCount(), push.callCount())
	}
}
