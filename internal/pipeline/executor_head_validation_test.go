package pipeline

import (
	"context"
	"errors"
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
	mutateUntil int
}

func (s *headMutatingStep) Name() types.StepName { return s.name }

func (s *headMutatingStep) Execute(sctx *StepContext) (*StepOutcome, error) {
	s.calls++
	if s.mutateEvery || (s.mutateFirst && s.calls == 1) || (s.mutateUntil > 0 && s.calls <= s.mutateUntil) {
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

type testFixHeadStep struct {
	calls        int
	mutateOnCall int
	failOnMutate bool
}

func (s *testFixHeadStep) Name() types.StepName { return types.StepTest }

func (s *testFixHeadStep) Execute(sctx *StepContext) (*StepOutcome, error) {
	s.calls++
	if s.calls == s.mutateOnCall {
		if err := sctx.DB.UpdateStepStatus(sctx.StepResultID, types.StepStatusFixing); err != nil {
			return nil, err
		}
		path := filepath.Join(sctx.WorkDir, fmt.Sprintf("test-fix-%d", s.calls))
		if err := os.WriteFile(path, []byte("test fix"), 0o644); err != nil {
			return nil, err
		}
		if _, err := git.Run(sctx.Ctx, sctx.WorkDir, "add", filepath.Base(path)); err != nil {
			return nil, err
		}
		if _, err := git.Run(sctx.Ctx, sctx.WorkDir, "commit", "-m", "fix configured test"); err != nil {
			return nil, err
		}
		head, err := git.HeadSHA(sctx.Ctx, sctx.WorkDir)
		if err != nil {
			return nil, err
		}
		if err := sctx.AdvanceHeadSHA(head); err != nil {
			return nil, err
		}
		if s.failOnMutate {
			return nil, errors.New("configured Test still fails")
		}
	}
	return &StepOutcome{TestedHeadSHA: sctx.Run.HeadSHA}, nil
}

func TestExecutor_InitialTestFixCommitNeedsNoReplay(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &testFixHeadStep{mutateOnCall: 1}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{
		testStep, newPassStep(types.StepDocument), newPassStep(types.StepLint),
	}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.ValidationReplayCount != 0 ||
		got.ValidationTargetSHA != nil || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("initial Test fix state = %#v", got)
	}
}

func TestExecutor_TestFixCommitRetargetsActiveReplay(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &testFixHeadStep{mutateOnCall: 2}
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{
		testStep, document, newPassStep(types.StepLint), newPassStep(types.StepPush),
	}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.ValidationReplayCount != 2 ||
		got.ValidationTargetSHA != nil || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("replay Test fix state = %#v", got)
	}
	if testStep.calls != 2 || document.calls != 2 {
		t.Fatalf("calls = Test %d Document %d, want 2/2", testStep.calls, document.calls)
	}
}

func TestExecutor_TestFixFailureKeepsRetargetedProofStale(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &testFixHeadStep{mutateOnCall: 2, failOnMutate: true}
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{
		testStep, document, newPassStep(types.StepLint), push,
	}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err == nil {
		t.Fatal("Execute() succeeded after configured Test fix failure")
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunFailed || got.ValidationTargetSHA == nil ||
		*got.ValidationTargetSHA != got.HeadSHA || got.ValidationReplayCount != 2 ||
		got.TestHeadSHA != nil || push.callCount() != 0 {
		t.Fatalf("failed Test retarget state = %#v, push calls %d", got, push.callCount())
	}
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

func TestExecutor_ReducedPipelineReplaysConfiguredTestAtFinalHead(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
	lint := &headMutatingStep{name: types.StepLint}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if testStep.calls != 2 || document.calls != 2 || lint.calls != 2 {
		t.Fatalf("calls = test %d document %d lint %d, want 2/2/2", testStep.calls, document.calls, lint.calls)
	}
	persisted, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != types.RunCompleted || persisted.TestHeadSHA == nil || *persisted.TestHeadSHA != persisted.HeadSHA {
		t.Fatalf("reduced pipeline proof = status %s proof %v head %s", persisted.Status, persisted.TestHeadSHA, persisted.HeadSHA)
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

func TestExecutor_ExactReplayBoundaryCanDeliverProvenCandidate(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	testStep := &proofRecordingTestStep{}
	document := &headMutatingStep{name: types.StepDocument, mutateUntil: 3}
	lint := newPassStep(types.StepLint)
	push := newPassStep(types.StepPush)
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, lint, push}, nil)

	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatal(err)
	}
	if testStep.calls != 4 || document.calls != 4 || lint.callCount() != 4 || push.callCount() != 1 {
		t.Fatalf(
			"boundary calls = Test %d Document %d Lint %d Push %d",
			testStep.calls, document.calls, lint.callCount(), push.callCount(),
		)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.ValidationReplayCount != 3 ||
		got.ValidationTargetSHA != nil || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("exact boundary run = %#v", got)
	}
}

func TestExecutor_DaemonShutdownDuringValidationReplayResumesIdempotently(t *testing.T) {
	for _, interruptedStep := range []types.StepName{types.StepTest, types.StepDocument, types.StepLint} {
		t.Run(string(interruptedStep), func(t *testing.T) {
			database, p, run, repo := setupTest(t)
			workDir := initExecutorGitRepo(t, database, run)
			ctx, cancel := context.WithCancelCause(context.Background())
			calls := map[types.StepName]int{}
			step := func(name types.StepName) Step {
				return &adaptiveCallStep{name: name, fn: func(sctx *StepContext) (*StepOutcome, error) {
					calls[name]++
					if name == interruptedStep && calls[name] == 2 {
						cancel(ErrDaemonShutdown)
						return nil, sctx.Ctx.Err()
					}
					outcome := &StepOutcome{}
					if name == types.StepTest {
						outcome.TestedHeadSHA = sctx.Run.HeadSHA
					}
					return outcome, nil
				}}
			}
			document := &headMutatingStep{name: types.StepDocument, mutateFirst: true}
			if interruptedStep == types.StepDocument {
				document = nil
			}
			steps := []Step{
				step(types.StepTest),
				step(types.StepDocument),
				step(types.StepLint),
				newPassStep(types.StepPush),
				newPassStep(types.StepPR),
				newPassStep(types.StepCI),
			}
			if document != nil {
				steps[1] = document
			} else {
				steps[1] = &adaptiveCallStep{name: types.StepDocument, fn: func(sctx *StepContext) (*StepOutcome, error) {
					calls[types.StepDocument]++
					if calls[types.StepDocument] == 1 {
						mutator := &headMutatingStep{name: types.StepDocument, mutateEvery: true}
						return mutator.Execute(sctx)
					}
					if calls[types.StepDocument] == 2 {
						cancel(ErrDaemonShutdown)
						return nil, sctx.Ctx.Err()
					}
					return &StepOutcome{}, nil
				}}
			}
			if interruptedStep != types.StepDocument {
				steps[1] = &adaptiveCallStep{name: types.StepDocument, fn: func(sctx *StepContext) (*StepOutcome, error) {
					calls[types.StepDocument]++
					if calls[types.StepDocument] == 1 {
						mutator := &headMutatingStep{name: types.StepDocument, mutateEvery: true}
						return mutator.Execute(sctx)
					}
					return &StepOutcome{}, nil
				}}
			}
			cfg := &config.Config{}
			cfg.Commands.Test = "configured-test-command"
			exec := NewExecutor(database, p, cfg, nil, steps, nil)

			err := exec.Execute(ctx, run, repo, workDir)
			if !errors.Is(err, ErrValidationRunInterrupted) {
				t.Fatalf("Execute() error = %v, want validation interruption", err)
			}
			interrupted, err := database.GetRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if interrupted.Status != types.RunRunning || interrupted.ValidationTargetSHA == nil || interrupted.ValidationReplayCount != 1 {
				t.Fatalf("interrupted run = status %s target %v count %d", interrupted.Status, interrupted.ValidationTargetSHA, interrupted.ValidationReplayCount)
			}

			resume := NewExecutor(database, p, cfg, nil, steps, nil)
			if err := resume.ResumeHeadValidation(context.Background(), interrupted, repo, workDir); err != nil {
				t.Fatalf("ResumeHeadValidation() error = %v", err)
			}
			completed, err := database.GetRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if completed.Status != types.RunCompleted || completed.ValidationReplayCount != 1 ||
				completed.TestHeadSHA == nil || *completed.TestHeadSHA != completed.HeadSHA {
				t.Fatalf("resumed run = status %s count %d proof %v head %s", completed.Status, completed.ValidationReplayCount, completed.TestHeadSHA, completed.HeadSHA)
			}
		})
	}
}

func TestExecutor_RecoversPersistedReplayThroughReducedTopology(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, run.HeadSHA); err != nil {
		t.Fatal(err)
	}
	testedHead := run.HeadSHA
	run.TestHeadSHA = &testedHead
	testStep := &proofRecordingTestStep{}
	steps := []Step{testStep, newPassStep(types.StepDocument), newPassStep(types.StepLint)}
	for _, step := range steps {
		result, err := database.InsertStepResult(run.ID, step.Name())
		if err != nil {
			t.Fatal(err)
		}
		switch step.Name() {
		case types.StepTest:
			if err := database.CompleteStep(result.ID, 0, 1, "test.log"); err != nil {
				t.Fatal(err)
			}
		case types.StepDocument:
			if err := database.StartStep(result.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(workDir, "reduced-recovery")
	if err := os.WriteFile(path, []byte("candidate"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), workDir, "add", filepath.Base(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), workDir, "commit", "-m", "advance reduced candidate"); err != nil {
		t.Fatal(err)
	}
	candidate, err := git.HeadSHA(context.Background(), workDir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	sctx := &StepContext{Ctx: context.Background(), Run: run, Repo: repo, WorkDir: workDir, Config: cfg, DB: database}
	if err := sctx.AdvanceHeadSHA(candidate); err != nil {
		t.Fatal(err)
	}
	if err := sctx.AdvanceHeadSHA(candidate); err != nil {
		t.Fatalf("idempotent head advance: %v", err)
	}

	interrupted, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.ValidationTargetSHA == nil || *interrupted.ValidationTargetSHA != candidate || interrupted.ValidationReplayCount != 1 {
		t.Fatalf("persisted boundary state = target %v count %d", interrupted.ValidationTargetSHA, interrupted.ValidationReplayCount)
	}
	if err := ValidateHeadValidationRecovery(database, interrupted, steps); err != nil {
		t.Fatalf("ValidateHeadValidationRecovery() error = %v", err)
	}
	exec := NewExecutor(database, p, cfg, nil, steps, nil)
	if err := exec.ResumeHeadValidation(context.Background(), interrupted, repo, workDir); err != nil {
		t.Fatalf("ResumeHeadValidation() error = %v", err)
	}
	completed, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != types.RunCompleted || completed.ValidationReplayCount != 1 ||
		completed.TestHeadSHA == nil || *completed.TestHeadSHA != completed.HeadSHA {
		t.Fatalf("reduced recovery = status %s count %d proof %v head %s", completed.Status, completed.ValidationReplayCount, completed.TestHeadSHA, completed.HeadSHA)
	}
}

func TestExecutor_DaemonShutdownDoesNotMaskIndependentReplayFailure(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := initExecutorGitRepo(t, database, run)
	ctx, cancel := context.WithCancelCause(context.Background())
	testStep := &proofRecordingTestStep{}
	documentCalls := 0
	document := &adaptiveCallStep{name: types.StepDocument, fn: func(sctx *StepContext) (*StepOutcome, error) {
		documentCalls++
		if documentCalls == 1 {
			mutator := &headMutatingStep{name: types.StepDocument, mutateEvery: true}
			return mutator.Execute(sctx)
		}
		cancel(ErrDaemonShutdown)
		return nil, errors.New("independent repository failure")
	}}
	cfg := &config.Config{}
	cfg.Commands.Test = "configured-test-command"
	exec := NewExecutor(database, p, cfg, nil, []Step{testStep, document, newPassStep(types.StepLint)}, nil)

	err := exec.Execute(ctx, run, repo, workDir)
	if err == nil || errors.Is(err, ErrValidationRunInterrupted) || !strings.Contains(err.Error(), "independent repository failure") {
		t.Fatalf("Execute() error = %v, want independent terminal failure", err)
	}
	failed, getErr := database.GetRun(run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if failed.Status != types.RunFailed || failed.Error == nil || !strings.Contains(*failed.Error, "independent repository failure") {
		t.Fatalf("failed replay run = status %s error %v", failed.Status, failed.Error)
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
