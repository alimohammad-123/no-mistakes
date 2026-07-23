package steps

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type exactBoundaryTestStep struct {
	calls int
}

func (s *exactBoundaryTestStep) Name() types.StepName { return types.StepTest }

func (s *exactBoundaryTestStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	s.calls++
	return &pipeline.StepOutcome{TestedHeadSHA: sctx.Run.HeadSHA}, nil
}

type exactBoundaryPassStep struct {
	name  types.StepName
	calls int
}

func (s *exactBoundaryPassStep) Name() types.StepName { return s.name }

func (s *exactBoundaryPassStep) Execute(*pipeline.StepContext) (*pipeline.StepOutcome, error) {
	s.calls++
	return &pipeline.StepOutcome{}, nil
}

type exactBoundaryFixture struct {
	database *db.DB
	run      *db.Run
	repo     *db.Repo
	workDir  string
	baseSHA  string
	test     *exactBoundaryTestStep
	lint     *exactBoundaryPassStep
	push     *exactBoundaryPassStep
	pr       *exactBoundaryPassStep
	ci       *exactBoundaryPassStep
	calls    int
	exec     *pipeline.Executor
}

func newExactBoundaryFixture(t *testing.T, boundaryCall func(context.Context, agent.RunOpts) (*agent.Result, error)) *exactBoundaryFixture {
	t.Helper()
	workDir, baseSHA, headSHA := setupGitRepo(t)
	database, err := db.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repo, err := database.InsertRepo(workDir, "https://github.com/test/repo", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature", headSHA, baseSHA)
	if err != nil {
		t.Fatal(err)
	}

	fixture := &exactBoundaryFixture{
		database: database,
		run:      run,
		repo:     repo,
		workDir:  workDir,
		baseSHA:  baseSHA,
		test:     &exactBoundaryTestStep{},
		lint:     &exactBoundaryPassStep{name: types.StepLint},
		push:     &exactBoundaryPassStep{name: types.StepPush},
		pr:       &exactBoundaryPassStep{name: types.StepPR},
		ci:       &exactBoundaryPassStep{name: types.StepCI},
	}
	ag := &mockAgent{
		name: "boundary-document-agent",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			fixture.calls++
			if fixture.calls <= 3 {
				path := filepath.Join(opts.CWD, "docs-boundary-"+string(rune('0'+fixture.calls))+".md")
				if err := os.WriteFile(path, []byte("documented\n"), 0o644); err != nil {
					return nil, err
				}
				return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"update boundary docs"}`)}, nil
			}
			return boundaryCall(ctx, opts)
		},
	}
	cfg := &config.Config{Agent: types.AgentClaude}
	cfg.Commands.Test = "configured-test"
	cfg.Commands.Lint = "true"
	fixture.exec = pipeline.NewExecutor(database, paths.WithRoot(t.TempDir()), cfg, ag, []pipeline.Step{
		fixture.test,
		&DocumentStep{},
		fixture.lint,
		fixture.push,
		fixture.pr,
		fixture.ci,
	}, nil)
	return fixture
}

func (f *exactBoundaryFixture) originalState(t *testing.T) (head, sourceRef, status string) {
	t.Helper()
	head, err := git.HeadSHA(context.Background(), f.workDir)
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err = git.ResolveRef(context.Background(), f.workDir, "refs/heads/feature")
	if err != nil {
		t.Fatal(err)
	}
	status, err = git.Run(context.Background(), f.workDir, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	return head, sourceRef, status
}

func assertNoDocumentAssessmentCopy(t *testing.T, workDir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(workDir), ".no-mistakes-document-assessment-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("boundary assessment copies leaked: %v", matches)
	}
}

func TestPipeline_ProductionDocumentNoOpAtExactReplayBoundaryDelivers(t *testing.T) {
	f := newExactBoundaryFixture(t, func(context.Context, agent.RunOpts) (*agent.Result, error) {
		return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"documentation current"}`)}, nil
	})

	if err := f.exec.Execute(context.Background(), f.run, f.repo, f.workDir); err != nil {
		t.Fatalf("execute exact-boundary no-op: %v", err)
	}
	got, err := f.database.GetRun(f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.ValidationReplayCount != 3 || got.ValidationTargetSHA != nil || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("completed exact-boundary run = %#v", got)
	}
	if f.calls != 4 || f.test.calls != 4 || f.lint.calls != 4 || f.push.calls != 1 || f.pr.calls != 1 || f.ci.calls != 1 {
		t.Fatalf("calls: document=%d test=%d lint=%d push=%d pr=%d ci=%d", f.calls, f.test.calls, f.lint.calls, f.push.calls, f.pr.calls, f.ci.calls)
	}
	if status := gitStatusPorcelain(t, f.workDir); status != "" {
		t.Fatalf("delivered worktree is dirty: %q", status)
	}
	assertNoDocumentAssessmentCopy(t, f.workDir)
}

func TestPipeline_ProductionDocumentProposalAtExactReplayBoundaryFailsWithoutMutation(t *testing.T) {
	f := newExactBoundaryFixture(t, func(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
		if err := os.WriteFile(filepath.Join(opts.CWD, "README.md"), []byte("proposed boundary change\n"), 0o644); err != nil {
			return nil, err
		}
		return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"change boundary docs"}`)}, nil
	})

	err := f.exec.Execute(context.Background(), f.run, f.repo, f.workDir)
	if err == nil || !strings.Contains(err.Error(), db.ErrHeadValidationMutationExhausted.Error()) || !strings.Contains(err.Error(), "README.md") {
		t.Fatalf("execute exact-boundary proposal error = %v", err)
	}
	got, getErr := f.database.GetRun(f.run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.Status != types.RunFailed || got.ValidationReplayCount != 3 || got.ValidationTargetSHA == nil || *got.ValidationTargetSHA != got.HeadSHA || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("failed exact-boundary run = %#v", got)
	}
	head, sourceRef, status := f.originalState(t)
	if head != got.HeadSHA || sourceRef != got.HeadSHA || status != "" {
		t.Fatalf("candidate changed: head=%s source=%s status=%q run=%s", head, sourceRef, status, got.HeadSHA)
	}
	if pending, pendingErr := f.database.GetRunHeadTransition(f.run.ID); pendingErr != nil || pending != nil {
		t.Fatalf("head transition = %#v, err %v", pending, pendingErr)
	}
	steps, stepsErr := f.database.GetStepsByRun(f.run.ID)
	if stepsErr != nil {
		t.Fatal(stepsErr)
	}
	for _, step := range steps {
		if step.StepName == types.StepDocument {
			rounds, roundsErr := f.database.GetRoundsByStep(step.ID)
			if roundsErr != nil {
				t.Fatal(roundsErr)
			}
			if step.Status != types.StepStatusFailed || len(rounds) != 3 {
				t.Fatalf("document state = %s with %d rounds", step.Status, len(rounds))
			}
		}
	}
	if f.push.calls != 0 || f.pr.calls != 0 || f.ci.calls != 0 {
		t.Fatalf("delivery ran after boundary proposal: push=%d pr=%d ci=%d", f.push.calls, f.pr.calls, f.ci.calls)
	}
	assertNoDocumentAssessmentCopy(t, f.workDir)
}

func TestPipeline_ProductionDocumentBoundaryAssessmentCleansUpOnErrorAndCancellation(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(context.Context, context.CancelCauseFunc, agent.RunOpts) (*agent.Result, error)
		status types.RunStatus
	}{
		{
			name: "agent error",
			invoke: func(_ context.Context, _ context.CancelCauseFunc, opts agent.RunOpts) (*agent.Result, error) {
				if err := os.WriteFile(filepath.Join(opts.CWD, "error-proposal.md"), []byte("temporary\n"), 0o644); err != nil {
					return nil, err
				}
				return nil, errors.New("assessment agent failed")
			},
			status: types.RunFailed,
		},
		{
			name: "cancellation",
			invoke: func(ctx context.Context, cancel context.CancelCauseFunc, opts agent.RunOpts) (*agent.Result, error) {
				if err := os.WriteFile(filepath.Join(opts.CWD, "cancelled-proposal.md"), []byte("temporary\n"), 0o644); err != nil {
					return nil, err
				}
				cancel(context.Canceled)
				return nil, ctx.Err()
			},
			status: types.RunCancelled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			f := newExactBoundaryFixture(t, func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
				return tc.invoke(ctx, cancel, opts)
			})
			if err := f.exec.Execute(ctx, f.run, f.repo, f.workDir); err == nil {
				t.Fatal("boundary assessment unexpectedly succeeded")
			}
			got, err := f.database.GetRun(f.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.status || got.ValidationReplayCount != 3 || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
				t.Fatalf("terminal assessment run = %#v", got)
			}
			head, sourceRef, status := f.originalState(t)
			if head != got.HeadSHA || sourceRef != got.HeadSHA || status != "" {
				t.Fatalf("candidate changed: head=%s source=%s status=%q run=%s", head, sourceRef, status, got.HeadSHA)
			}
			assertNoDocumentAssessmentCopy(t, f.workDir)
		})
	}
}
