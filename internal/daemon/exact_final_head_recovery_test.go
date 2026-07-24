package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type exactRecoveryStep struct {
	name    types.StepName
	started chan struct{}
	release <-chan struct{}
}

func (s *exactRecoveryStep) Name() types.StepName { return s.name }

func (s *exactRecoveryStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-sctx.Ctx.Done():
			return nil, sctx.Ctx.Err()
		case <-s.release:
		}
	}
	return &pipeline.StepOutcome{}, nil
}

type exactRecoveryManagerFixture struct {
	t        *testing.T
	p        *paths.Paths
	d        *db.DB
	repo     *db.Repo
	run      *db.Run
	gate     string
	worktree string
	started  chan struct{}
	release  chan struct{}
	manager  *RunManager
}

func newExactRecoveryManagerFixture(t *testing.T) *exactRecoveryManagerFixture {
	t.Helper()
	root := t.TempDir()
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	source := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, source, "init", "--initial-branch=main")
	gitCmd(t, source, "config", "user.name", "Test")
	gitCmd(t, source, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("published\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, source, "add", ".")
	gitCmd(t, source, "commit", "-m", "published")
	published := gitOutput(t, source, "rev-parse", "HEAD")
	gitCmd(t, source, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(source, "candidate.txt"), []byte("exact candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, source, "add", ".")
	gitCmd(t, source, "commit", "-m", "exact candidate")
	head := gitOutput(t, source, "rev-parse", "HEAD")

	repo, err := d.InsertRepo(source, "file://"+source, "main")
	if err != nil {
		t.Fatal(err)
	}
	gate := p.RepoDir(repo.ID)
	gitCmd(t, "", "clone", "--bare", source, gate)
	run, err := d.InsertRun(repo.ID, "feature", head, published)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if count, err := d.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 1 {
		t.Fatalf("schedule replay 1: %d %v", count, err)
	}
	if err := d.UpdateRunHeadSHA(run.ID, published); err != nil {
		t.Fatal(err)
	}
	if count, err := d.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 2 {
		t.Fatalf("schedule replay 2: %d %v", count, err)
	}
	if err := d.UpdateRunHeadSHA(run.ID, head); err != nil {
		t.Fatal(err)
	}
	if count, err := d.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 3 {
		t.Fatalf("schedule replay 3: %d %v", count, err)
	}
	if err := d.RecordSuccessfulTestHead(run.ID, head); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPRURL(run.ID, "https://github.com/test/project/pull/42"); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPushBinding(run.ID, db.PushBinding{
		HeadSHA: published, TargetKind: "upstream", TargetFingerprint: "fingerprint", Ref: "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range types.AllSteps() {
		step, err := d.InsertStepResult(run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case name.Order() <= types.StepTest.Order():
			if err := d.CompleteStep(step.ID, 0, 1, string(name)+".log"); err != nil {
				t.Fatal(err)
			}
		case name == types.StepDocument:
			if err := d.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			if err := d.FailStep(step.ID, db.ExactFinalHeadCapacityStepError(3), 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := d.UpdateRunErrorStatus(run.ID, db.ExactFinalHeadCapacityRunError(3), types.RunFailed); err != nil {
		t.Fatal(err)
	}
	run, err = d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	stepFactory := func() []pipeline.Step {
		result := make([]pipeline.Step, 0, len(types.AllSteps()))
		for _, name := range types.AllSteps() {
			step := &exactRecoveryStep{name: name}
			if name == types.StepDocument {
				step.started = started
				step.release = release
			}
			result = append(result, step)
		}
		return result
	}
	manager := NewRunManager(d, p, stepFactory)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		manager.Shutdown()
	})
	return &exactRecoveryManagerFixture{
		t: t, p: p, d: d, repo: repo, run: run, gate: gate,
		worktree: p.WorktreeDir(repo.ID, run.ID), started: started, release: release, manager: manager,
	}
}

func installExactRecoveryManagerStubs(t *testing.T) *int {
	t.Helper()
	oldPrepare := prepareExactFinalHeadRecoveryRuntime
	oldExternal := validateExactFinalHeadRecoveryExternalState
	calls := 0
	prepareExactFinalHeadRecoveryRuntime = func(context.Context, *RunManager, *db.Run, *db.Repo, string) (*config.Config, agent.Agent, error) {
		cfg := &config.Config{}
		cfg.Commands.Test = "configured-test"
		return cfg, agent.NewNoop(), nil
	}
	validateExactFinalHeadRecoveryExternalState = func(context.Context, *db.Run, *db.Repo, string, *config.Config, bool) error {
		calls++
		return nil
	}
	t.Cleanup(func() {
		prepareExactFinalHeadRecoveryRuntime = oldPrepare
		validateExactFinalHeadRecoveryExternalState = oldExternal
	})
	return &calls
}

func TestHandleRecoverExactFinalHeadCapacityReconstructsAndResumesSameRun(t *testing.T) {
	f := newExactRecoveryManagerFixture(t)
	externalCalls := installExactRecoveryManagerStubs(t)
	if _, err := os.Lstat(f.worktree); !os.IsNotExist(err) {
		t.Fatalf("fixture worktree exists before recovery: %v", err)
	}

	runID, err := f.manager.HandleRecoverExactFinalHeadCapacity(context.Background(), f.repo.ID, f.run.ID)
	if err != nil {
		t.Fatalf("recover exact final-head capacity: %v", err)
	}
	if runID != f.run.ID {
		t.Fatalf("recovered run ID = %q, want %q", runID, f.run.ID)
	}
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovered Document step did not start")
	}
	if *externalCalls != 2 {
		t.Fatalf("external state checks = %d, want preflight and preclaim", *externalCalls)
	}
	if head, err := git.HeadSHA(context.Background(), f.worktree); err != nil || head != f.run.HeadSHA {
		t.Fatalf("reconstructed worktree head = %q, err %v", head, err)
	}
	if ref, err := git.ResolveRef(context.Background(), f.gate, "refs/heads/feature"); err != nil || ref != f.run.HeadSHA {
		t.Fatalf("gate source ref = %q, err %v", ref, err)
	}
	got, err := f.d.GetRun(f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunRunning || got.ID != f.run.ID || got.ValidationReplayCount != 3 || got.TestHeadSHA == nil || *got.TestHeadSHA != got.HeadSHA {
		t.Fatalf("active recovered run = %#v", got)
	}
	if again, err := f.manager.HandleRecoverExactFinalHeadCapacity(context.Background(), f.repo.ID, f.run.ID); err != nil || again != f.run.ID {
		t.Fatalf("idempotent active recovery = %q, %v", again, err)
	}
	close(f.release)
	waitRunStatus(t, f.d, f.run.ID, types.RunCompleted)
}

func TestHandleRecoverExactFinalHeadCapacityRefusesChangedExternalOrGateState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*exactRecoveryManagerFixture)
	}{
		{
			name: "stale published head",
			mutate: func(*exactRecoveryManagerFixture) {
				validateExactFinalHeadRecoveryExternalState = func(context.Context, *db.Run, *db.Repo, string, *config.Config, bool) error {
					return errors.New("published branch head changed")
				}
			},
		},
		{
			name: "stale PR identity",
			mutate: func(*exactRecoveryManagerFixture) {
				validateExactFinalHeadRecoveryExternalState = func(context.Context, *db.Run, *db.Repo, string, *config.Config, bool) error {
					return errors.New("stored PR identity changed")
				}
			},
		},
		{
			name: "mismatched source ref",
			mutate: func(f *exactRecoveryManagerFixture) {
				gitCmd(t, f.gate, "update-ref", "refs/heads/feature", f.run.BaseSHA, f.run.HeadSHA)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newExactRecoveryManagerFixture(t)
			installExactRecoveryManagerStubs(t)
			tc.mutate(f)
			if _, err := f.manager.HandleRecoverExactFinalHeadCapacity(context.Background(), f.repo.ID, f.run.ID); err == nil {
				t.Fatal("inconsistent recovery state was accepted")
			}
			got, err := f.d.GetRun(f.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != types.RunFailed || got.Error == nil || *got.Error != db.ExactFinalHeadCapacityRunError(3) {
				t.Fatalf("refusal changed terminal run: %#v", got)
			}
			event, err := f.d.GetRunRecoveryEvent(f.run.ID, db.RunRecoveryExactFinalHeadCapacity)
			if err != nil {
				t.Fatal(err)
			}
			if event != nil {
				t.Fatalf("refusal appended recovery event: %#v", event)
			}
			if _, err := os.Lstat(f.worktree); !os.IsNotExist(err) {
				t.Fatalf("refusal leaked reconstructed worktree: %v", err)
			}
		})
	}
}

func waitRunStatus(t *testing.T, d *db.DB, runID string, want types.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := d.GetRun(runID)
		if err == nil && run != nil && run.Status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	run, _ := d.GetRun(runID)
	t.Fatalf("run status = %v, want %s", run, want)
}
