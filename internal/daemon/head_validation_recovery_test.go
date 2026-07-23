package daemon

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type recoveredHeadValidationStep struct {
	name  types.StepName
	calls *atomic.Int32
	prURL string
}

func (s *recoveredHeadValidationStep) Name() types.StepName { return s.name }

func (s *recoveredHeadValidationStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	if s.calls != nil {
		s.calls.Add(1)
	}
	outcome := &pipeline.StepOutcome{}
	if s.name == types.StepTest {
		outcome.TestedHeadSHA = sctx.Run.HeadSHA
	}
	if s.name == types.StepPR {
		outcome.PRURL = s.prURL
	}
	return outcome, nil
}

func TestRecoverOnStartup_LegacyRunningCIReplaysFinalHeadProofInSameRun(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dtest-head-proof-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	p := paths.WithRoot(tmpDir)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	mockClaude := writeMockClaude(t, t.TempDir())
	if err := os.WriteFile(p.ConfigFile(), []byte("agent: claude\nagent_path_override:\n  claude: "+mockClaude+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	repo, _ := setupTestGitRepo(t, p, database, "legacy-running-ci-proof")
	trustedConfig := "auto_fix:\n  lint: 0\n  test: 0\n  review: 0\ncommands:\n  test: echo exact-final-head\n"
	if err := os.WriteFile(filepath.Join(repo.WorkingPath, ".no-mistakes.yaml"), []byte(trustedConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, repo.WorkingPath, "add", ".no-mistakes.yaml")
	gitCmd(t, repo.WorkingPath, "commit", "-m", "configure final-head test")
	headSHA := gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD")
	gitCmd(t, repo.WorkingPath, "push", "--force", "gate", "HEAD:refs/heads/main")
	gitCmd(t, repo.WorkingPath, "push", "--force", "gate", "HEAD:refs/heads/feature/test")

	run, err := database.InsertRun(repo.ID, "feature/test", headSHA, headSHA)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	const prURL = "https://github.com/test/repo/pull/1477"
	if err := database.UpdateRunPRURL(run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPushBinding(run.ID, db.PushBinding{
		HeadSHA:           headSHA,
		TargetKind:        "upstream",
		TargetFingerprint: "legacy-target",
		Ref:               "refs/heads/feature/test",
	}); err != nil {
		t.Fatal(err)
	}
	worktree := p.WorktreeDir(repo.ID, run.ID)
	if err := gitpkg.WorktreeAdd(context.Background(), p.RepoDir(repo.ID), worktree, headSHA); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(context.Background(), worktree, "update-ref", "refs/heads/feature/test", headSHA); err != nil {
		t.Fatal(err)
	}

	var testCalls, prCalls atomic.Int32
	steps := []pipeline.Step{
		&recoveredHeadValidationStep{name: types.StepIntent},
		&recoveredHeadValidationStep{name: types.StepRebase},
		&recoveredHeadValidationStep{name: types.StepReview},
		&recoveredHeadValidationStep{name: types.StepTest, calls: &testCalls},
		&recoveredHeadValidationStep{name: types.StepDocument},
		&recoveredHeadValidationStep{name: types.StepLint},
		&recoveredHeadValidationStep{name: types.StepPush},
		&recoveredHeadValidationStep{name: types.StepPR, calls: &prCalls, prURL: prURL},
		&recoveredHeadValidationStep{name: types.StepCI},
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunWithOptions(p, database, func() []pipeline.Step { return steps })
	}()
	defer func() {
		client, dialErr := ipc.Dial(p.Socket())
		if dialErr == nil {
			_ = client.Call(ipc.MethodShutdown, &ipc.ShutdownParams{}, nil)
			_ = client.Close()
		}
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("daemon exit: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	completed := waitForRunTerminalState(t, database, run.ID)
	if completed.Status != types.RunCompleted || completed.ID != run.ID {
		t.Fatalf("recovered run = id %s status %s, want same completed run", completed.ID, completed.Status)
	}
	if completed.TestHeadSHA == nil || *completed.TestHeadSHA != completed.HeadSHA {
		t.Fatalf("recovered final-head proof = %v, head = %s", completed.TestHeadSHA, completed.HeadSHA)
	}
	if completed.ValidationReplayCount != 1 {
		t.Fatalf("replay count = %d, want 1", completed.ValidationReplayCount)
	}
	if completed.PRURL == nil || *completed.PRURL != prURL || prCalls.Load() != 1 {
		t.Fatalf("PR identity = %v, replay PR calls = %d", completed.PRURL, prCalls.Load())
	}
	if testCalls.Load() != 1 {
		t.Fatalf("configured Test replay calls = %d, want 1", testCalls.Load())
	}
}
