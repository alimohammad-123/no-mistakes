package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const arenaInterruptedRunID = "01KY4FMNYM4AX8PA9MY92QMHN4"

type interruptedRecoveryFixture struct {
	p          *paths.Paths
	d          *db.DB
	manager    *RunManager
	repo       *db.Repo
	run        *db.Run
	gate       *db.StepResult
	worktree   string
	submitted  string
	pipeline   string
	intent     string
	findings   string
	operatorAt string
}

func newInterruptedRecoveryFixture(t *testing.T) *interruptedRecoveryFixture {
	t.Helper()
	root := t.TempDir()
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	mockClaude := writeMockClaude(t, t.TempDir())
	if err := os.WriteFile(p.ConfigFile(), []byte("agent: claude\nagent_path_override:\n  claude: "+mockClaude+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	repo, submitted := setupTestGitRepo(t, p, d, "legacy-interrupted-repo")
	branch := "fm/arena-no-mistakes-beta"
	gitCmd(t, p.RepoDir(repo.ID), "update-ref", "refs/heads/"+branch, submitted)

	run, err := d.InsertRunWithBaseBranch(repo.ID, branch, submitted, submitted, "main")
	if err != nil {
		t.Fatal(err)
	}
	legacyIDDB, err := sql.Open("sqlite", p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyIDDB.Exec(`UPDATE runs SET id = ? WHERE id = ?`, arenaInterruptedRunID, run.ID); err != nil {
		_ = legacyIDDB.Close()
		t.Fatal(err)
	}
	if err := legacyIDDB.Close(); err != nil {
		t.Fatal(err)
	}
	run.ID = arenaInterruptedRunID
	intent := "adopt the no-mistakes runtime without losing the canary"
	if err := d.UpdateRunIntent(run.ID, db.RunIntent{Summary: intent, Source: db.RunIntentSourceAgent, Score: 1}); err != nil {
		t.Fatal(err)
	}
	run.Intent = &intent
	source := db.RunIntentSourceAgent
	run.IntentSource = &source

	worktree := p.WorktreeDir(repo.ID, run.ID)
	if err := gitpkg.WorktreeAdd(context.Background(), p.RepoDir(repo.ID), worktree, submitted); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, worktree, "config", "user.name", "test")
	gitCmd(t, worktree, "config", "user.email", "test@example.com")
	for i, content := range []string{"review fix\n", "test candidate\n"} {
		if err := os.WriteFile(filepath.Join(worktree, "pipeline-fix.txt"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		gitCmd(t, worktree, "add", "pipeline-fix.txt")
		gitCmd(t, worktree, "commit", "-m", "pipeline fix "+string(rune('1'+i)))
	}
	pipelineHead, err := gitpkg.HeadSHA(context.Background(), worktree)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunHeadSHA(run.ID, pipelineHead); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, worktree, "update-ref", "refs/heads/"+branch, pipelineHead)
	run.HeadSHA = pipelineHead

	findings := `{"findings":[{"id":"test-1","severity":"error","description":"source-ref command summary","action":"auto-fix"}],"summary":"source-ref command summary"}`
	var gate *db.StepResult
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
			if err := d.CompleteStep(step.ID, 0, 17, filepath.Join(p.RunLogDir(run.ID), string(name)+".log")); err != nil {
				t.Fatal(err)
			}
		case types.StepTest:
			gate = step
			if err := d.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			if err := d.SetStepFindings(step.ID, findings); err != nil {
				t.Fatal(err)
			}
			if _, err := d.InsertStepRound(step.ID, 1, "initial", &findings, nil, 33); err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateStepStatusWithDuration(step.ID, types.StepStatusAwaitingApproval, 33); err != nil {
				t.Fatal(err)
			}
		}
	}
	if gate == nil {
		t.Fatal("missing Test gate")
	}
	if err := d.SetRunAwaitingAgent(run.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteRunAwaitingAgent(run.ID, 25); err != nil {
		t.Fatal(err)
	}
	if err := d.FailStep(gate.ID, db.LegacyDaemonShutdownError, 33); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunErrorStatus(run.ID, db.LegacyDaemonShutdownError, types.RunFailed); err != nil {
		t.Fatal(err)
	}
	legacyDB, err := sql.Open("sqlite", p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyDB.Exec(`UPDATE runs SET source_ref = NULL WHERE id = ?`, run.ID); err != nil {
		_ = legacyDB.Close()
		t.Fatal(err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatal(err)
	}
	run.Status = types.RunFailed
	run.SourceRef = nil
	runErr := db.LegacyDaemonShutdownError
	run.Error = &runErr

	stepsFactory := func() []pipeline.Step {
		out := make([]pipeline.Step, 0, len(types.AllSteps()))
		for _, name := range types.AllSteps() {
			out = append(out, &mockPassStep{name: name})
		}
		return out
	}
	manager := NewRunManager(d, p, stepsFactory)
	t.Cleanup(manager.Shutdown)
	return &interruptedRecoveryFixture{
		p: p, d: d, manager: manager, repo: repo, run: run, gate: gate,
		worktree: worktree, submitted: submitted, pipeline: pipelineHead, intent: intent, findings: findings,
		operatorAt: gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD"),
	}
}

func TestStartupCleanupPreservesExactLegacyInterruptedGate(t *testing.T) {
	f := newInterruptedRecoveryFixture(t)
	cleanupOrphanWorktrees(f.d, f.p)
	if _, err := os.Stat(f.worktree); err != nil {
		t.Fatalf("startup cleanup removed recoverable pipeline copy: %v", err)
	}
}

func TestHandleRecoverInterruptedGateRestoresSameRunAndResumes(t *testing.T) {
	f := newInterruptedRecoveryFixture(t)
	runID, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if err != nil {
		t.Fatal(err)
	}
	if !matched || runID != f.run.ID {
		t.Fatalf("recovery = %q, %v, want %q", runID, matched, f.run.ID)
	}

	restored, err := f.d.GetRun(f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status != types.RunRunning || restored.HeadSHA != f.pipeline || restored.SourceRef == nil || *restored.SourceRef != "refs/heads/"+f.run.Branch {
		t.Fatalf("restored run = %#v", restored)
	}
	step, err := f.d.GetStepResult(f.gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if step.Status != types.StepStatusAwaitingApproval || step.FindingsJSON == nil || *step.FindingsJSON != f.findings || step.DurationMS == nil || *step.DurationMS != 33 {
		t.Fatalf("restored gate = %#v", step)
	}
	if got := gitOutput(t, f.worktree, "rev-parse", "refs/heads/"+f.run.Branch); got != f.pipeline {
		t.Fatalf("pipeline source ref = %s, want %s", got, f.pipeline)
	}
	if got := gitOutput(t, f.repo.WorkingPath, "rev-parse", "HEAD"); got != f.operatorAt {
		t.Fatalf("operator branch moved from %s to %s", f.operatorAt, got)
	}
	runs, _ := f.d.GetRunsByRepo(f.repo.ID)
	if len(runs) != 1 {
		t.Fatalf("run count = %d, want 1", len(runs))
	}

	secondID, secondMatched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if err != nil || !secondMatched || secondID != f.run.ID {
		t.Fatalf("idempotent reattach = %q, %v, %v", secondID, secondMatched, err)
	}
	rounds, _ := f.d.GetRoundsByStep(f.gate.ID)
	if len(rounds) != 1 {
		t.Fatalf("second reattach duplicated rounds: %d", len(rounds))
	}

	respondDeadline := time.Now().Add(3 * time.Second)
	for {
		err := f.manager.HandleRespond(f.run.ID, types.StepTest, types.ActionApprove, nil)
		if err == nil {
			break
		}
		if time.Now().After(respondDeadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	completed := waitForRunTerminalState(t, f.d, f.run.ID)
	if completed.Status != types.RunCompleted {
		t.Fatalf("status after response = %s, want completed", completed.Status)
	}
	f.manager.Shutdown()
}

func TestHandleRecoverInterruptedGateRefusesUnsafePipelineCopy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *interruptedRecoveryFixture)
		want   string
	}{
		{name: "missing", mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
			if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
				t.Fatal(err)
			}
		}, want: "worktree is missing"},
		{name: "dirty", mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
			if err := os.WriteFile(filepath.Join(f.worktree, "dirty.txt"), []byte("dirty"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, want: "worktree is dirty"},
		{name: "head mismatch", mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
			gitCmd(t, f.worktree, "checkout", "--detach", f.submitted)
		}, want: "worktree head does not match"},
		{name: "source ref moved", mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
			gitCmd(t, f.worktree, "update-ref", "refs/heads/"+f.run.Branch, f.submitted)
		}, want: "pipeline source ref does not match"},
		{name: "in-progress git operation", mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
			marker, err := gitpkg.Run(context.Background(), f.worktree, "rev-parse", "--git-path", "MERGE_HEAD")
			if err != nil {
				t.Fatal(err)
			}
			if !filepath.IsAbs(marker) {
				marker = filepath.Join(f.worktree, marker)
			}
			if err := os.WriteFile(marker, []byte(f.submitted+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, want: "in-progress Git operation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInterruptedRecoveryFixture(t)
			tc.mutate(t, f)
			before, _ := f.d.GetRun(f.run.ID)
			beforeStep, _ := f.d.GetStepResult(f.gate.ID)
			_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			if !matched || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("matched=%v err=%v, want %q", matched, err, tc.want)
			}
			after, _ := f.d.GetRun(f.run.ID)
			afterStep, _ := f.d.GetStepResult(f.gate.ID)
			if !reflect.DeepEqual(after, before) || !reflect.DeepEqual(afterStep, beforeStep) {
				t.Fatalf("refusal mutated state: run before=%#v after=%#v; step before=%#v after=%#v", before, after, beforeStep, afterStep)
			}
		})
	}
}

func TestHandleRecoverInterruptedGateRefusesMismatchedIdentityAndIntent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		repoID    func(*interruptedRecoveryFixture) string
		branch    func(*interruptedRecoveryFixture) string
		head      func(*interruptedRecoveryFixture) string
		intent    func(*interruptedRecoveryFixture) string
		wantMatch bool
	}{
		{name: "repository", repoID: func(*interruptedRecoveryFixture) string { return "other" }, wantMatch: false},
		{name: "branch", branch: func(*interruptedRecoveryFixture) string { return "other" }, wantMatch: false},
		{name: "head", head: func(f *interruptedRecoveryFixture) string { return f.pipeline[:12] }, wantMatch: true},
		{name: "pipeline head is not submitted head", head: func(f *interruptedRecoveryFixture) string { return f.pipeline }, wantMatch: true},
		{name: "intent", intent: func(*interruptedRecoveryFixture) string { return "different intent" }, wantMatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInterruptedRecoveryFixture(t)
			repoID, branch, head, intent := f.repo.ID, f.run.Branch, f.submitted, f.intent
			if tc.repoID != nil {
				repoID = tc.repoID(f)
			}
			if tc.branch != nil {
				branch = tc.branch(f)
			}
			if tc.head != nil {
				head = tc.head(f)
			}
			if tc.intent != nil {
				intent = tc.intent(f)
			}
			_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), repoID, branch, head, intent)
			if matched != tc.wantMatch || (matched && err == nil) {
				t.Fatalf("matched=%v err=%v", matched, err)
			}
			after, _ := f.d.GetRun(f.run.ID)
			if after.Status != types.RunFailed {
				t.Fatalf("identity refusal mutated run: %#v", after)
			}
		})
	}
}

func TestParkedFixReviewRestartDoesNotDeleteDirtyPipelineCopy(t *testing.T) {
	f := newInterruptedRecoveryFixture(t)
	legacyDB, err := sql.Open("sqlite", f.p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyDB.Exec(`UPDATE step_rounds SET trigger_type = 'auto_fix' WHERE step_result_id = ?`, f.gate.ID); err != nil {
		_ = legacyDB.Close()
		t.Fatal(err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent); err != nil {
		t.Fatal(err)
	}
	dirtyPath := filepath.Join(f.worktree, "uncommitted-fix-review.txt")
	if err := os.WriteFile(dirtyPath, []byte("preserve me\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.manager.Shutdown()

	manager2 := NewRunManager(f.d, f.p, f.manager.steps)
	t.Cleanup(manager2.Shutdown)
	recoverOnStartup(f.d, f.p, manager2)
	recovered, err := f.d.GetRun(f.run.ID)
	if err != nil || recovered.Status != types.RunRunning || recovered.AwaitingAgentSince == nil {
		t.Fatalf("dirty parked gate was not resumed: %#v, %v", recovered, err)
	}
	recoveredGate, err := f.d.GetStepResult(f.gate.ID)
	if err != nil || recoveredGate.Status != types.StepStatusFixReview {
		t.Fatalf("dirty recovered gate = %#v, %v", recoveredGate, err)
	}
	if data, err := os.ReadFile(dirtyPath); err != nil || string(data) != "preserve me\n" {
		t.Fatalf("dirty fix-review evidence was lost: %q, %v", data, err)
	}
}

func TestRecoveredGateSurvivesManagerRestart(t *testing.T) {
	f := newInterruptedRecoveryFixture(t)
	if _, _, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent); err != nil {
		t.Fatal(err)
	}
	f.manager.Shutdown()

	parked, err := f.d.GetRun(f.run.ID)
	if err != nil || parked.Status != types.RunRunning || parked.AwaitingAgentSince == nil {
		t.Fatalf("recovered gate was not durable after shutdown: %#v, %v", parked, err)
	}
	roundsBefore, _ := f.d.GetRoundsByStep(f.gate.ID)
	manager2 := NewRunManager(f.d, f.p, f.manager.steps)
	t.Cleanup(manager2.Shutdown)
	recoverOnStartup(f.d, f.p, manager2)
	if runID, matched, err := manager2.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent); err != nil || !matched || runID != f.run.ID {
		t.Fatalf("reattach after restart = %q, %v, %v", runID, matched, err)
	}
	roundsAfter, _ := f.d.GetRoundsByStep(f.gate.ID)
	if len(roundsAfter) != len(roundsBefore) || roundsAfter[0].ID != roundsBefore[0].ID {
		t.Fatalf("restart changed rounds: before=%#v after=%#v", roundsBefore, roundsAfter)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := manager2.HandleRespond(f.run.ID, types.StepTest, types.ActionApprove, nil)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	completed := waitForRunTerminalState(t, f.d, f.run.ID)
	if completed.Status != types.RunCompleted || completed.HeadSHA != f.pipeline {
		t.Fatalf("restart response completed %#v", completed)
	}
}
