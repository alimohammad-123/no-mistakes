package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
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
	return newInterruptedRecoveryFixtureForRepo(t, "legacy-interrupted-repo")
}

func newInterruptedRecoveryFixtureForRepo(t *testing.T, repoID string) *interruptedRecoveryFixture {
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
	repo, submitted := setupTestGitRepo(t, p, d, repoID)
	if repoID == arenaMissingWorktreeRepoID {
		gitCmd(t, repo.WorkingPath, "remote", "add", "origin", repo.UpstreamURL)
		if err := gitpkg.IsolateHooksPath(context.Background(), p.RepoDir(repo.ID)); err != nil {
			t.Fatal(err)
		}
	}
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
	if repoID == arenaMissingWorktreeRepoID {
		gitCmd(t, worktree, "config", "--worktree", "user.name", "Test")
		gitCmd(t, worktree, "config", "--worktree", "user.email", "test@test.com")
	} else {
		gitCmd(t, worktree, "config", "user.name", "test")
		gitCmd(t, worktree, "config", "user.email", "test@example.com")
	}
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

func allowSyntheticInterruptedWorktreeReconstruction(t *testing.T) {
	t.Helper()
	oldMatcher := matchInterruptedWorktreeIncident
	oldExternal := probeInterruptedExternalState
	oldProcess := probeInterruptedProcessOwners
	oldPoint := interruptedReconstructionPoint
	oldAdd := addInterruptedWorktree
	oldRegisteredPath := validateInterruptedRegisteredWorkingPath
	matchInterruptedWorktreeIncident = func(*db.Repo, *db.Run, *db.StepResult) error { return nil }
	probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error { return nil }
	probeInterruptedProcessOwners = func(context.Context, string, string) error { return nil }
	interruptedReconstructionPoint = func(string) error { return nil }
	addInterruptedWorktree = gitpkg.WorktreeAdd
	validateInterruptedRegisteredWorkingPath = validateRegisteredWorkingPath
	t.Cleanup(func() {
		matchInterruptedWorktreeIncident = oldMatcher
		probeInterruptedExternalState = oldExternal
		probeInterruptedProcessOwners = oldProcess
		interruptedReconstructionPoint = oldPoint
		addInterruptedWorktree = oldAdd
		validateInterruptedRegisteredWorkingPath = oldRegisteredPath
	})
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

func TestHandleRecoverInterruptedGateReconstructsExactMissingWorktree(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := f.d.UpsertRunAgentSession(f.run.ID, string(pipeline.SessionRoleReviewer), "claude", "reviewer-session"); err != nil {
		t.Fatal(err)
	}
	if err := f.d.UpsertRunAgentSession(f.run.ID, string(pipeline.SessionRoleFixer), "claude", "fixer-session"); err != nil {
		t.Fatal(err)
	}
	roundsBefore, _ := f.d.GetRoundsByStep(f.gate.ID)
	sessionsBefore, _ := f.d.GetRunAgentSessions(f.run.ID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}

	runID, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if err != nil || !matched || runID != f.run.ID {
		t.Fatalf("reconstruction = %q, %v, %v", runID, matched, err)
	}
	restored, _ := f.d.GetRun(f.run.ID)
	if restored.Status != types.RunRunning || restored.HeadSHA != f.pipeline || restored.SourceRef == nil || *restored.SourceRef != "refs/heads/"+f.run.Branch {
		t.Fatalf("restored run = %#v", restored)
	}
	if head := gitOutput(t, f.worktree, "rev-parse", "HEAD"); head != f.pipeline {
		t.Fatalf("reconstructed HEAD = %s, want %s", head, f.pipeline)
	}
	sourceIdentity, err := gitpkg.ReadLocalUserIdentity(context.Background(), f.repo.WorkingPath)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := gitpkg.ReadWorktreeUserIdentity(context.Background(), f.worktree)
	if err != nil || targetIdentity != sourceIdentity {
		t.Fatalf("reconstructed identity = %#v, %v, want %#v", targetIdentity, err, sourceIdentity)
	}
	status, err := gitpkg.Run(context.Background(), f.worktree, "status", "--porcelain=v1")
	if err != nil || status != "" {
		t.Fatalf("reconstructed worktree status = %q, %v", status, err)
	}
	common := gitOutput(t, f.worktree, "rev-parse", "--git-common-dir")
	if !samePath(resolveGitPath(f.worktree, common), f.p.RepoDir(f.repo.ID)) {
		t.Fatalf("reconstructed common dir = %s", common)
	}
	if _, err := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)); !os.IsNotExist(err) {
		t.Fatalf("admitted journal still exists: %v", err)
	}
	roundsAfter, _ := f.d.GetRoundsByStep(f.gate.ID)
	sessionsAfter, _ := f.d.GetRunAgentSessions(f.run.ID)
	if !reflect.DeepEqual(roundsAfter, roundsBefore) || !reflect.DeepEqual(sessionsAfter, sessionsBefore) {
		t.Fatalf("reconstruction changed rounds or sessions")
	}
	secondID, secondMatched, secondErr := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if secondErr != nil || !secondMatched || secondID != f.run.ID {
		t.Fatalf("duplicate reattach = %q, %v, %v", secondID, secondMatched, secondErr)
	}
}

func TestMissingWorktreeReconstructionCopiesNoInventedIdentity(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	for _, key := range []string{"user.name", "user.email"} {
		if _, err := gitpkg.Run(context.Background(), f.repo.WorkingPath, "config", "--local", "--unset-all", key); err != nil {
			t.Fatal(err)
		}
	}
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent); err != nil || !matched {
		t.Fatalf("identity-free reconstruction = %v, %v", matched, err)
	}
	identity, err := gitpkg.ReadWorktreeUserIdentity(context.Background(), f.worktree)
	if err != nil || identity != (gitpkg.LocalUserIdentity{}) {
		t.Fatalf("reconstruction invented identity: %#v, %v", identity, err)
	}
}

func TestMissingWorktreeReconstructionConcurrentReattachmentReturnsSameRun(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	type result struct {
		id      string
		matched bool
		err     error
	}
	results := make(chan result, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			ready.Done()
			<-start
			id, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			results <- result{id: id, matched: matched, err: err}
		}()
	}
	ready.Wait()
	close(start)
	for range 2 {
		got := <-results
		if got.err != nil || !got.matched || got.id != f.run.ID {
			t.Fatalf("concurrent reattach = %#v", got)
		}
	}
	runs, _ := f.d.GetRunsByRepo(f.repo.ID)
	if len(runs) != 1 {
		t.Fatalf("concurrent reattachment created %d runs", len(runs))
	}
}

func TestMissingWorktreeReconstructionRollsBackPreClaimFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*testing.T, *interruptedRecoveryFixture)
	}{
		{
			name: "trusted config unavailable",
			prepare: func(t *testing.T, f *interruptedRecoveryFixture) {
				if err := os.WriteFile(f.p.ConfigFile(), []byte("[invalid"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "external state changed before claim",
			prepare: func(t *testing.T, _ *interruptedRecoveryFixture) {
				calls := 0
				probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error {
					calls++
					if calls > 1 {
						return fmt.Errorf("candidate branch appeared")
					}
					return nil
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowSyntheticInterruptedWorktreeReconstruction(t)
			f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
			if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
				t.Fatal(err)
			}
			before, _ := f.d.GetRun(f.run.ID)
			beforeStep, _ := f.d.GetStepResult(f.gate.ID)
			tc.prepare(t, f)
			_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			if !matched || err == nil {
				t.Fatalf("matched=%v err=%v", matched, err)
			}
			after, _ := f.d.GetRun(f.run.ID)
			afterStep, _ := f.d.GetStepResult(f.gate.ID)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeStep, afterStep) {
				t.Fatalf("pre-claim refusal mutated run or gate")
			}
			if _, err := os.Lstat(f.worktree); !os.IsNotExist(err) {
				t.Fatalf("journal-owned worktree survived rollback: %v", err)
			}
			if _, err := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)); !os.IsNotExist(err) {
				t.Fatalf("journal survived rollback: %v", err)
			}
		})
	}
}

func TestMissingWorktreeReconstructionResumesJournalAfterCrash(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	interruptedReconstructionPoint = func(point string) error {
		if point == "worktree-added" {
			panic("simulated reconstruction crash")
		}
		return nil
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("simulated crash did not panic")
			}
		}()
		_, _, _ = f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	}()
	if _, err := os.Lstat(f.worktree); err != nil {
		t.Fatalf("crash did not preserve its completed new worktree: %v", err)
	}
	journalDir := f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)
	journalInfo, err := os.Lstat(journalDir)
	if err != nil {
		t.Fatalf("crash did not preserve ownership journal: %v", err)
	}
	if journalInfo.Mode().Perm() != 0o700 {
		t.Fatalf("journal mode = %o, want 700", journalInfo.Mode().Perm())
	}
	manifestPath := filepath.Join(journalDir, interruptedJournalManifestName)
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil || manifestInfo.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode = %v, %v, want 600", manifestInfo, err)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{f.intent, f.findings, "session"} {
		if strings.Contains(string(manifestBytes), private) {
			t.Fatalf("journal leaked private recovery content %q", private)
		}
	}
	interruptedReconstructionPoint = func(string) error { return nil }
	runID, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if err != nil || !matched || runID != f.run.ID {
		t.Fatalf("journal resume = %q, %v, %v", runID, matched, err)
	}
}

func TestMissingWorktreeReconstructionStartupReconcilesCrashPhases(t *testing.T) {
	for _, tc := range []struct {
		name            string
		crashPoint      string
		wantTarget      bool
		wantRunning     bool
		wantJournalGone bool
	}{
		{name: "journal only", crashPoint: "journal-created", wantTarget: false, wantRunning: false, wantJournalGone: true},
		{name: "completed worktree before registration journal", crashPoint: "worktree-added", wantTarget: true, wantRunning: false, wantJournalGone: false},
		{name: "registration before identity", crashPoint: "registration-recorded", wantTarget: true, wantRunning: false, wantJournalGone: false},
		{name: "identity before config", crashPoint: "identity-restored", wantTarget: true, wantRunning: false, wantJournalGone: false},
		{name: "database claimed before admission", crashPoint: "database-claimed", wantTarget: true, wantRunning: true, wantJournalGone: true},
		{name: "executor admitted before journal deletion", crashPoint: "executor-admitted", wantTarget: true, wantRunning: true, wantJournalGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowSyntheticInterruptedWorktreeReconstruction(t)
			f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
			if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
				t.Fatal(err)
			}
			interruptedReconstructionPoint = func(point string) error {
				if point == tc.crashPoint {
					panic("simulated reconstruction crash at " + point)
				}
				return nil
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("simulated crash did not panic")
					}
				}()
				_, _, _ = f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			}()
			interruptedReconstructionPoint = func(string) error { return nil }
			if tc.crashPoint == "executor-admitted" {
				f.manager.mu.Lock()
				delete(f.manager.executors, f.run.ID)
				delete(f.manager.cancels, f.run.ID)
				delete(f.manager.dones, f.run.ID)
				f.manager.mu.Unlock()
				f.manager.wg.Done()
			}

			manager2 := NewRunManager(f.d, f.p, f.manager.steps)
			t.Cleanup(manager2.Shutdown)
			if tc.wantRunning {
				recoverOnStartup(f.d, f.p, manager2)
			} else {
				manager2.reconcileInterruptedWorktreeJournal(context.Background())
			}
			_, targetErr := os.Lstat(f.worktree)
			if tc.wantTarget && targetErr != nil {
				t.Fatalf("target missing after reconciliation: %v", targetErr)
			}
			if !tc.wantTarget && !os.IsNotExist(targetErr) {
				t.Fatalf("unexpected target after reconciliation: %v", targetErr)
			}
			_, journalErr := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID))
			if tc.wantJournalGone && !os.IsNotExist(journalErr) {
				t.Fatalf("journal survived reconciliation: %v", journalErr)
			}
			if !tc.wantJournalGone && journalErr != nil {
				t.Fatalf("journal was not preserved: %v", journalErr)
			}
			run, _ := f.d.GetRun(f.run.ID)
			if tc.wantRunning && (run.Status != types.RunRunning || run.AwaitingAgentSince == nil) {
				t.Fatalf("post-claim restart did not preserve same parked run: %#v", run)
			}
			if !tc.wantRunning && run.Status != types.RunFailed {
				t.Fatalf("pre-claim crash changed run: %#v", run)
			}
		})
	}
}

func TestMissingWorktreeReconstructionStartupClearsOnlyEmptyOrValidJournals(t *testing.T) {
	t.Run("empty marker before manifest", func(t *testing.T) {
		allowSyntheticInterruptedWorktreeReconstruction(t)
		f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
		if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
			t.Fatal(err)
		}
		dir := f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		f.manager.reconcileInterruptedWorktreeJournal(context.Background())
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Fatalf("empty marker survived deterministic reconciliation: %v", err)
		}
	})

	t.Run("invalid manifest preserves target", func(t *testing.T) {
		allowSyntheticInterruptedWorktreeReconstruction(t)
		f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
		if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
			t.Fatal(err)
		}
		interruptedReconstructionPoint = func(point string) error {
			if point == "worktree-added" {
				panic("simulated crash")
			}
			return nil
		}
		func() {
			defer func() { _ = recover() }()
			_, _, _ = f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
		}()
		manifest := filepath.Join(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID), interruptedJournalManifestName)
		if err := os.WriteFile(manifest, []byte(`{"invalid":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		manager2 := NewRunManager(f.d, f.p, f.manager.steps)
		t.Cleanup(manager2.Shutdown)
		recoverOnStartup(f.d, f.p, manager2)
		if _, err := os.Lstat(f.worktree); err != nil {
			t.Fatalf("invalid journal target was removed: %v", err)
		}
		run, _ := f.d.GetRun(f.run.ID)
		if run.Status != types.RunFailed || run.Error == nil || *run.Error != db.LegacyDaemonShutdownError {
			t.Fatalf("invalid journal changed legacy run: %#v", run)
		}
	})

	t.Run("invalid post-claim manifest preserves parked run", func(t *testing.T) {
		allowSyntheticInterruptedWorktreeReconstruction(t)
		f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
		if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
			t.Fatal(err)
		}
		interruptedReconstructionPoint = func(point string) error {
			if point == "database-claimed" {
				panic("simulated crash")
			}
			return nil
		}
		func() {
			defer func() { _ = recover() }()
			_, _, _ = f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
		}()
		manifest := filepath.Join(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID), interruptedJournalManifestName)
		if err := os.WriteFile(manifest, []byte(`{"invalid":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		manager2 := NewRunManager(f.d, f.p, f.manager.steps)
		t.Cleanup(manager2.Shutdown)
		recoverOnStartup(f.d, f.p, manager2)
		if _, err := os.Lstat(f.worktree); err != nil {
			t.Fatalf("invalid post-claim journal target was removed: %v", err)
		}
		run, _ := f.d.GetRun(f.run.ID)
		if run.Status != types.RunRunning || run.AwaitingAgentSince == nil {
			t.Fatalf("ambiguous post-claim run was terminalized: %#v", run)
		}
	})
}

func TestMissingWorktreeReconstructionRefusesPathAndTopologyAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *interruptedRecoveryFixture)
	}{
		{
			name: "pre-existing directory",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				if err := os.MkdirAll(f.worktree, 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "pre-existing file",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				if err := os.MkdirAll(filepath.Dir(f.worktree), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.worktree, []byte("do not replace"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink target",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				outside := t.TempDir()
				if err := os.MkdirAll(filepath.Dir(f.worktree), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, f.worktree); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlinked repository parent",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				outside := t.TempDir()
				sentinel := filepath.Join(outside, "sentinel")
				if err := os.WriteFile(sentinel, []byte("unchanged"), 0o644); err != nil {
					t.Fatal(err)
				}
				parent := filepath.Dir(f.worktree)
				if err := os.Remove(parent); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, parent); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					data, err := os.ReadFile(sentinel)
					if err != nil || string(data) != "unchanged" {
						t.Errorf("outside sentinel changed: %q, %v", data, err)
					}
				})
			},
		},
		{
			name: "stale prunable registration",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				if err := gitpkg.WorktreeAdd(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree, f.pipeline); err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(f.worktree); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "moved replacement worktree",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				if err := gitpkg.WorktreeAdd(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree, f.pipeline); err != nil {
					t.Fatal(err)
				}
				moved := filepath.Join(t.TempDir(), "moved-run")
				gitCmd(t, f.p.RepoDir(f.repo.ID), "worktree", "move", f.worktree, moved)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowSyntheticInterruptedWorktreeReconstruction(t)
			f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
			if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, f)
			before, _ := f.d.GetRun(f.run.ID)
			_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			if !matched || err == nil {
				t.Fatalf("matched=%v err=%v", matched, err)
			}
			after, _ := f.d.GetRun(f.run.ID)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("ambiguous path refusal mutated run")
			}
			if _, err := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)); !os.IsNotExist(err) {
				t.Fatalf("ambiguous path created a journal: %v", err)
			}
		})
	}
}

func TestMissingWorktreeReconstructionRollsBackPartialPermissionFailure(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	addInterruptedWorktree = func(_ context.Context, _, target, _ string) error {
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(target, "partial"), []byte("owned"), 0o644); err != nil {
			return err
		}
		return os.ErrPermission
	}
	before, _ := f.d.GetRun(f.run.ID)
	_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if !matched || err == nil || !strings.Contains(err.Error(), "permission") {
		t.Fatalf("matched=%v err=%v", matched, err)
	}
	after, _ := f.d.GetRun(f.run.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("partial-add failure mutated run")
	}
	if _, err := os.Lstat(f.worktree); !os.IsNotExist(err) {
		t.Fatalf("partial target survived owned rollback: %v", err)
	}
}

func TestMissingWorktreeReconstructionNeverRollsBackChangedTarget(t *testing.T) {
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	calls := 0
	foreign := filepath.Join(f.worktree, "foreign-untracked")
	probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error {
		calls++
		if calls > 1 {
			if err := os.WriteFile(foreign, []byte("do not delete"), 0o644); err != nil {
				t.Fatal(err)
			}
			return fmt.Errorf("external state changed")
		}
		return nil
	}
	before, _ := f.d.GetRun(f.run.ID)
	_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if !matched || err == nil || !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("matched=%v err=%v", matched, err)
	}
	after, _ := f.d.GetRun(f.run.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("changed-target rollback refusal mutated the legacy run")
	}
	data, readErr := os.ReadFile(foreign)
	if readErr != nil || string(data) != "do not delete" {
		t.Fatalf("changed target content was removed: %q, %v", data, readErr)
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error { return nil }
	if runID, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent); err != nil || !matched || runID != f.run.ID {
		t.Fatalf("clean journal retry = %q, %v, %v", runID, matched, err)
	}
}

func TestMissingWorktreeReconstructionRetainsJournalWhenRollbackIsDenied(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory mode does not deny removal on Windows")
	}
	allowSyntheticInterruptedWorktreeReconstruction(t)
	f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
	if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(f.worktree)
	addInterruptedWorktree = func(_ context.Context, _, target, _ string) error {
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(target, "partial"), []byte("owned"), 0o644); err != nil {
			return err
		}
		if err := os.Chmod(parent, 0o500); err != nil {
			return err
		}
		return os.ErrPermission
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	before, _ := f.d.GetRun(f.run.ID)
	_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
	if !matched || err == nil || !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("matched=%v err=%v", matched, err)
	}
	after, _ := f.d.GetRun(f.run.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rollback failure mutated the legacy run")
	}
	if _, err := os.Lstat(f.worktree); err != nil {
		t.Fatalf("rollback failure did not retain partial target: %v", err)
	}
	if _, err := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)); err != nil {
		t.Fatalf("rollback failure did not retain journal: %v", err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	f.manager.reconcileInterruptedWorktreeJournal(context.Background())
	if _, err := os.Lstat(f.worktree); !os.IsNotExist(err) {
		t.Fatalf("restart reconciliation did not remove owned partial target: %v", err)
	}
	if _, err := os.Lstat(f.p.InterruptedWorktreeRecoveryDir(f.repo.ID, f.run.ID)); !os.IsNotExist(err) {
		t.Fatalf("restart reconciliation did not remove owned journal: %v", err)
	}
}

func TestMissingWorktreeReconstructionRefusesLiveOwnersAndMissingRef(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *interruptedRecoveryFixture)
	}{
		{
			name: "manager owner",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				f.manager.mu.Lock()
				f.manager.cancels[f.run.ID] = func(error) {}
				f.manager.mu.Unlock()
				t.Cleanup(func() {
					f.manager.mu.Lock()
					delete(f.manager.cancels, f.run.ID)
					f.manager.mu.Unlock()
				})
			},
		},
		{
			name: "process owner",
			mutate: func(t *testing.T, _ *interruptedRecoveryFixture) {
				probeInterruptedProcessOwners = func(context.Context, string, string) error { return fmt.Errorf("live process") }
			},
		},
		{
			name: "external probe unavailable",
			mutate: func(t *testing.T, _ *interruptedRecoveryFixture) {
				probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error {
					return fmt.Errorf("remote or PR probe unavailable")
				}
			},
		},
		{
			name: "moved source ref",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				gitCmd(t, f.p.RepoDir(f.repo.ID), "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.pipeline)
			},
		},
		{
			name: "missing pipeline object",
			mutate: func(t *testing.T, f *interruptedRecoveryFixture) {
				objectPath := filepath.Join(f.p.RepoDir(f.repo.ID), "objects", f.pipeline[:2], f.pipeline[2:])
				if err := os.Remove(objectPath); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowSyntheticInterruptedWorktreeReconstruction(t)
			f := newInterruptedRecoveryFixtureForRepo(t, arenaMissingWorktreeRepoID)
			if err := gitpkg.WorktreeRemove(context.Background(), f.p.RepoDir(f.repo.ID), f.worktree); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, f)
			before, _ := f.d.GetRun(f.run.ID)
			_, matched, err := f.manager.HandleRecoverInterruptedGate(context.Background(), f.repo.ID, f.run.Branch, f.submitted, f.intent)
			if !matched || err == nil {
				t.Fatalf("matched=%v err=%v", matched, err)
			}
			after, _ := f.d.GetRun(f.run.ID)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("owner/ref refusal mutated run")
			}
		})
	}
}

func TestArenaCandidatePRFilterRequiresExactOwnerBranchAndBase(t *testing.T) {
	decode := func(value string) []interruptedPR {
		t.Helper()
		var prs []interruptedPR
		if err := json.Unmarshal([]byte(value), &prs); err != nil {
			t.Fatal(err)
		}
		return prs
	}
	prs := decode(`[
		{"number":1,"headRefName":"fm/arena-no-mistakes-beta","baseRefName":"staging","headRepositoryOwner":{"login":"other"}},
		{"number":4,"headRefName":"fm/arena-no-mistakes-beta","baseRefName":"staging","headRepositoryOwner":{"login":"arenacrm"}}
	]`)
	if number, ok, err := arenaCandidatePR(prs, arenaMissingWorktreeBranch, arenaMissingWorktreeBaseBranch); err != nil || !ok || number != 4 {
		t.Fatalf("candidate PR = %d, %v, %v, want 4, true, nil", number, ok, err)
	}
	if _, ok, err := arenaCandidatePR(prs[:1], arenaMissingWorktreeBranch, arenaMissingWorktreeBaseBranch); err != nil || ok {
		t.Fatalf("foreign owner result = %v, %v, want false, nil", ok, err)
	}
	for _, malformed := range []string{
		`[{"number":2,"headRefName":"other","baseRefName":"staging","headRepositoryOwner":{"login":"ArenaCRM"}}]`,
		`[{"number":3,"headRefName":"fm/arena-no-mistakes-beta","baseRefName":"main","headRepositoryOwner":{"login":"ArenaCRM"}}]`,
		`[{"number":4,"headRefName":"fm/arena-no-mistakes-beta","baseRefName":"staging","headRepositoryOwner":null}]`,
	} {
		if _, _, err := arenaCandidatePR(decode(malformed), arenaMissingWorktreeBranch, arenaMissingWorktreeBaseBranch); err == nil {
			t.Fatal("incomplete or mismatched PR identity was accepted")
		}
	}
}

func TestArenaMissingWorktreeIncidentFingerprintIsExact(t *testing.T) {
	exact := func() (*db.Repo, *db.Run, *db.StepResult, string) {
		intent := "synthetic exact intent"
		bootstrapRepo := arenaMissingWorktreeBootstrapRepo
		bootstrapBase := arenaMissingWorktreeBaseBranch
		bootstrapCommand := arenaMissingWorktreeBootstrapCommand
		bootstrapDigest := arenaMissingWorktreeBootstrapDigest
		submitted := arenaMissingWorktreeSubmittedHead
		runErr := db.LegacyDaemonShutdownError
		repo := &db.Repo{ID: arenaMissingWorktreeRepoID, UpstreamURL: "https://github.com/ArenaCRM/arena-crm", DefaultBranch: arenaMissingWorktreeDefaultBranch, BaseBranch: arenaMissingWorktreeBaseBranch}
		run := &db.Run{
			ID: arenaMissingWorktreeRunID, RepoID: arenaMissingWorktreeRepoID, Branch: arenaMissingWorktreeBranch,
			HeadSHA: arenaMissingWorktreePipelineHead, BaseSHA: strings.Repeat("0", 40), BaseBranch: arenaMissingWorktreeBaseBranch,
			SubmittedHeadSHA: &submitted, Status: types.RunFailed, Error: &runErr, Intent: &intent,
			BootstrapTestRepository: &bootstrapRepo, BootstrapTestBaseBranch: &bootstrapBase,
			BootstrapTestCommand: &bootstrapCommand, BootstrapTestPolicySHA256: &bootstrapDigest,
			CreatedAt: arenaMissingWorktreeCreatedAt, UpdatedAt: arenaMissingWorktreeUpdatedAt, ParkedMS: arenaMissingWorktreeParkedMS,
		}
		return repo, run, &db.StepResult{ID: arenaMissingWorktreeTestStepID, RunID: run.ID, StepName: types.StepTest}, sha256Hex(intent)
	}
	repo, run, gate, digest := exact()
	if err := matchArenaMissingWorktreeIncident(repo, run, gate, digest); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*db.Repo, *db.Run, *db.StepResult, *string)
	}{
		{"repo id", func(r *db.Repo, _ *db.Run, _ *db.StepResult, _ *string) { r.ID = "other" }},
		{"upstream", func(r *db.Repo, _ *db.Run, _ *db.StepResult, _ *string) {
			r.UpstreamURL = "https://github.com/other/repo"
		}},
		{"fork", func(r *db.Repo, _ *db.Run, _ *db.StepResult, _ *string) { r.ForkURL = "https://github.com/fork/repo" }},
		{"default branch", func(r *db.Repo, _ *db.Run, _ *db.StepResult, _ *string) { r.DefaultBranch = "staging" }},
		{"repo base", func(r *db.Repo, _ *db.Run, _ *db.StepResult, _ *string) { r.BaseBranch = "main" }},
		{"run id", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.ID = "other" }},
		{"run repo", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.RepoID = "other" }},
		{"branch", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.Branch = "other" }},
		{"base branch", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.BaseBranch = "main" }},
		{"base sha", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) {
			r.BaseSHA = arenaMissingWorktreeSubmittedHead
		}},
		{"submitted", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) {
			value := arenaMissingWorktreePipelineHead
			r.SubmittedHeadSHA = &value
		}},
		{"pipeline", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) {
			r.HeadSHA = arenaMissingWorktreeSubmittedHead
		}},
		{"intent", func(_ *db.Repo, _ *db.Run, _ *db.StepResult, d *string) { *d = strings.Repeat("0", 64) }},
		{"bootstrap", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) {
			value := "other"
			r.BootstrapTestCommand = &value
		}},
		{"created", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.CreatedAt++ }},
		{"updated", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.UpdatedAt++ }},
		{"parked", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.ParkedMS++ }},
		{"status", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { r.Status = types.RunCompleted }},
		{"error", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) { value := "other"; r.Error = &value }},
		{"source ref", func(_ *db.Repo, r *db.Run, _ *db.StepResult, _ *string) {
			value := "refs/heads/other"
			r.SourceRef = &value
		}},
		{"step id", func(_ *db.Repo, _ *db.Run, s *db.StepResult, _ *string) { s.ID = "other" }},
		{"step run", func(_ *db.Repo, _ *db.Run, s *db.StepResult, _ *string) { s.RunID = "other" }},
		{"step name", func(_ *db.Repo, _ *db.Run, s *db.StepResult, _ *string) { s.StepName = types.StepReview }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo, run, gate, digest := exact()
			tc.mutate(repo, run, gate, &digest)
			if err := matchArenaMissingWorktreeIncident(repo, run, gate, digest); err == nil {
				t.Fatal("mismatched fingerprint was accepted")
			}
		})
	}
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
