//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// TestAxiRecoverFinalHeadCapacityJourney seeds the precise historical terminal
// footprint left by the old production Document preflight, then drives the real
// AXI command through worktree reconstruction, isolated Document no-op
// assessment, Lint, exact-target closure, Push, PR/CI suffix handling, and
// completion without creating a replacement run.
func TestAxiRecoverFinalHeadCapacityJourney(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude"})
	t.Setenv("NM_E2E_SYNTHETIC_EXACT_FINAL_HEAD_RECOVERY", "1")
	t.Setenv("FAKEAGENT_GH_MODE", "exact-recovery")
	t.Setenv("FAKEAGENT_GH_LOG", filepath.Join(filepath.Dir(h.AgentLog), "gh-exact-recovery.log"))
	t.Setenv("FAKEAGENT_GH_STATE_DIR", filepath.Dir(h.AgentLog))

	trustedConfig := "allow_repo_commands: true\ncommands:\n  test: \"true\"\n"
	h.CommitChange("main", ".no-mistakes.yaml", trustedConfig, "configure exact test")
	if out, err := h.runGit(context.Background(), h.WorkDir, "push", "origin", "main"); err != nil {
		t.Fatalf("push trusted main: %v\n%s", err, out)
	}
	published := h.CommitChange("feature/exact-final-head", "published.txt", "published\n", "published candidate")
	if out, err := h.runGit(context.Background(), h.WorkDir, "push", "-u", "origin", "feature/exact-final-head"); err != nil {
		t.Fatalf("push published feature head: %v\n%s", err, out)
	}
	exact := h.CommitChange("feature/exact-final-head", "exact.txt", "exact\n", "unpublished exact candidate")
	t.Setenv("FAKEAGENT_GH_HEAD", exact)
	base := strings.TrimSpace(string(mustGitOutput(t, h, h.WorkDir, "rev-parse", "main")))

	if out, err := h.Run("init"); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	p := paths.WithRoot(h.NMHome)
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	workingPath, err := filepath.EvalSymlinks(h.WorkDir)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.GetRepoByPath(workingPath)
	if err != nil || repo == nil {
		t.Fatalf("registered repo = %#v, err %v", repo, err)
	}
	repo, err = database.UpdateRepoMetadata(repo.ID, "https://github.com/test/project.git", repo.DefaultBranch)
	if err != nil {
		t.Fatalf("bind synthetic GitHub repository identity: %v", err)
	}
	gate := p.RepoDir(repo.ID)
	if out, err := h.runGit(context.Background(), gate, "fetch", h.WorkDir, exact); err != nil {
		t.Fatalf("stage exact candidate object in gate: %v\n%s", err, out)
	}
	if out, err := h.runGit(context.Background(), gate, "update-ref", "refs/heads/feature/exact-final-head", exact); err != nil {
		t.Fatalf("bind exact gate source ref: %v\n%s", err, out)
	}

	run, err := database.InsertRun(repo.ID, "feature/exact-final-head", exact, base)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if count, err := database.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 1 {
		t.Fatalf("schedule replay 1: count=%d err=%v", count, err)
	}
	if err := database.UpdateRunHeadSHA(run.ID, published); err != nil {
		t.Fatal(err)
	}
	if count, err := database.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 2 {
		t.Fatalf("schedule replay 2: count=%d err=%v", count, err)
	}
	if err := database.UpdateRunHeadSHA(run.ID, exact); err != nil {
		t.Fatal(err)
	}
	if count, err := database.ScheduleHeadValidationReplay(run.ID, 3); err != nil || count != 3 {
		t.Fatalf("schedule replay 3: count=%d err=%v", count, err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, exact); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPRURL(run.ID, "https://github.com/test/project/pull/42"); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPushBinding(run.ID, db.PushBinding{
		HeadSHA: published, TargetKind: "upstream", TargetFingerprint: branchsync.TargetFingerprint(h.UpstreamDir), Ref: "refs/heads/feature/exact-final-head",
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range types.AllSteps() {
		step, err := database.InsertStepResult(run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case name.Order() <= types.StepTest.Order():
			if err := database.CompleteStep(step.ID, 0, 1, string(name)+".log"); err != nil {
				t.Fatal(err)
			}
		case name == types.StepDocument:
			if err := database.StartStep(step.ID); err != nil {
				t.Fatal(err)
			}
			if err := database.FailStep(step.ID, db.ExactFinalHeadCapacityStepError(3), 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := database.UpdateRunErrorStatus(run.ID, db.ExactFinalHeadCapacityRunError(3), types.RunFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(p.WorktreeDir(repo.ID, run.ID)); !os.IsNotExist(err) {
		t.Fatalf("terminal fixture unexpectedly has a worktree: %v", err)
	}

	out, err := h.Run("axi", "recover-final-head", "--run", run.ID)
	if err != nil {
		ghLog, _ := os.ReadFile(filepath.Join(filepath.Dir(h.AgentLog), "gh-exact-recovery.log"))
		t.Fatalf("recover final head: %v\nrepo upstream: %s\ngh log:\n%s\n%s", err, repo.UpstreamURL, ghLog, out)
	}
	t.Logf("recover-final-head CLI output:\n%s", out)
	for _, want := range []string{"outcome: passed", run.ID} {
		if !strings.Contains(out, want) {
			t.Errorf("recovery output missing %q:\n%s", want, out)
		}
	}
	runs := h.Runs()
	if len(runs) != 1 || runs[0].ID != run.ID || runs[0].Status != types.RunCompleted {
		t.Fatalf("recovery replaced or failed the historical run: %#v", runs)
	}
	event, err := database.GetRunRecoveryEvent(run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil || event == nil || event.HeadSHA != exact || event.LastPushedSHA != published {
		t.Fatalf("recovery audit event = %#v, err %v", event, err)
	}
	if got := h.UpstreamBranchSHA("feature/exact-final-head"); got != exact {
		t.Fatalf("upstream feature head = %s, want exact %s", got, exact)
	}
	if _, err := os.Stat(filepath.Join(p.RunLogDir(run.ID), "document.log")); err != nil {
		t.Fatalf("Document recovery log is missing: %v", err)
	}
}

func mustGitOutput(t *testing.T, h *Harness, dir string, args ...string) []byte {
	t.Helper()
	out, err := h.runGit(context.Background(), dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}
