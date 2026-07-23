//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func interruptedRecoveryScenario(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "interrupted-recovery.yaml")
	content := `actions:
  - match: "Review the code changes and return structured findings"
    structured:
      findings:
        - id: "review-1"
          severity: warning
          file: "feature.txt"
          line: 1
          description: "unsafe value needs a guard"
          action: auto-fix
      summary: "review requires one fix"
      risk_level: medium
      risk_rationale: "the unsafe value needs a guard"
  - match: "Investigate previous review findings"
    text: "fixed unsafe value"
    edits:
      - path: "feature.txt"
        old: "unsafe"
        new: "safe"
    structured:
      summary: "guard unsafe value"
  - match: "Review the code changes and return structured findings"
    structured:
      findings: []
      summary: "review clean after fix"
      risk_level: low
      risk_rationale: "the guard is present"
  - match: "You are validating a code change by testing it"
    structured:
      findings:
        - id: "test-1"
          severity: error
          file: "feature.txt"
          line: 1
          description: "source-ref Test command summary"
          action: auto-fix
      summary: "source-ref Test command summary"
      tested: ["fakeagent: source-ref preflight"]
      testing_summary: "source-ref preflight needs a fix"
      risk_level: medium
      risk_rationale: "the source-ref preflight failed"
  - match: "Fix the failing tests in this repository"
    text: "fixed source-ref preflight"
    edits:
      - path: "feature.txt"
        old: "safe"
        new: "tested"
    structured:
      summary: "fix source-ref preflight"
  - match: "You are validating a code change by testing it"
    structured:
      findings: []
      summary: "tests pass after fix"
      tested: ["fakeagent: source-ref preflight"]
      testing_summary: "source-ref preflight passed"
      risk_level: low
      risk_rationale: "the test now passes"
  - text: "no issues found"
    structured:
      findings: []
      summary: "no issues found"
      risk_level: low
      risk_rationale: "no remaining risk"
      tested: ["fakeagent: focused verification"]
      testing_summary: "simulated checks passed"
      title: "fix: resume interrupted gate"
      body: "resume interrupted gate"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestAxiRunRecoversLegacyGracefulShutdownGate exercises the user-visible
// upgrade path with the real binary and isolated daemon. The daemon is stopped
// only inside this test's private NM_HOME.
func TestAxiRunReconstructsAllowlistedMissingLegacyWorktree(t *testing.T) {
	t.Setenv("NM_E2E_SYNTHETIC_INTERRUPTED_RECONSTRUCTION", "1")
	h := NewHarness(t, SetupOpts{Agent: "claude", Scenario: interruptedRecoveryScenario(t)})
	h.CommitChange("init-missing-interrupted", "seed.txt", "seed\n", "seed interrupted reconstruction")
	initWorktree := h.AddWorktree("init-missing-interrupted")
	if out, err := h.RunInDir(initWorktree, "init"); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	if out, err := h.RunInDir(initWorktree, "daemon", "stop"); err != nil {
		t.Fatalf("stop private daemon before repo-id fixture migration: %v\n%s", err, out)
	}

	oldRepoID := h.repoID()
	p := paths.WithRoot(h.NMHome)
	oldGate := p.RepoDir(oldRepoID)
	newGate := p.RepoDir(arenaEvidenceRepoID)
	if err := os.Rename(oldGate, newGate); err != nil {
		t.Fatal(err)
	}
	sqlDB, err := sql.Open("sqlite", p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE repos SET id = ? WHERE id = ?`, arenaEvidenceRepoID, oldRepoID); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := h.runGit(context.Background(), h.WorkDir, "remote", "set-url", "no-mistakes", newGate); err != nil {
		t.Fatalf("retarget synthetic gate remote: %v\n%s", err, out)
	}

	branch := "feature/interrupted-reconstruction"
	intent := "preserve the same run while reconstructing only its missing pipeline worktree"
	h.CommitChange(branch, "feature.txt", "unsafe\n", "add unsafe feature")
	operator := h.AddWorktree(branch)
	initialOut, err := h.RunInDir(operator, "axi", "run", "--intent", intent)
	if err != nil || !strings.Contains(initialOut, "review-1") {
		t.Fatalf("initial Review gate: %v\n%s", err, initialOut)
	}
	fixOut, err := h.RunInDir(operator, "axi", "respond", "--action", "fix", "--findings", "review-1")
	if err != nil || !strings.Contains(fixOut, "status: fix_review") {
		t.Fatalf("Review fix gate: %v\n%s", err, fixOut)
	}
	testOut, err := h.RunInDir(operator, "axi", "respond", "--action", "approve")
	if err != nil || !strings.Contains(testOut, "step: test") || !strings.Contains(testOut, "test-1") {
		t.Fatalf("Test gate: %v\n%s", err, testOut)
	}

	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	runs, err := database.GetRunsByRepo(arenaEvidenceRepoID)
	if err != nil || len(runs) != 1 {
		_ = database.Close()
		t.Fatalf("synthetic incident runs = %d, %v", len(runs), err)
	}
	original := runs[0]
	steps, err := database.GetStepsByRun(original.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	var testStep *db.StepResult
	for _, step := range steps {
		if step.StepName == types.StepTest {
			testStep = step
			break
		}
	}
	if testStep == nil || testStep.Status != types.StepStatusAwaitingApproval {
		_ = database.Close()
		t.Fatalf("synthetic Test gate = %#v", testStep)
	}
	roundsBefore, _ := database.GetRoundsByStep(testStep.ID)
	sessionsBefore, _ := database.GetRunAgentSessions(original.ID)
	invocationsBefore := len(h.AgentInvocations())
	operatorBefore := strings.TrimSpace(h.WorktreeRefSHA(branch))
	gateBeforeBytes, gateErr := h.runGit(context.Background(), newGate, "rev-parse", "refs/heads/"+branch)
	if gateErr != nil {
		_ = database.Close()
		t.Fatalf("read gate ref before reconstruction: %v\n%s", gateErr, gateBeforeBytes)
	}
	gateBefore := strings.TrimSpace(string(gateBeforeBytes))
	remoteBeforeBytes, remoteErr := h.runGit(context.Background(), operator, "ls-remote", "origin", "refs/heads/"+branch)
	if remoteErr != nil {
		_ = database.Close()
		t.Fatalf("read remote before reconstruction: %v\n%s", remoteErr, remoteBeforeBytes)
	}
	remoteBefore := strings.TrimSpace(string(remoteBeforeBytes))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	if out, err := h.RunInDir(operator, "daemon", "stop", "--force"); err != nil {
		t.Fatalf("stop isolated daemon: %v\n%s", err, out)
	}
	oldWorktree := p.WorktreeDir(arenaEvidenceRepoID, original.ID)
	if out, err := h.runGit(context.Background(), newGate, "worktree", "remove", "--force", oldWorktree); err != nil {
		t.Fatalf("remove synthetic pipeline worktree: %v\n%s", err, out)
	}

	sqlDB, err = sql.Open("sqlite", p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	now := time.Now().Unix()
	statements := []struct {
		query string
		args  []any
	}{
		{`UPDATE runs SET id = ? WHERE id = ?`, []any{arenaEvidenceRunID, original.ID}},
		{`UPDATE step_results SET run_id = ? WHERE run_id = ?`, []any{arenaEvidenceRunID, original.ID}},
		{`UPDATE run_agent_sessions SET run_id = ? WHERE run_id = ?`, []any{arenaEvidenceRunID, original.ID}},
		{`UPDATE agent_invocations SET run_id = ? WHERE run_id = ?`, []any{arenaEvidenceRunID, original.ID}},
		{`UPDATE runs SET status = 'failed', error = ?, awaiting_agent_since = NULL, source_ref = NULL, updated_at = ? WHERE id = ?`, []any{db.LegacyDaemonShutdownError, now, arenaEvidenceRunID}},
		{`UPDATE step_results SET status = 'failed', error = ?, completed_at = ?, last_activity_at = ?, last_activity = 'step failed: daemon shutting down', agent_pid = NULL WHERE id = ?`, []any{db.LegacyDaemonShutdownError, now, now, testStep.ID}},
	}
	for _, statement := range statements {
		if _, err := sqlDB.Exec(statement.query, statement.args...); err != nil {
			_ = sqlDB.Close()
			t.Fatal(err)
		}
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	reconstructedOut, err := h.RunInDir(operator, "axi", "run", "--intent", intent)
	if err != nil {
		t.Fatalf("reconstruct missing interrupted Test gate: %v\n%s", err, reconstructedOut)
	}
	t.Logf("ordinary axi run reconstructed the missing pipeline worktree and returned the preserved Test gate:\n%s", reconstructedOut)
	for _, want := range []string{arenaEvidenceRunID, "step: test", "status: awaiting_approval", "test-1"} {
		if !strings.Contains(reconstructedOut, want) {
			t.Errorf("reconstructed output missing %q:\n%s", want, reconstructedOut)
		}
	}
	reconstructedWorktree := p.WorktreeDir(arenaEvidenceRepoID, arenaEvidenceRunID)
	if out, err := h.runGit(context.Background(), reconstructedWorktree, "rev-parse", "HEAD"); err != nil || strings.TrimSpace(string(out)) != original.HeadSHA {
		t.Fatalf("reconstructed worktree HEAD = %s, %v, want %s", strings.TrimSpace(string(out)), err, original.HeadSHA)
	}
	commonBytes, commonErr := h.runGit(context.Background(), reconstructedWorktree, "rev-parse", "--git-common-dir")
	if commonErr != nil {
		t.Fatalf("reconstructed common dir: %v\n%s", commonErr, commonBytes)
	}
	common := strings.TrimSpace(string(commonBytes))
	if !filepath.IsAbs(common) {
		common = filepath.Join(reconstructedWorktree, common)
	}
	resolvedCommon, _ := filepath.EvalSymlinks(common)
	resolvedGate, _ := filepath.EvalSymlinks(newGate)
	if filepath.Clean(resolvedCommon) != filepath.Clean(resolvedGate) {
		t.Fatalf("reconstructed common dir = %q, want %q", resolvedCommon, resolvedGate)
	}
	statusBytes, statusErr := h.runGit(context.Background(), reconstructedWorktree, "status", "--porcelain=v1")
	if statusErr != nil || strings.TrimSpace(string(statusBytes)) != "" {
		t.Fatalf("reconstructed worktree is dirty: %v\n%s", statusErr, statusBytes)
	}
	gateAfterBytes, gateErr := h.runGit(context.Background(), newGate, "rev-parse", "refs/heads/"+branch)
	remoteAfterBytes, remoteErr := h.runGit(context.Background(), operator, "ls-remote", "origin", "refs/heads/"+branch)
	if gateErr != nil || strings.TrimSpace(string(gateAfterBytes)) != gateBefore || strings.TrimSpace(h.WorktreeRefSHA(branch)) != operatorBefore ||
		remoteErr != nil || strings.TrimSpace(string(remoteAfterBytes)) != remoteBefore {
		t.Fatalf("reconstruction moved refs: gate=%q/%q operator=%q/%q remote=%q/%q errors=%v/%v",
			strings.TrimSpace(string(gateAfterBytes)), gateBefore, strings.TrimSpace(h.WorktreeRefSHA(branch)), operatorBefore,
			strings.TrimSpace(string(remoteAfterBytes)), remoteBefore, gateErr, remoteErr)
	}
	database, err = db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	runs, _ = database.GetRunsByRepo(arenaEvidenceRepoID)
	roundsAfter, _ := database.GetRoundsByStep(testStep.ID)
	sessionsAfter, _ := database.GetRunAgentSessions(arenaEvidenceRunID)
	if len(runs) != 1 || runs[0].ID != arenaEvidenceRunID || len(roundsAfter) != len(roundsBefore) || len(sessionsAfter) != len(sessionsBefore) {
		_ = database.Close()
		t.Fatalf("reconstruction changed durable identity: runs=%d rounds=%d/%d sessions=%d/%d", len(runs), len(roundsAfter), len(roundsBefore), len(sessionsAfter), len(sessionsBefore))
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(h.AgentInvocations()); got != invocationsBefore {
		t.Fatalf("reconstruction invoked an agent before response: %d -> %d", invocationsBefore, got)
	}

	resumeOut, err := h.RunInDir(operator, "axi", "respond", "--action", "fix", "--findings", "test-1")
	if err != nil {
		t.Fatalf("respond to reconstructed Test gate: %v\n%s", err, resumeOut)
	}
	for i := 0; i < 4 && strings.Contains(resumeOut, "gate:"); i++ {
		resumeOut, err = h.RunInDir(operator, "axi", "respond", "--action", "approve")
		if err != nil {
			t.Fatalf("approve post-reconstruction gate: %v\n%s", err, resumeOut)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		database, openErr := db.Open(p.DB())
		if openErr != nil {
			t.Fatal(openErr)
		}
		finished, getErr := database.GetRun(arenaEvidenceRunID)
		_ = database.Close()
		if getErr == nil && finished != nil && finished.Status == types.RunCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("same reconstructed run did not complete; last output:\n%s", resumeOut)
		}
		time.Sleep(100 * time.Millisecond)
	}
	statusOut, err := h.RunInDir(operator, "axi", "status")
	if err != nil {
		t.Fatalf("read completed reconstructed run: %v\n%s", err, statusOut)
	}
	for _, want := range []string{arenaEvidenceRunID, "status: completed"} {
		if !strings.Contains(statusOut, want) {
			t.Errorf("completed reconstructed run output missing %q:\n%s", want, statusOut)
		}
	}
	t.Logf("same reconstructed run completed after the explicit Test-gate response:\n%s", statusOut)
}

const (
	arenaEvidenceRepoID = "935b6bc75a9a"
	arenaEvidenceRunID  = "01KY4FMNYM4AX8PA9MY92QMHN4"
)

func TestAxiRunRecoversLegacyGracefulShutdownGate(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude", Scenario: interruptedRecoveryScenario(t)})
	h.CommitChange("init-interrupted", "seed.txt", "seed\n", "seed interrupted recovery")
	initWorktree := h.AddWorktree("init-interrupted")
	if out, err := h.RunInDir(initWorktree, "init"); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	branch := "feature/interrupted-recovery"
	intent := "preserve all pipeline fixes while resuming the interrupted Test decision"
	submitted := h.CommitChange(branch, "feature.txt", "unsafe\n", "add unsafe feature")
	operator := h.AddWorktree(branch)
	initialOut, err := h.RunInDir(operator, "axi", "run", "--intent", intent)
	if err != nil || !strings.Contains(initialOut, "review-1") {
		t.Fatalf("initial Review gate: %v\n%s", err, initialOut)
	}
	fixOut, err := h.RunInDir(operator, "axi", "respond", "--action", "fix", "--findings", "review-1")
	if err != nil || !strings.Contains(fixOut, "status: fix_review") {
		t.Fatalf("Review fix gate: %v\n%s", err, fixOut)
	}
	testOut, err := h.RunInDir(operator, "axi", "respond", "--action", "approve")
	if err != nil || !strings.Contains(testOut, "step: test") || !strings.Contains(testOut, "test-1") {
		t.Fatalf("Test gate after Review approval: %v\n%s", err, testOut)
	}

	original := h.ActiveRun(branch)
	if original == nil {
		t.Fatal("Test gate has no active run")
	}
	if original.HeadSHA == submitted {
		t.Fatal("Review fix did not advance the pipeline head")
	}
	testStep, ok := findStep(original.Steps, types.StepTest)
	if !ok || testStep.Status != types.StepStatusAwaitingApproval || testStep.FindingsJSON == nil || !strings.Contains(*testStep.FindingsJSON, "source-ref Test command summary") {
		t.Fatalf("preserved Test gate = %#v", testStep)
	}
	runsBefore := len(h.Runs())
	operatorBefore := strings.TrimSpace(h.WorktreeRefSHA(branch))
	gateDir := filepath.Join(h.NMHome, "repos", h.repoID()+".git")
	gateBeforeBytes, gitErr := h.runGit(context.Background(), gateDir, "rev-parse", "refs/heads/"+branch)
	if gitErr != nil {
		t.Fatalf("gate head before shutdown: %v\n%s", gitErr, gateBeforeBytes)
	}
	gateBefore := strings.TrimSpace(string(gateBeforeBytes))
	database, err := db.Open(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatal(err)
	}
	roundsBefore, err := database.GetRoundsByStep(testStep.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	sessionsBefore, err := database.GetRunAgentSessions(original.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	invocationsBefore := len(h.AgentInvocations())
	remoteBeforeBytes, remoteErr := h.runGit(context.Background(), operator, "ls-remote", "origin", "refs/heads/"+branch)
	if remoteErr != nil {
		t.Fatalf("remote branch before shutdown: %v\n%s", remoteErr, remoteBeforeBytes)
	}
	remoteBefore := strings.TrimSpace(string(remoteBeforeBytes))

	if out, err := h.RunInDir(operator, "daemon", "stop", "--force"); err != nil {
		t.Fatalf("stop isolated daemon: %v\n%s", err, out)
	}
	p := paths.WithRoot(h.NMHome)
	sqlDB, err := sql.Open("sqlite", p.DB())
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.Exec(`PRAGMA busy_timeout = 10000`); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if _, err := sqlDB.Exec(`UPDATE runs SET status = 'failed', error = ?, awaiting_agent_since = NULL, source_ref = NULL, updated_at = ? WHERE id = ?`, db.LegacyDaemonShutdownError, now, original.ID); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE step_results SET status = 'failed', error = ?, completed_at = ?, last_activity_at = ?, last_activity = 'step failed: daemon shutting down', agent_pid = NULL WHERE id = ?`, db.LegacyDaemonShutdownError, now, now, testStep.ID); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	reattachOut, err := h.RunInDir(operator, "axi", "run", "--intent", intent)
	if err != nil {
		t.Fatalf("recover interrupted Test gate: %v\n%s", err, reattachOut)
	}
	t.Logf("recovered Test gate through ordinary axi run:\n%s", reattachOut)
	for _, want := range []string{original.ID, "step: test", "status: awaiting_approval", "test-1", "source-ref Test command summary"} {
		if !strings.Contains(reattachOut, want) {
			t.Errorf("recovered output missing %q:\n%s", want, reattachOut)
		}
	}
	if got := len(h.Runs()); got != runsBefore {
		t.Fatalf("recovery changed run count from %d to %d", runsBefore, got)
	}
	database, err = db.Open(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatal(err)
	}
	roundsAfter, err := database.GetRoundsByStep(testStep.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	sessionsAfter, err := database.GetRunAgentSessions(original.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if len(roundsAfter) != len(roundsBefore) || roundsAfter[0].ID != roundsBefore[0].ID {
		t.Fatalf("recovery changed Test rounds: before=%#v after=%#v", roundsBefore, roundsAfter)
	}
	if len(sessionsAfter) != len(sessionsBefore) {
		t.Fatalf("recovery changed session count: %d -> %d", len(sessionsBefore), len(sessionsAfter))
	}
	if got := len(h.AgentInvocations()); got != invocationsBefore {
		t.Fatalf("recovery invoked an agent before response: %d -> %d", invocationsBefore, got)
	}
	recovered := h.ActiveRun(branch)
	if recovered == nil || recovered.ID != original.ID || recovered.HeadSHA != original.HeadSHA {
		t.Fatalf("recovered run = %#v, want same ID and head", recovered)
	}
	if got := strings.TrimSpace(h.WorktreeRefSHA(branch)); got != operatorBefore {
		t.Fatalf("operator branch moved from %s to %s", operatorBefore, got)
	}
	gateAfterBytes, gitErr := h.runGit(context.Background(), gateDir, "rev-parse", "refs/heads/"+branch)
	if gitErr != nil || strings.TrimSpace(string(gateAfterBytes)) != gateBefore {
		t.Fatalf("gate candidate after recovery = %s (err %v), want %s", strings.TrimSpace(string(gateAfterBytes)), gitErr, gateBefore)
	}
	remoteAfterBytes, remoteErr := h.runGit(context.Background(), operator, "ls-remote", "origin", "refs/heads/"+branch)
	if remoteErr != nil || strings.TrimSpace(string(remoteAfterBytes)) != remoteBefore {
		t.Fatalf("remote branch moved during recovery: before=%q after=%q err=%v", remoteBefore, strings.TrimSpace(string(remoteAfterBytes)), remoteErr)
	}

	resumeOut, err := h.RunInDir(operator, "axi", "respond", "--action", "fix", "--findings", "test-1")
	if err != nil {
		t.Fatalf("respond to recovered Test gate: %v\n%s", err, resumeOut)
	}
	t.Logf("recovered Test response:\n%s", resumeOut)
	// A fix-review gate is a valid next state. Accept it so this isolated e2e
	// daemon can finish cleanly, while keeping the assertion that no step was
	// silently rerun before the explicit response above.
	for i := 0; i < 3 && strings.Contains(resumeOut, "gate:"); i++ {
		resumeOut, err = h.RunInDir(operator, "axi", "respond", "--action", "approve")
		if err != nil {
			t.Fatalf("approve post-recovery gate: %v\n%s", err, resumeOut)
		}
	}
	finished := h.WaitForRun(branch, 60*time.Second)
	if finished.ID != original.ID || finished.Status != types.RunCompleted {
		t.Fatalf("resumed run = %s/%s, want %s/completed; last output:\n%s", finished.ID, finished.Status, original.ID, resumeOut)
	}
	if got := len(h.Runs()); got != runsBefore {
		t.Fatalf("response created a replacement run: %d -> %d", runsBefore, got)
	}
}
