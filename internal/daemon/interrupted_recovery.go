package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// HandleRecoverInterruptedGate is the single runtime owner of compatibility
// recovery for approval waits terminalized by an older daemon's graceful
// shutdown. A false matched result means there is no exact legacy signature and
// the caller may follow the ordinary fresh-run path. Once the signature is
// recognized, every mismatch is a fail-closed refusal and the old run remains
// terminal and unchanged.
func (m *RunManager) HandleRecoverInterruptedGate(ctx context.Context, repoID, branch, localHead, intent string) (runID string, matched bool, err error) {
	if m.shuttingDown.Load() {
		return "", true, fmt.Errorf("interrupted run recovery refused: daemon is shutting down")
	}
	if repoID == "" || branch == "" {
		return "", false, nil
	}

	lockKey := repoID + "/" + branch
	lockVal, _ := m.branchLocks.LoadOrStore(lockKey, &sync.Mutex{})
	branchMu := lockVal.(*sync.Mutex)
	branchMu.Lock()
	defer branchMu.Unlock()
	return m.handleRecoverInterruptedGateLocked(ctx, repoID, branch, localHead, intent)
}

func (m *RunManager) handleRecoverInterruptedGateLocked(ctx context.Context, repoID, branch, localHead, intent string) (string, bool, error) {
	if active, err := m.db.GetActiveRun(repoID, branch); err != nil {
		return "", true, fmt.Errorf("interrupted run recovery refused: read active run: %w", err)
	} else if active != nil {
		if interruptedReattachIdentityMatches(active, localHead, intent) && pipeline.ValidateRecoveredRun(m.db, active, m.steps()) == nil {
			return active.ID, true, nil
		}
		// This RPC is called only after the CLI could not branch/head-match an
		// active run. A different active run belongs to ordinary branch-ownership
		// handling, not legacy terminal-run recovery.
		return "", false, nil
	}

	run, err := m.db.GetLatestRunForBranch(repoID, branch)
	if err != nil {
		return "", true, fmt.Errorf("interrupted run recovery refused: %w", err)
	}
	if run == nil || run.Status != types.RunFailed || run.Error == nil || *run.Error != db.LegacyDaemonShutdownError {
		return "", false, nil
	}
	refuse := func(reason string, args ...any) (string, bool, error) {
		return "", true, fmt.Errorf("interrupted run recovery refused: "+reason, args...)
	}
	refusePostClaim := func(reason string, cleanupErr error) (string, bool, error) {
		if cleanupErr != nil {
			return refuse("%s; terminalization also failed: %v", reason, cleanupErr)
		}
		return refuse("%s", reason)
	}
	if !interruptedReattachIdentityMatches(run, localHead, intent) {
		return refuse("submitted head or authoritative intent does not match the interrupted run")
	}
	repo, err := m.db.GetRepo(repoID)
	if err != nil {
		return refuse("read repository: %v", err)
	}
	if repo == nil {
		return refuse("repository is missing")
	}
	canonicalRef, err := sourceprovenance.CanonicalSourceRefFromBranch(run.Branch)
	if err != nil {
		return refuse("durable branch identity is invalid: %v", err)
	}

	execSteps := m.steps()
	expected := make([]types.StepName, len(execSteps))
	for i, step := range execSteps {
		expected[i] = step.Name()
	}
	if _, err := m.db.InspectLegacyInterruptedGate(run.ID, repoID, branch, run.HeadSHA, localHead, strings.TrimSpace(intent), canonicalRef, expected); err != nil {
		return refuse("%v", err)
	}
	workDir, gateDir, err := m.validateInterruptedPipelineCopy(ctx, repo, run)
	if err != nil {
		return refuse("%v", err)
	}

	preparedRun := *run
	preparedRun.Status = types.RunRunning
	preparedRun.Error = nil
	preparedRun.SourceRef = &canonicalRef
	parked := int64(1)
	preparedRun.AwaitingAgentSince = &parked
	cfg, err := m.loadRecoveredConfig(ctx, &preparedRun, repo, workDir)
	if err != nil {
		return refuse("trusted recovery configuration is unavailable: %v", err)
	}
	ag, err := newPipelineAgent(ctx, cfg, exec.LookPath)
	if err != nil {
		return refuse("pipeline agent is unavailable: %v", err)
	}
	closeAgent := true
	defer func() {
		if closeAgent {
			_ = ag.Close()
		}
	}()
	if cfg.SessionReuse {
		if err := validateRecoveredSessionProviders(m.db, run.ID, ag); err != nil {
			return refuse("stored agent sessions are incompatible: %v", err)
		}
	}

	// Repeat mutable Git checks immediately before the database claim. Git refs
	// and SQLite cannot share one transaction, so recovery also verifies them
	// again after binding and before executor registration.
	if _, _, err := m.validateInterruptedPipelineCopy(ctx, repo, run); err != nil {
		return refuse("pipeline copy changed before claim: %v", err)
	}
	restored, err := m.db.RestoreLegacyInterruptedGate(run.ID, repoID, branch, run.HeadSHA, localHead, strings.TrimSpace(intent), canonicalRef, expected)
	if err != nil {
		return refuse("%v", err)
	}
	if err := sourceprovenance.BindCandidateIfUnchanged(ctx, workDir, canonicalRef, restored.Run.HeadSHA, restored.Run.HeadSHA); err != nil {
		cleanupErr := m.failClaimedInterruptedRecovery(restored, fmt.Sprintf("source-ref binding failed: %v", err))
		return refusePostClaim("post-claim source-ref binding failed", cleanupErr)
	}
	if err := sourceprovenance.VerifyCandidateBinding(ctx, workDir, canonicalRef, restored.Run.HeadSHA); err != nil {
		cleanupErr := m.failClaimedInterruptedRecovery(restored, fmt.Sprintf("source-ref verification failed: %v", err))
		return refusePostClaim("post-claim source-ref verification failed", cleanupErr)
	}
	if _, _, err := m.validateInterruptedPipelineCopy(ctx, repo, restored.Run); err != nil {
		cleanupErr := m.failClaimedInterruptedRecovery(restored, fmt.Sprintf("pipeline copy changed after claim: %v", err))
		return refusePostClaim("post-claim pipeline copy verification failed", cleanupErr)
	}
	if err := pipeline.ValidateRecoveredRun(m.db, restored.Run, execSteps); err != nil {
		cleanupErr := m.failClaimedInterruptedRecovery(restored, fmt.Sprintf("restored gate validation failed: %v", err))
		return refusePostClaim("post-claim gate validation failed", cleanupErr)
	}

	plan := recoveredRunPlan{run: restored.Run, repo: repo, workDir: workDir, gateDir: gateDir, cfg: cfg, agent: ag, steps: execSteps}
	closeAgent = false
	m.resumeRecoveredRun(plan)
	return restored.Run.ID, true, nil
}

func interruptedReattachIdentityMatches(run *db.Run, localHead, intent string) bool {
	if run == nil || strings.TrimSpace(localHead) == "" || strings.TrimSpace(intent) == "" {
		return false
	}
	headMatches := run.SubmittedHeadSHA != nil && localHead == *run.SubmittedHeadSHA
	return headMatches && run.Intent != nil && run.IntentSource != nil && *run.IntentSource == db.RunIntentSourceAgent &&
		run.IntentScore != nil && *run.IntentScore == 1 && strings.TrimSpace(intent) == *run.Intent
}

func (m *RunManager) validateInterruptedPipelineCopy(ctx context.Context, repo *db.Repo, run *db.Run) (string, string, error) {
	workDir := m.paths.WorktreeDir(repo.ID, run.ID)
	info, err := os.Lstat(workDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("worktree is missing or not a real directory")
	}
	head, err := git.HeadSHA(ctx, workDir)
	if err != nil || head != run.HeadSHA {
		return "", "", fmt.Errorf("worktree head does not match recorded pipeline head")
	}
	gateDir := m.paths.RepoDir(repo.ID)
	commonDir, err := git.Run(ctx, workDir, "rev-parse", "--git-common-dir")
	if err != nil || !samePath(resolveGitPath(workDir, commonDir), gateDir) {
		return "", "", fmt.Errorf("worktree does not belong to its gate repository")
	}
	bare, err := git.Run(ctx, workDir, "rev-parse", "--is-bare-repository")
	if err != nil || bare != "false" {
		return "", "", fmt.Errorf("pipeline copy is not an isolated worktree")
	}
	if _, err := git.Run(ctx, gateDir, "rev-parse", "--verify", run.HeadSHA+"^{commit}"); err != nil {
		return "", "", fmt.Errorf("recorded pipeline head is unreachable in the local gate")
	}
	canonicalRef, err := sourceprovenance.CanonicalSourceRefFromBranch(run.Branch)
	if err != nil {
		return "", "", fmt.Errorf("durable branch identity is invalid")
	}
	if err := sourceprovenance.VerifyCandidateBinding(ctx, workDir, canonicalRef, run.HeadSHA); err != nil {
		return "", "", fmt.Errorf("pipeline source ref does not match recorded pipeline head")
	}
	dirty, err := git.HasUncommittedChanges(ctx, workDir)
	if err != nil {
		return "", "", fmt.Errorf("inspect worktree cleanliness: %w", err)
	}
	if dirty {
		return "", "", fmt.Errorf("worktree is dirty")
	}
	for _, marker := range []string{"rebase-merge", "rebase-apply", "MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "sequencer"} {
		path, err := git.Run(ctx, workDir, "rev-parse", "--git-path", marker)
		if err != nil {
			return "", "", fmt.Errorf("inspect in-progress Git state: %w", err)
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(workDir, path)
		}
		if _, err := os.Stat(path); err == nil {
			return "", "", fmt.Errorf("worktree has an in-progress Git operation")
		} else if !os.IsNotExist(err) {
			return "", "", fmt.Errorf("inspect in-progress Git state: %w", err)
		}
	}
	return workDir, gateDir, nil
}

func (m *RunManager) failClaimedInterruptedRecovery(restored *db.InterruptedGateRestore, detail string) error {
	if restored == nil || restored.Run == nil {
		return fmt.Errorf("claimed run snapshot is missing")
	}
	errMsg := "interrupted run recovery integrity check failed: " + detail
	if restored.Step == nil {
		statusErr := m.db.UpdateRunErrorStatus(restored.Run.ID, errMsg, types.RunFailed)
		clearErr := m.db.ClearRunAwaitingAgent(restored.Run.ID)
		if statusErr != nil || clearErr != nil {
			return fmt.Errorf("run fallback failed: status=%v clear=%v", statusErr, clearErr)
		}
		return nil
	}
	duration := int64(0)
	if restored.Step.DurationMS != nil {
		duration = *restored.Step.DurationMS
	}
	if err := m.db.FailClaimedInterruptedGate(restored.Run.ID, restored.Step.ID, errMsg, duration); err != nil {
		statusErr := m.db.UpdateRunErrorStatus(restored.Run.ID, errMsg, types.RunFailed)
		clearErr := m.db.ClearRunAwaitingAgent(restored.Run.ID)
		stepErr := m.db.FailStep(restored.Step.ID, errMsg, duration)
		return fmt.Errorf("transaction failed: %v; fallback status=%v clear=%v step=%v", err, statusErr, clearErr, stepErr)
	}
	return nil
}
