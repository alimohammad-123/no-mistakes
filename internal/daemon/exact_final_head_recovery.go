package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/pipeline/steps"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

var (
	prepareExactFinalHeadRecoveryRuntime        = defaultPrepareExactFinalHeadRecoveryRuntime
	validateExactFinalHeadRecoveryExternalState = steps.ValidateExactFinalHeadRecoveryExternalState
	reconcileStaleExactFinalHeadPushCustody     = steps.ReconcileStaleExactFinalHeadPushCustody
	addExactFinalHeadRecoveryWorktree           = git.WorktreeAdd
)

func (m *RunManager) reconcileRecoveredPushCustody(ctx context.Context, run *db.Run, repo *db.Repo, workDir string, execSteps []pipeline.Step) (*db.Run, error) {
	if run == nil || !run.PushActive {
		return run, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, hasExecutor := m.executors[run.ID]
	_, hasCancel := m.cancels[run.ID]
	_, hasDone := m.dones[run.ID]
	if hasExecutor || hasCancel || hasDone {
		return nil, fmt.Errorf("reconcile stale exact recovery Push custody: run still has a live executor owner")
	}
	reconciled, err := reconcileStaleExactFinalHeadPushCustody(
		ctx, m.db, run, repo, workDir, pipeline.MaxHeadValidationReplays, stepNames(execSteps),
	)
	if err != nil {
		return nil, err
	}
	if !reconciled {
		return nil, fmt.Errorf("reconcile stale exact recovery Push custody: custody was not reconciled")
	}
	refreshed, err := m.db.GetRun(run.ID)
	if err != nil {
		return nil, fmt.Errorf("reconcile stale exact recovery Push custody: reload run: %w", err)
	}
	if refreshed == nil || refreshed.PushActive {
		return nil, fmt.Errorf("reconcile stale exact recovery Push custody: durable custody remains active")
	}
	return refreshed, nil
}

func defaultPrepareExactFinalHeadRecoveryRuntime(ctx context.Context, manager *RunManager, run *db.Run, repo *db.Repo, workDir string) (*config.Config, agent.Agent, error) {
	cfg, err := manager.loadRecoveredConfig(ctx, run, repo, workDir)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Commands.Test == "" {
		return nil, nil, fmt.Errorf("exact final-head recovery requires the trusted configured Test command")
	}
	ag, err := newPipelineAgent(ctx, cfg, exec.LookPath)
	if err != nil {
		return nil, nil, err
	}
	if cfg.SessionReuse {
		if err := validateRecoveredSessionProviders(manager.db, run.ID, ag); err != nil {
			_ = ag.Close()
			return nil, nil, err
		}
	}
	return cfg, ag, nil
}

// HandleRecoverExactFinalHeadCapacity intentionally revives one terminal run
// only when its exact replay-capacity failure, source ref, gate ref, worktree,
// Test proof, earlier published branch, and stored PR identity still agree.
// The database claim appends recovery provenance before changing status. Any
// mismatch leaves the terminal run unchanged.
func (m *RunManager) HandleRecoverExactFinalHeadCapacity(ctx context.Context, repoID, runID string) (string, error) {
	if m.shuttingDown.Load() {
		return "", fmt.Errorf("exact final-head recovery refused: daemon is shutting down")
	}
	if strings.TrimSpace(repoID) == "" || strings.TrimSpace(runID) == "" {
		return "", fmt.Errorf("exact final-head recovery refused: repository and run are required")
	}
	run, err := m.db.GetRun(runID)
	if err != nil {
		return "", fmt.Errorf("exact final-head recovery refused: %w", err)
	}
	if run == nil || run.RepoID != repoID || strings.TrimSpace(run.Branch) == "" {
		return "", fmt.Errorf("exact final-head recovery refused: run is missing or belongs to another repository")
	}
	lockKey := repoID + "/" + run.Branch
	lockVal, _ := m.branchLocks.LoadOrStore(lockKey, &sync.Mutex{})
	branchMu := lockVal.(*sync.Mutex)
	branchMu.Lock()
	defer branchMu.Unlock()
	return m.handleRecoverExactFinalHeadCapacityLocked(ctx, repoID, runID)
}

func (m *RunManager) handleRecoverExactFinalHeadCapacityLocked(ctx context.Context, repoID, runID string) (string, error) {
	refuse := func(reason string, args ...any) (string, error) {
		return "", fmt.Errorf("exact final-head recovery refused: "+reason, args...)
	}
	run, err := m.db.GetRun(runID)
	if err != nil {
		return refuse("read run: %v", err)
	}
	if run == nil || run.RepoID != repoID {
		return refuse("run is missing or belongs to another repository")
	}
	if run.Status == types.RunRunning {
		m.mu.Lock()
		_, owned := m.executors[run.ID]
		m.mu.Unlock()
		if owned {
			if event, eventErr := m.db.GetRunRecoveryEvent(run.ID, db.RunRecoveryExactFinalHeadCapacity); eventErr == nil && event != nil {
				return run.ID, nil
			}
		}
		if err := m.db.ValidateActiveExactFinalHeadCapacityRecovery(run.ID, pipeline.MaxHeadValidationReplays, stepNames(m.steps())); err != nil {
			return refuse("run is active but not an exact capacity recovery: %v", err)
		}
		return run.ID, nil
	}
	failure, err := m.db.InspectExactFinalHeadCapacityFailure(run.ID, pipeline.MaxHeadValidationReplays, stepNames(m.steps()))
	if err != nil {
		return refuse("%v", err)
	}
	if active, err := m.db.GetActiveRun(repoID, run.Branch); err != nil {
		return refuse("read active branch owner: %v", err)
	} else if active != nil {
		return refuse("another active run owns the branch")
	}
	m.mu.Lock()
	_, hasExecutor := m.executors[run.ID]
	_, hasCancel := m.cancels[run.ID]
	_, hasDone := m.dones[run.ID]
	m.mu.Unlock()
	if hasExecutor || hasCancel || hasDone {
		return refuse("run still has a live executor owner")
	}
	repo, err := m.db.GetRepo(repoID)
	if err != nil {
		return refuse("read repository: %v", err)
	}
	if repo == nil {
		return refuse("repository is missing")
	}
	gateDir := m.paths.RepoDir(repo.ID)
	if info, err := os.Lstat(gateDir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return refuse("gate repository is missing or not a real directory")
	}
	if resolved, err := git.ResolveRef(ctx, gateDir, failure.CanonicalRef); err != nil || resolved != run.HeadSHA {
		return refuse("gate source ref does not match the exact candidate")
	}
	if resolved, err := git.Run(ctx, gateDir, "rev-parse", "--verify", run.HeadSHA+"^{commit}"); err != nil || strings.TrimSpace(resolved) != run.HeadSHA {
		return refuse("exact candidate is not a reachable gate commit")
	}

	workDir := m.paths.WorktreeDir(repo.ID, run.ID)
	created, err := m.ensureExactFinalHeadRecoveryWorktree(ctx, run, gateDir, workDir)
	if err != nil {
		return refuse("%v", err)
	}
	cleanupCreated := func(primary error) (string, error) {
		if !created {
			return refuse("%v", primary)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cleanupErr := git.WorktreeRemove(cleanupCtx, gateDir, workDir); cleanupErr != nil {
			return refuse("%v; reconstructed worktree cleanup failed: %v", primary, cleanupErr)
		}
		return refuse("%v", primary)
	}
	if created {
		if err := git.CopyLocalUserIdentity(ctx, repo.WorkingPath, workDir); err != nil {
			return cleanupCreated(fmt.Errorf("copy registered commit identity: %w", err))
		}
	}
	if err := m.validateExactFinalHeadRecoveryWorktree(ctx, run, gateDir, workDir); err != nil {
		return cleanupCreated(err)
	}
	cfg, ag, err := prepareExactFinalHeadRecoveryRuntime(ctx, m, run, repo, workDir)
	if err != nil {
		return cleanupCreated(fmt.Errorf("prepare trusted runtime: %w", err))
	}
	closeAgent := true
	defer func() {
		if closeAgent {
			_ = ag.Close()
		}
	}()
	if cfg == nil || cfg.Commands.Test == "" {
		return cleanupCreated(fmt.Errorf("trusted configured Test command is missing"))
	}
	if err := validateExactFinalHeadRecoveryExternalState(ctx, m.db, run, repo, workDir, cfg, false); err != nil {
		return cleanupCreated(fmt.Errorf("external delivery state: %w", err))
	}
	if m.shuttingDown.Load() {
		return cleanupCreated(fmt.Errorf("daemon began shutting down before claim"))
	}
	if err := m.validateExactFinalHeadRecoveryWorktree(ctx, run, gateDir, workDir); err != nil {
		return cleanupCreated(fmt.Errorf("state changed before claim: %w", err))
	}
	reinspected, err := m.db.InspectExactFinalHeadCapacityFailure(run.ID, pipeline.MaxHeadValidationReplays, stepNames(m.steps()))
	if err != nil || reinspected.EvidenceToken != failure.EvidenceToken {
		if err == nil {
			err = fmt.Errorf("durable evidence changed before claim")
		}
		return cleanupCreated(err)
	}
	if err := validateExactFinalHeadRecoveryExternalState(ctx, m.db, run, repo, workDir, cfg, false); err != nil {
		return cleanupCreated(fmt.Errorf("external delivery state changed before claim: %w", err))
	}
	restored, err := m.db.RestoreExactFinalHeadCapacityFailure(run.ID, failure.EvidenceToken, pipeline.MaxHeadValidationReplays, stepNames(m.steps()))
	if err != nil {
		return cleanupCreated(err)
	}
	failClaimed := func(primary error) (string, error) {
		errMsg := "exact final-head recovery integrity check failed: " + primary.Error()
		statusErr := m.db.UpdateRunErrorStatus(restored.ID, errMsg, types.RunFailed)
		if created {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if cleanupErr := git.WorktreeRemove(cleanupCtx, gateDir, workDir); cleanupErr != nil {
				return refuse("%v; terminalization=%v; cleanup=%v", primary, statusErr, cleanupErr)
			}
		}
		if statusErr != nil {
			return refuse("%v; terminalization=%v", primary, statusErr)
		}
		return refuse("%v", primary)
	}
	if err := m.db.ValidateActiveExactFinalHeadCapacityRecovery(restored.ID, pipeline.MaxHeadValidationReplays, stepNames(m.steps())); err != nil {
		return failClaimed(err)
	}
	if err := m.validateExactFinalHeadRecoveryWorktree(ctx, restored, gateDir, workDir); err != nil {
		return failClaimed(err)
	}

	execSteps := m.steps()
	plan := recoveredRunPlan{
		run: restored, repo: repo, workDir: workDir, gateDir: gateDir,
		cfg: cfg, agent: ag, steps: execSteps, headValidation: true,
	}
	closeAgent = false
	m.resumeRecoveredRun(plan)
	return restored.ID, nil
}

func stepNames(steps []pipeline.Step) []types.StepName {
	names := make([]types.StepName, len(steps))
	for index, step := range steps {
		names[index] = step.Name()
	}
	return names
}

func (m *RunManager) ensureExactFinalHeadRecoveryWorktree(ctx context.Context, run *db.Run, gateDir, workDir string) (bool, error) {
	info, err := os.Lstat(workDir)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("recovery worktree path is not a real directory")
		}
		return false, nil
	}
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("inspect recovery worktree path: %w", err)
	}
	root := filepath.Clean(m.paths.WorktreesDir())
	parent := filepath.Clean(filepath.Dir(workDir))
	if parent != filepath.Join(root, run.RepoID) || filepath.Base(workDir) != run.ID {
		return false, fmt.Errorf("recovery worktree path is outside its canonical location")
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return false, fmt.Errorf("create recovery worktree parent: %w", err)
	}
	if err := addExactFinalHeadRecoveryWorktree(ctx, gateDir, workDir, run.HeadSHA); err != nil {
		return false, fmt.Errorf("reconstruct exact candidate worktree: %w", err)
	}
	return true, nil
}

func (m *RunManager) validateExactFinalHeadRecoveryWorktree(ctx context.Context, run *db.Run, gateDir, workDir string) error {
	info, err := os.Lstat(workDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("recovery worktree is missing or not a real directory")
	}
	head, err := git.HeadSHA(ctx, workDir)
	if err != nil || head != run.HeadSHA {
		return fmt.Errorf("recovery worktree head does not match the exact candidate")
	}
	status, err := git.Run(ctx, workDir, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil || strings.TrimSpace(status) != "" {
		return fmt.Errorf("recovery worktree is not clean")
	}
	branch, err := git.Run(ctx, workDir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil || strings.TrimSpace(branch) != "HEAD" {
		return fmt.Errorf("recovery worktree is not detached")
	}
	commonDir, err := git.Run(ctx, workDir, "rev-parse", "--git-common-dir")
	if err != nil || !samePath(resolveGitPath(workDir, commonDir), gateDir) {
		return fmt.Errorf("recovery worktree does not belong to its gate repository")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		return err
	}
	if err := sourceprovenance.VerifyCandidateBinding(ctx, workDir, ref, run.HeadSHA); err != nil {
		return fmt.Errorf("recovery source ref does not match the exact candidate: %w", err)
	}
	if transition, err := m.db.GetRunHeadTransition(run.ID); err != nil || transition != nil {
		return fmt.Errorf("recovery head transition is pending or unreadable")
	}
	return nil
}
