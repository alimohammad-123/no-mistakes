package steps

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/safeurl"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// PushStep force-pushes the worktree state to the configured push remote.
type PushStep struct {
	afterEvidenceClassification func(bool)
}

func (s *PushStep) Name() types.StepName { return types.StepPush }

func (s *PushStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	ctx := sctx.Ctx
	releaseCustody, err := sctx.AcquirePushCustody()
	if err != nil {
		return nil, err
	}
	defer releaseCustody()

	// Run format command if configured (before committing, so changes are formatted)
	if fmtCmd := sctx.Config.Commands.Format; fmtCmd != "" {
		if err := sctx.PreflightHeadMutation(); err != nil {
			return nil, err
		}
		sctx.Log(fmt.Sprintf("running formatter: %s", fmtCmd))
		output, exitCode, err := runStepShellCommand(sctx, fmtCmd)
		if err != nil {
			sctx.Log(fmt.Sprintf("warning: format command failed: %v", err))
		} else if exitCode != 0 {
			sctx.Log(fmt.Sprintf("warning: format command exited with code %d: %s", exitCode, output))
		}
	}

	// Commit any uncommitted changes from agent fixes
	if err := s.stageInRepoEvidence(sctx); err != nil {
		return nil, err
	}
	status, _ := git.Run(ctx, sctx.WorkDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		if err := sctx.PreflightHeadMutation(); err != nil {
			return nil, err
		}
		sctx.Log("committing agent changes...")
		if _, err := git.Run(ctx, sctx.WorkDir, "add", "-A"); err != nil {
			return nil, fmt.Errorf("stage agent changes: %w", err)
		}
		_, err := git.Run(ctx, sctx.WorkDir, "commit", "-m", "no-mistakes: apply agent fixes")
		if err != nil {
			return nil, fmt.Errorf("commit agent changes: %w", err)
		}
		if _, err := git.HeadSHA(ctx, sctx.WorkDir); err != nil {
			return nil, fmt.Errorf("resolve head after commit: %w", err)
		}
	}

	ref := normalizedBranchRef(sctx.Run.Branch)
	branch := strings.TrimPrefix(ref, "refs/heads/")

	pushURL := resolvePushURL(sctx)
	pushTarget := "upstream"
	usingFork := strings.TrimSpace(sctx.Repo.ForkURL) != ""
	if usingFork {
		pushTarget = "fork"
		sctx.Log(fmt.Sprintf("pushing to fork %s (%s)...", safeurl.Redact(pushURL), ref))
	} else {
		sctx.Log(fmt.Sprintf("pushing to %s (%s)...", safeurl.Redact(pushURL), ref))
	}

	headBeingPushed, err := git.HeadSHA(ctx, sctx.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("resolve head before push: %w", err)
	}
	if headBeingPushed != sctx.Run.HeadSHA {
		// Push owns final local formatting/evidence commits. Advance the durable
		// candidate and source ref before any network operation, then yield to the
		// executor when that new head lacks configured-Test proof.
		if err := sctx.AdvanceHeadSHAWithPushCustody(headBeingPushed); err != nil {
			return nil, fmt.Errorf("advance source ref after finalizing push candidate: %w", err)
		}
	}
	if sctx.Config.Commands.Test != "" && (sctx.Run.TestHeadSHA == nil || *sctx.Run.TestHeadSHA != headBeingPushed) {
		sctx.Log("final push candidate changed after configured Test; replaying validation before publication")
		return &pipeline.StepOutcome{ReplayValidation: true}, nil
	}
	if err := sctx.ValidateDeliveryCandidate(); err != nil {
		return nil, err
	}

	// Decide whether force-pushing would discard commits the pipeline never saw.
	// The lease is anchored to the remote-tracking ref the rebase step freshly
	// fetched (the exact commit this branch was rebased against), so a push that
	// would clobber an out-of-band or stale-mirror commit fails loudly instead
	// of silently dropping it. A bare --force-with-lease offers no protection
	// when pushing to a URL (no remote-tracking refs), so the anchor is explicit.
	lastSeen := lastFetchedBranchTip(ctx, sctx.WorkDir, branch, usingFork)
	gitRun := func(args ...string) (string, error) { return git.Run(ctx, sctx.WorkDir, args...) }
	decision, err := resolveForcePushDecision(gitRun, pushURL, ref, headBeingPushed, lastSeen, sctx.Run.BaseSHA)
	if err != nil {
		return nil, fmt.Errorf("push to %s: %w", pushTarget, err)
	}
	switch {
	case decision.newBranch:
		// New branch: regular push (no force needed).
		if err := git.PushSHA(ctx, sctx.WorkDir, pushURL, headBeingPushed, ref, "", false); err != nil {
			return nil, fmt.Errorf("push to %s: %w", pushTarget, err)
		}
	case decision.upToDate:
		// Remote already at this exact head. This freshly verified equality is a
		// successful binding even though no objects needed to move.
	default:
		// Existing branch: force-with-lease anchored to the verified remote head.
		if err := git.PushSHA(ctx, sctx.WorkDir, pushURL, headBeingPushed, ref, decision.remoteSHA, true); err != nil {
			return nil, fmt.Errorf("push to %s: %w", pushTarget, err)
		}
	}
	verifiedRemote, err := git.LsRemote(ctx, sctx.WorkDir, pushURL, ref)
	if err != nil || verifiedRemote != headBeingPushed {
		if err != nil {
			return nil, fmt.Errorf("verify successful push to %s: %w", pushTarget, err)
		}
		return nil, fmt.Errorf("verify successful push to %s: remote head %s does not equal pushed head %s", pushTarget, verifiedRemote, headBeingPushed)
	}
	if err := sctx.DB.UpdateRunPushBinding(sctx.Run.ID, db.PushBinding{
		HeadSHA:           headBeingPushed,
		TargetKind:        pushTarget,
		TargetFingerprint: branchsync.TargetFingerprint(pushURL),
		Ref:               ref,
	}); err != nil {
		return nil, err
	}

	headSHA, err := git.HeadSHA(ctx, sctx.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("resolve HEAD after push: %w", err)
	}
	if headSHA != sctx.Run.HeadSHA {
		if err := sctx.AdvanceHeadSHAWithPushCustody(headSHA); err != nil {
			return nil, fmt.Errorf("advance source ref after push: %w", err)
		}
	}

	sctx.Log("pushed successfully")
	return &pipeline.StepOutcome{}, nil
}

func (s *PushStep) stageInRepoEvidence(sctx *pipeline.StepContext) error {
	ctx := sctx.Ctx
	location := resolveTestEvidenceLocation(sctx.WorkDir, sctx.Run.Branch, sctx.Run.ID, sctx.Config.Test.Evidence)
	if !location.StoreInRepo {
		return nil
	}
	if gitIgnoresPath(ctx, sctx.WorkDir, location.Dir) {
		return nil
	}
	rel, err := filepath.Rel(sctx.WorkDir, location.Dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return nil
	}
	rel = filepath.ToSlash(rel)
	pending, err := inRepoEvidenceMutationPending(ctx, sctx.WorkDir, rel)
	if err != nil {
		return err
	}
	if s.afterEvidenceClassification != nil {
		s.afterEvidenceClassification(pending)
	}
	if !pending {
		return nil
	}
	if err := sctx.PreflightHeadMutation(); err != nil {
		return err
	}
	if _, err := git.Run(ctx, sctx.WorkDir, "add", "-f", "--", rel); err != nil {
		return fmt.Errorf("stage test evidence: %w", err)
	}
	return nil
}

func inRepoEvidenceMutationPending(ctx context.Context, workDir, rel string) (bool, error) {
	commands := [][]string{
		{"diff", "--name-only", "--", rel},
		{"diff", "--cached", "--name-only", "--", rel},
		{"ls-files", "--others", "--", rel},
	}
	for _, args := range commands {
		output, err := git.Run(ctx, workDir, args...)
		if err != nil {
			return false, fmt.Errorf("classify test evidence: %w", err)
		}
		if strings.TrimSpace(output) != "" {
			return true, nil
		}
	}
	return false, nil
}
