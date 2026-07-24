package steps

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/safeurl"
	"github.com/kunchenguid/no-mistakes/internal/scm"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// PushStep force-pushes the worktree state to the configured push remote.
type PushStep struct {
	afterEvidenceClassification func(bool)
	beforeRemoteMutation        func()
	ownershipClaimed            func()
	recoveryRefObserved         func(*pipeline.StepContext, string) (string, error)
	invocationMarked            func() error
	successReceiptWritten       func() error
}

func (s *PushStep) Name() types.StepName { return types.StepPush }

func (s *PushStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	ctx := sctx.Ctx
	releaseCustody, err := sctx.AcquirePushCustody()
	if err != nil {
		return nil, err
	}
	defer releaseCustody()

	alreadyBound, err := sctx.DB.ExactRecoveryPushAlreadyBound(sctx.Run.ID, sctx.Run.HeadSHA)
	if err != nil {
		return nil, err
	}
	if alreadyBound {
		ref := normalizedBranchRef(sctx.Run.Branch)
		pushURL := resolvePushURL(sctx)
		if err := sctx.WithDeliverySourceOwnership(func() error {
			operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
			if err != nil {
				return err
			}
			remoteHead, err := exactRecoveryPushRemoteHead(sctx, pushURL, ref, operation)
			if err != nil {
				return fmt.Errorf("verify recovered exact push binding: %w", err)
			}
			if remoteHead != sctx.Run.HeadSHA {
				if err := persistExactRecoveryUnexpectedRemoteOID(sctx.DB, sctx.Run, ref, remoteHead, sctx.Run.HeadSHA); err != nil {
					return fmt.Errorf("verify recovered exact push binding: persist unexpected OID: %w", err)
				}
				return fmt.Errorf("verify recovered exact push binding: remote head %s does not equal candidate %s", remoteHead, sctx.Run.HeadSHA)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		sctx.Log("exact candidate already has its durable push binding")
		return &pipeline.StepOutcome{}, nil
	}

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
	recoveryEvent, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return nil, err
	}
	gitRun := func(args ...string) (string, error) { return git.Run(ctx, sctx.WorkDir, args...) }
	decision, err := resolveForcePushDecision(gitRun, pushURL, ref, headBeingPushed, lastSeen, sctx.Run.BaseSHA)
	if decision.remoteSHA != "" {
		allowedRemote := ""
		if recoveryEvent != nil {
			allowedRemote = recoveryEvent.LastPushedSHA
		}
		if persistErr := persistExactRecoveryUnexpectedRemoteOID(sctx.DB, sctx.Run, ref, decision.remoteSHA, allowedRemote); persistErr != nil {
			return nil, fmt.Errorf("push to %s: persist unexpected remote OID: %w", pushTarget, persistErr)
		}
	}
	if err != nil {
		if persistedErr := persistExactRecoveryRemoteRefError(sctx, nil, err); persistedErr != nil {
			err = persistedErr
		}
		return nil, fmt.Errorf("push to %s: %w", pushTarget, err)
	}
	if recoveryEvent != nil && decision.newBranch {
		var missingErr error = &git.RemoteRefObservationError{Ref: ref, Observation: git.RemoteRefMissing}
		if persistedErr := persistExactRecoveryRemoteRefError(sctx, nil, missingErr); persistedErr != nil {
			missingErr = persistedErr
		}
		return nil, fmt.Errorf("push to %s: %w", pushTarget, missingErr)
	}
	recoveryOperation, err := s.prepareExactRecoveryPushOperation(sctx, ref, headBeingPushed, decision.remoteSHA)
	if err != nil {
		return nil, err
	}
	if s.beforeRemoteMutation != nil {
		s.beforeRemoteMutation()
	}
	if err := sctx.WithDeliverySourceOwnership(func() error {
		if s.ownershipClaimed != nil {
			s.ownershipClaimed()
		}
		currentOperation, err := s.prepareExactRecoveryPushOperation(sctx, ref, headBeingPushed, decision.remoteSHA)
		if err != nil {
			return err
		}
		if (recoveryOperation == nil) != (currentOperation == nil) ||
			recoveryOperation != nil && recoveryOperation.OperationID != currentOperation.OperationID {
			return fmt.Errorf("exact recovery Push operation changed before invocation")
		}
		if currentOperation != nil {
			if err := sctx.DB.MarkExactRecoveryPushInvoked(
				sctx.Run.ID, currentOperation.OperationID, decision.remoteSHA,
			); err != nil {
				return err
			}
			if s.invocationMarked != nil {
				if err := s.invocationMarked(); err != nil {
					return err
				}
			}
		}
		var pushErr error
		switch {
		case decision.newBranch:
			pushErr = git.PushSHA(ctx, sctx.WorkDir, pushURL, headBeingPushed, ref, "", false)
		case decision.upToDate:
		case currentOperation != nil:
			pushErr = git.PushSHAWithReceipt(
				ctx, sctx.WorkDir, pushURL, headBeingPushed, ref,
				decision.remoteSHA, true, currentOperation.ReceiptRef,
			)
		default:
			pushErr = git.PushSHA(ctx, sctx.WorkDir, pushURL, headBeingPushed, ref, decision.remoteSHA, true)
		}
		if pushErr != nil {
			if recoveryEvent != nil {
				remoteObservation, observeErr := exactRecoveryLsRemote(sctx.Ctx, sctx.WorkDir, pushURL, ref)
				if observeErr != nil {
					return fmt.Errorf("push to %s: %w; verify failed Push remote: %v", pushTarget, pushErr, observeErr)
				}
				if remoteObservation.Invalid != "" {
					observationErr := &git.RemoteRefObservationError{Ref: ref, Observation: remoteObservation.Invalid}
					if err := persistExactRecoveryRemoteRefError(sctx, currentOperation, observationErr); err != nil {
						return fmt.Errorf("push to %s: %w; verify failed Push remote: %v", pushTarget, pushErr, err)
					}
				}
				if remoteObservation.OID != "" {
					attributed := false
					if remoteObservation.OID == recoveryEvent.HeadSHA {
						if receiptOID, receiptErr := exactRecoveryPushReceiptOID(sctx, currentOperation); receiptErr == nil {
							if err := sctx.DB.RecordExactRecoveryPushSuccessReceipt(
								sctx.Run.ID, currentOperation.OperationID,
								currentOperation.ReceiptRef, receiptOID,
							); err != nil {
								return fmt.Errorf("push to %s: %w; persist successful Push receipt: %v", pushTarget, pushErr, err)
							}
							pushErr = nil
							attributed = true
						} else {
							if err := persistExactRecoveryUnexpectedRemoteOID(
								sctx.DB, sctx.Run, ref, remoteObservation.OID,
								recoveryEvent.LastPushedSHA,
							); err != nil {
								return fmt.Errorf("push to %s: %w; persist unattributed target: %v", pushTarget, pushErr, err)
							}
						}
					}
					if !attributed {
						if err := persistExactRecoveryUnexpectedRemoteOID(
							sctx.DB, sctx.Run, ref, remoteObservation.OID,
							recoveryEvent.LastPushedSHA,
						); err != nil {
							return fmt.Errorf("push to %s: %w; verify failed Push remote: %v", pushTarget, pushErr, err)
						}
					}
				}
			}
			if pushErr == nil {
				goto verifyPush
			}
			return fmt.Errorf("push to %s: %w", pushTarget, pushErr)
		}
		if currentOperation != nil {
			if s.successReceiptWritten != nil {
				if err := s.successReceiptWritten(); err != nil {
					return err
				}
			}
			receiptOID, err := exactRecoveryPushReceiptOID(sctx, currentOperation)
			if err != nil {
				return fmt.Errorf("verify successful push to %s: %w", pushTarget, err)
			}
			if err := sctx.DB.RecordExactRecoveryPushSuccessReceipt(
				sctx.Run.ID, currentOperation.OperationID,
				currentOperation.ReceiptRef, receiptOID,
			); err != nil {
				return err
			}
		}
	verifyPush:
		verifiedRemote, err := exactRecoveryPushRemoteHead(sctx, pushURL, ref, currentOperation)
		if err != nil {
			return fmt.Errorf("verify successful push to %s: %w", pushTarget, err)
		}
		if verifiedRemote != headBeingPushed {
			allowedOIDs := []string{headBeingPushed}
			if currentOperation != nil {
				allowedOIDs = append(allowedOIDs, currentOperation.StaleOID)
			}
			if err := persistExactRecoveryUnexpectedRemoteOID(sctx.DB, sctx.Run, ref, verifiedRemote, allowedOIDs...); err != nil {
				return fmt.Errorf("verify successful push to %s: persist unexpected OID: %w", pushTarget, err)
			}
			return fmt.Errorf("verify successful push to %s: remote head %s does not equal pushed head %s", pushTarget, verifiedRemote, headBeingPushed)
		}
		binding := db.PushBinding{
			HeadSHA:           headBeingPushed,
			TargetKind:        pushTarget,
			TargetFingerprint: branchsync.TargetFingerprint(pushURL),
			Ref:               ref,
		}
		if currentOperation != nil {
			return sctx.DB.BindExactRecoveryPushOperation(
				sctx.Run.ID, currentOperation.OperationID, binding,
			)
		}
		return sctx.DB.UpdateRunPushBinding(sctx.Run.ID, binding)
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

func exactRecoveryPushRemoteHead(sctx *pipeline.StepContext, pushURL, ref string, operation *db.ExactRecoveryPushOperation) (string, error) {
	observation, err := exactRecoveryLsRemote(sctx.Ctx, sctx.WorkDir, pushURL, ref)
	if err != nil {
		return "", err
	}
	if observation.Invalid == "" {
		return observation.OID, nil
	}
	observationErr := &git.RemoteRefObservationError{Ref: ref, Observation: observation.Invalid}
	if err := persistExactRecoveryRemoteRefError(sctx, operation, observationErr); err != nil {
		return "", err
	}
	return "", observationErr
}

func persistExactRecoveryRemoteRefError(sctx *pipeline.StepContext, operation *db.ExactRecoveryPushOperation, remoteErr error) error {
	var observationErr *git.RemoteRefObservationError
	if !errors.As(remoteErr, &observationErr) {
		return nil
	}
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return fmt.Errorf("%w; read exact recovery provenance: %v", remoteErr, err)
	}
	if event == nil {
		return remoteErr
	}
	if err := sctx.DB.RecordExactRecoveryRemoteRefAmbiguity(sctx.Run.ID, observationErr.Observation); err != nil {
		return fmt.Errorf("%w; persist exact recovery remote ambiguity: %v", remoteErr, err)
	}
	if operation != nil {
		if err := sctx.DB.RecordExactRecoveryRefObservation(sctx.Run.ID, observationErr.Observation); err != nil {
			return fmt.Errorf("%w; persist provider recovery remote ambiguity: %v", remoteErr, err)
		}
	}
	return remoteErr
}

func (s *PushStep) prepareExactRecoveryPushOperation(sctx *pipeline.StepContext, ref, expectedHead, observedRemote string) (*db.ExactRecoveryPushOperation, error) {
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil || event == nil {
		return nil, err
	}
	provider := scm.DetectProviderContext(sctx.Ctx, sctx.Repo.UpstreamURL)
	if provider == scm.ProviderUnknown && sctx.Run.PRURL != nil {
		provider = scm.DetectProviderContext(sctx.Ctx, *sctx.Run.PRURL)
	}
	observed := strings.TrimSpace(observedRemote)
	if provider == scm.ProviderAzureDevOps {
		observe := s.recoveryRefObserved
		if observe == nil {
			observe = observeExactRecoveryAzureSourceRef
		}
		observed, err = observe(sctx, event.LastPushedSHA)
		if err != nil {
			return nil, fmt.Errorf("read Azure recovery source ref before Push: %w", err)
		}
	}
	if observed == "" {
		observed = "missing"
	}
	_, err = sctx.DB.PrepareExactRecoveryRefObservation(
		sctx.Run.ID, string(provider), ref, expectedHead, observed,
		time.Now().Add(exactRecoveryRefReconcileWindow).Unix(),
	)
	if err != nil {
		return nil, fmt.Errorf("prepare exact recovery source ref observation: %w", err)
	}
	operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
	if err != nil {
		return nil, err
	}
	if operation == nil || operation.Phase != db.ExactRecoveryPushPrepared {
		return nil, fmt.Errorf("prepare exact recovery Push operation: durable prepared phase is missing")
	}
	return operation, nil
}

func exactRecoveryPushReceiptOID(sctx *pipeline.StepContext, operation *db.ExactRecoveryPushOperation) (string, error) {
	if operation == nil || strings.TrimSpace(operation.ReceiptRef) == "" {
		return "", fmt.Errorf("exact recovery Push success receipt is missing")
	}
	oid, err := git.Run(sctx.Ctx, sctx.WorkDir, "rev-parse", "--verify", operation.ReceiptRef)
	if err != nil {
		return "", fmt.Errorf("exact recovery Push success receipt is missing: %w", err)
	}
	oid = strings.TrimSpace(oid)
	if oid != operation.TargetOID {
		return "", fmt.Errorf("exact recovery Push success receipt is %s, want %s", oid, operation.TargetOID)
	}
	return oid, nil
}

func observeExactRecoveryAzureSourceRef(sctx *pipeline.StepContext, expected string) (string, error) {
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil || event == nil {
		return "", err
	}
	host, reason := buildHost(sctx, scm.ProviderAzureDevOps)
	if host == nil {
		return "", fmt.Errorf("build Azure recovery host: %s", reason)
	}
	if err := host.Available(sctx.Ctx); err != nil {
		return "", fmt.Errorf("check Azure recovery host: %w", err)
	}
	branch := strings.TrimPrefix(sctx.Run.Branch, "refs/heads/")
	existing, err := host.FindPR(sctx.Ctx, branch, sctx.BaseBranch())
	if err != nil {
		return "", fmt.Errorf("rediscover Azure recovery PR: %w", err)
	}
	if existing == nil || sctx.Run.PRURL == nil ||
		strings.TrimSpace(existing.URL) != strings.TrimSpace(*sctx.Run.PRURL) {
		return "", fmt.Errorf("stored Azure recovery PR identity is missing or changed")
	}
	reader, ok := host.(scm.PRSnapshotReader)
	if !host.Capabilities().RecoverySnapshot || !ok {
		return "", fmt.Errorf("Azure provider lacks authoritative exact recovery PR snapshots")
	}
	request := scm.PRSnapshotRequest{ExpectedHead: expected}
	observation, err := sctx.DB.GetExactRecoveryRefObservation(sctx.Run.ID)
	if err != nil {
		return "", err
	}
	if observation != nil {
		request.RecordObservation = func(_ context.Context, observed string) error {
			return sctx.DB.RecordExactRecoveryRefObservation(sctx.Run.ID, observed)
		}
	}
	snapshot, err := reader.GetPRSnapshot(sctx.Ctx, existing, request)
	if err != nil {
		return "", err
	}
	if err := validateExactRecoveryPRSnapshot(
		sctx, existing, *sctx.Run.PRURL, expected, reader.ExpectedRepository(), snapshot,
	); err != nil {
		return "", err
	}
	if snapshot.HeadSHA != event.LastPushedSHA {
		return "", fmt.Errorf("Azure recovery source ref changed before Push")
	}
	return snapshot.HeadSHA, nil
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
