package steps

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/scm"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

var (
	exactRecoveryReconcilePoint                   = func(string) {}
	exactRecoveryReconcileNow                     = time.Now
	exactRecoveryLsRemote                         = git.LsRemoteExact
	exactRecoveryCheckAtomicPushReceiptCapability = git.CheckAtomicPushReceiptCapability
	exactRecoveryReconcileWait                    = func(ctx context.Context, delay time.Duration) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			return nil
		}
	}
)

const (
	exactRecoveryRefReconcileWindow       = 5 * time.Second
	exactRecoveryRefReconcilePollInterval = 100 * time.Millisecond
)

func persistExactRecoveryUnexpectedRemoteOID(database *db.DB, run *db.Run, sourceRef, observedOID string, allowedOIDs ...string) error {
	if database == nil || run == nil {
		return fmt.Errorf("persist exact recovery unexpected remote OID: recovery context is incomplete")
	}
	event, err := database.GetRunRecoveryEvent(run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	for _, allowedOID := range allowedOIDs {
		if observedOID == allowedOID {
			return nil
		}
	}
	if err := database.RecordExactRecoveryUnexpectedRemoteOID(run.ID, sourceRef, observedOID); err != nil {
		return err
	}
	return fmt.Errorf("exact recovery remote ref observed unexpected OID %s", observedOID)
}

func validateBoundExactRecoveryPushReceipt(sctx *pipeline.StepContext) error {
	return validateBoundExactRecoveryPushReceiptState(sctx, false)
}

func validateBoundExactRecoveryPushReceiptState(sctx *pipeline.StepContext, allowMissingTarget bool) error {
	if sctx == nil || sctx.Run == nil || sctx.Repo == nil || sctx.DB == nil {
		return fmt.Errorf("validate exact recovery Push receipt: recovery context is incomplete")
	}
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil || event == nil {
		return err
	}
	if err := sctx.DB.CheckExactRecoveryRemoteRefAmbiguity(sctx.Run.ID); err != nil {
		return err
	}
	operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
	if err != nil {
		return err
	}
	if operation == nil || operation.Phase != db.ExactRecoveryPushBound ||
		operation.ReceiptOID == nil || *operation.ReceiptOID != operation.TargetOID {
		return fmt.Errorf("validate exact recovery Push receipt: bound operation provenance is incomplete")
	}
	pushURL := resolvePushURL(sctx)
	targetObservation, err := exactRecoveryLsRemote(
		sctx.Ctx, sctx.WorkDir, pushURL, operation.SourceRef,
	)
	if err != nil {
		return fmt.Errorf("validate exact recovery Push target: %w", err)
	}
	receiptObservation, err := exactRecoveryPushReceiptObservation(sctx, pushURL, operation)
	if err != nil {
		return err
	}
	if targetObservation.Invalid != "" {
		if allowMissingTarget && targetObservation.Invalid == git.RemoteRefMissing {
			targetObservation.Invalid = ""
		} else {
			observationErr := &git.RemoteRefObservationError{
				Ref: operation.SourceRef, Observation: targetObservation.Invalid,
			}
			if err := persistExactRecoveryRemoteRefError(sctx, operation, observationErr); err != nil {
				return err
			}
			return observationErr
		}
	}
	if receiptObservation.Invalid != "" {
		observationErr := &git.RemoteRefObservationError{
			Ref: operation.ReceiptRef, Observation: receiptObservation.Invalid,
		}
		if err := persistExactRecoveryRemoteRefError(sctx, operation, observationErr); err != nil {
			return err
		}
		return observationErr
	}
	if targetObservation.OID != "" && targetObservation.OID != operation.TargetOID {
		return persistExactRecoveryUnexpectedRemoteOID(
			sctx.DB, sctx.Run, operation.SourceRef, targetObservation.OID,
		)
	}
	if receiptObservation.OID != operation.TargetOID {
		return persistExactRecoveryUnexpectedRemoteOID(
			sctx.DB, sctx.Run, operation.SourceRef, receiptObservation.OID,
		)
	}
	return nil
}

// ValidateExactFinalHeadRecoveryExternalState proves that the previously
// published branch and PR still have the identities frozen on the failed run.
// Recovery may then resume the unpublished exact candidate without replacing
// the PR or claiming that the candidate was already delivered.
func ValidateExactFinalHeadRecoveryExternalState(ctx context.Context, database *db.DB, run *db.Run, repo *db.Repo, workDir string, cfg *config.Config, allowExactPublished bool) error {
	if run == nil || repo == nil || cfg == nil || run.PRURL == nil || run.LastPushedSHA == nil {
		return fmt.Errorf("exact final-head recovery external state is incomplete")
	}
	if database == nil {
		return fmt.Errorf("exact final-head recovery database is missing")
	}
	if err := database.CheckExactRecoveryRemoteRefAmbiguity(run.ID); err != nil {
		return err
	}
	if strings.TrimSpace(*run.PRURL) == "" || strings.TrimSpace(*run.LastPushedSHA) == "" ||
		(!allowExactPublished && *run.LastPushedSHA == run.HeadSHA) {
		return fmt.Errorf("exact final-head recovery has no distinct earlier published head and PR")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		return err
	}
	sctx := &pipeline.StepContext{Ctx: ctx, Run: run, Repo: repo, WorkDir: workDir, Config: cfg, DB: database}
	pushURL := resolvePushURL(sctx)
	if strings.TrimSpace(pushURL) == "" {
		return fmt.Errorf("resolve exact final-head recovery push target: URL is empty")
	}
	expectedTargetKind := "upstream"
	if strings.TrimSpace(repo.ForkURL) != "" {
		expectedTargetKind = "fork"
	}
	if run.PushTargetKind == nil || *run.PushTargetKind != expectedTargetKind ||
		run.PushTargetFingerprint == nil || *run.PushTargetFingerprint != branchsync.TargetFingerprint(pushURL) {
		return fmt.Errorf("recorded push target no longer matches repository routing")
	}
	event, err := database.GetRunRecoveryEvent(run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	publishedObservation, err := exactRecoveryLsRemote(ctx, workDir, pushURL, ref)
	if err != nil {
		return fmt.Errorf("read exact final-head recovery published head: %w", err)
	}
	if publishedObservation.Invalid != "" {
		if event != nil {
			if persistErr := database.RecordExactRecoveryRemoteRefAmbiguity(run.ID, publishedObservation.Invalid); persistErr != nil {
				return fmt.Errorf(
					"read exact final-head recovery published head: %w; persist remote ambiguity: %v",
					&git.RemoteRefObservationError{Ref: ref, Observation: publishedObservation.Invalid},
					persistErr,
				)
			}
		}
		return fmt.Errorf(
			"read exact final-head recovery published head: %w",
			&git.RemoteRefObservationError{Ref: ref, Observation: publishedObservation.Invalid},
		)
	}
	publishedHead := publishedObservation.OID
	if _, err := sctx.BindSourceRef(); err != nil {
		return err
	}
	publishedMatchesRecorded := publishedHead == *run.LastPushedSHA
	pushRunning := false
	if allowExactPublished {
		results, err := database.GetStepsByRun(run.ID)
		if err != nil {
			return fmt.Errorf("read exact final-head recovery delivery phase: %w", err)
		}
		for _, result := range results {
			if result.StepName == types.StepPush {
				pushRunning = result.Status == types.StepStatusRunning
				break
			}
		}
	}
	publishedMatchesExact := allowExactPublished && publishedHead == run.HeadSHA &&
		(*run.LastPushedSHA == run.HeadSHA || pushRunning)
	if !publishedMatchesRecorded && !publishedMatchesExact {
		if event != nil {
			if persistErr := persistExactRecoveryUnexpectedRemoteOID(database, run, ref, publishedHead); persistErr != nil {
				return fmt.Errorf("published branch head matches neither recorded delivery phase; persist unexpected OID: %w", persistErr)
			}
		}
		return fmt.Errorf("published branch head matches neither recorded delivery phase")
	}
	if event != nil {
		operation, err := database.GetExactRecoveryPushOperation(run.ID)
		if err != nil {
			return err
		}
		if operation != nil && operation.Phase == db.ExactRecoveryPushBound {
			if err := validateBoundExactRecoveryPushReceipt(sctx); err != nil {
				return err
			}
		}
	}
	capabilityReceiptRef := db.ExactRecoveryPushCapabilityReceiptRef(
		run.ID, ref, *run.LastPushedSHA, run.HeadSHA, *run.PushTargetFingerprint,
	)
	if err := exactRecoveryCheckAtomicPushReceiptCapability(
		ctx, workDir, pushURL, run.HeadSHA, ref,
		*run.LastPushedSHA, capabilityReceiptRef,
	); err != nil {
		return fmt.Errorf("validate exact final-head recovery Push transport: %w", err)
	}

	provider := scm.DetectProviderContext(ctx, repo.UpstreamURL)
	if provider == scm.ProviderUnknown {
		provider = scm.DetectProviderContext(ctx, *run.PRURL)
	}
	host, reason := buildHost(sctx, provider)
	if host == nil {
		return fmt.Errorf("validate exact final-head recovery PR: %s", reason)
	}
	if err := host.Available(ctx); err != nil {
		return fmt.Errorf("validate exact final-head recovery PR availability: %w", err)
	}
	branch := strings.TrimPrefix(run.Branch, "refs/heads/")
	existing, err := host.FindPR(ctx, branch, run.EffectiveBaseBranch(repo))
	if err != nil {
		return fmt.Errorf("rediscover exact final-head recovery PR: %w", err)
	}
	if existing == nil || strings.TrimSpace(existing.URL) != strings.TrimSpace(*run.PRURL) {
		return fmt.Errorf("stored PR identity is missing or changed")
	}
	snapshot, err := validateExactRecoveryPRAdmission(ctx, sctx, host, existing, *run.PRURL, publishedHead)
	if err != nil {
		return err
	}
	update, err := database.GetExactRecoveryPRUpdate(run.ID)
	if err != nil {
		return err
	}
	if update != nil {
		contentHash := db.ExactRecoveryPRContentHash(snapshot.Title, snapshot.Body)
		switch update.State {
		case db.ExactRecoveryPRUpdatePrepared:
			if contentHash != update.PriorContentHash && contentHash != update.IntendedContentHash {
				return fmt.Errorf("exact recovery PR content is stale, partial, or superseded")
			}
		case db.ExactRecoveryPRUpdateApplied:
			if contentHash != update.IntendedContentHash {
				return fmt.Errorf("applied exact recovery PR content changed")
			}
		default:
			return fmt.Errorf("exact recovery PR update phase is invalid")
		}
	}
	if _, err := sctx.BindSourceRef(); err != nil {
		return err
	}
	return nil
}

func validateExactRecoveryPRAdmission(ctx context.Context, sctx *pipeline.StepContext, host scm.Host, existing *scm.PR, expectedURL, expectedHead string) (scm.PRSnapshot, error) {
	if host == nil || existing == nil {
		return scm.PRSnapshot{}, fmt.Errorf("exact recovery PR host or identity is missing")
	}
	snapshotReader, ok := host.(scm.PRSnapshotReader)
	if !host.Capabilities().RecoverySnapshot || !ok {
		return scm.PRSnapshot{}, fmt.Errorf("provider %s lacks authoritative exact recovery PR snapshots", host.Provider())
	}
	request, err := exactRecoveryPRSnapshotRequest(sctx, expectedHead)
	if err != nil {
		return scm.PRSnapshot{}, err
	}
	snapshot, err := snapshotReader.GetPRSnapshot(ctx, existing, request)
	if err != nil {
		return scm.PRSnapshot{}, fmt.Errorf("read exact final-head recovery PR snapshot: %w", err)
	}
	if err := validateExactRecoveryPRSnapshot(
		sctx, existing, expectedURL, expectedHead, snapshotReader.ExpectedRepository(), snapshot,
	); err != nil {
		return scm.PRSnapshot{}, err
	}
	return snapshot, nil
}

func exactRecoveryPRSnapshotRequest(sctx *pipeline.StepContext, expectedHead string) (scm.PRSnapshotRequest, error) {
	request := scm.PRSnapshotRequest{ExpectedHead: strings.TrimSpace(expectedHead)}
	if sctx == nil || sctx.Run == nil || sctx.DB == nil {
		return request, nil
	}
	persisted, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		return scm.PRSnapshotRequest{}, fmt.Errorf("read exact recovery push visibility bound: %w", err)
	}
	provider := scm.DetectProviderContext(sctx.Ctx, sctx.Repo.UpstreamURL)
	if provider == scm.ProviderUnknown && sctx.Run.PRURL != nil {
		provider = scm.DetectProviderContext(sctx.Ctx, *sctx.Run.PRURL)
	}
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return scm.PRSnapshotRequest{}, fmt.Errorf("read exact recovery provenance for ref visibility: %w", err)
	}
	if persisted == nil || persisted.LastPushedSHA == nil || persisted.LastPushedAt == nil ||
		strings.TrimSpace(*persisted.LastPushedSHA) != request.ExpectedHead {
		if provider == scm.ProviderAzureDevOps && event != nil && request.ExpectedHead == event.HeadSHA {
			return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery Azure target lacks its durable Push binding")
		}
		return request, nil
	}
	if provider == scm.ProviderAzureDevOps && event != nil {
		observation, err := sctx.DB.GetExactRecoveryRefObservation(sctx.Run.ID)
		if err != nil {
			return scm.PRSnapshotRequest{}, err
		}
		operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
		if err != nil {
			return scm.PRSnapshotRequest{}, err
		}
		if observation != nil && (observation.Provider != string(scm.ProviderAzureDevOps) ||
			observation.SourceRef != event.SourceRef || observation.StaleOID != event.LastPushedSHA ||
			observation.ExpectedOID != event.HeadSHA || observation.DeadlineAt <= 0) {
			return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery Azure ref observation journal is missing or inconsistent")
		}
		if (observation == nil) != (operation == nil) {
			return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery Azure Push operation journal is incomplete")
		}
		if observation != nil && observation.State == db.ExactRecoveryRefObservationAmbiguous {
			return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery Azure ref observation journal is ambiguous")
		}
		if request.ExpectedHead == event.HeadSHA {
			if observation == nil || operation.Phase != db.ExactRecoveryPushBound ||
				operation.SourceRef != event.SourceRef || operation.StaleOID != event.LastPushedSHA ||
				operation.TargetOID != event.HeadSHA ||
				operation.TargetKind != event.PushTargetKind ||
				operation.TargetFingerprint != event.PushTargetFingerprint ||
				operation.PriorGeneration != event.PushGeneration ||
				operation.TargetGeneration != event.PushGeneration+1 ||
				operation.PriorPushedAt != event.LastPushedAt ||
				operation.BoundAt == nil || persisted.PushGeneration == nil ||
				*persisted.PushGeneration != operation.TargetGeneration ||
				persisted.LastPushedAt == nil || *persisted.LastPushedAt != *operation.BoundAt {
				return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery Azure ref observation journal is missing or inconsistent")
			}
			request.AllowedStaleHead = observation.StaleOID
			request.ReconcileUntil = time.Unix(observation.DeadlineAt, 0)
			request.RecordObservation = func(_ context.Context, observed string) error {
				return sctx.DB.RecordExactRecoveryRefObservation(sctx.Run.ID, observed)
			}
			return request, nil
		}
	}
	pushedAt := time.Unix(*persisted.LastPushedAt, 0)
	if pushedAt.After(time.Now().Add(time.Second)) {
		return scm.PRSnapshotRequest{}, fmt.Errorf("exact recovery push visibility bound is in the future")
	}
	request.ReconcileUntil = pushedAt.Add(exactRecoveryRefReconcileWindow)
	return request, nil
}

func ReconcileStaleExactFinalHeadPushCustody(ctx context.Context, database *db.DB, run *db.Run, repo *db.Repo, workDir string, maxReplays int, expected []types.StepName) (bool, error) {
	if database == nil || run == nil || repo == nil || !run.PushActive {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: recovery context is incomplete")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		return false, err
	}
	sctx := &pipeline.StepContext{Ctx: ctx, Run: run, Repo: repo, WorkDir: workDir, DB: database}
	pushURL := resolvePushURL(sctx)
	if strings.TrimSpace(pushURL) == "" {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: canonical push URL is empty")
	}
	expectedTargetKind := "upstream"
	if strings.TrimSpace(repo.ForkURL) != "" {
		expectedTargetKind = "fork"
	}
	if run.PushTargetKind == nil || *run.PushTargetKind != expectedTargetKind ||
		run.PushTargetFingerprint == nil || *run.PushTargetFingerprint != branchsync.TargetFingerprint(pushURL) ||
		run.PushRef == nil || *run.PushRef != ref {
		return false, fmt.Errorf("reconcile stale exact recovery Push custody: canonical push target changed")
	}
	for {
		remoteObservation, err := exactRecoveryLsRemote(ctx, workDir, pushURL, ref)
		if err != nil {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: read canonical remote: %w", err)
		}
		remoteHead := remoteObservation.Value()
		exactRecoveryReconcilePoint("remote-probed")
		if _, err := sctx.BindSourceRef(); err != nil {
			return false, err
		}
		exactRecoveryReconcilePoint("source-verified")
		if _, err := sctx.BindSourceRef(); err != nil {
			return false, err
		}
		operation, err := database.GetExactRecoveryPushOperation(run.ID)
		if err != nil {
			return false, err
		}
		if operation != nil && operation.Phase != db.ExactRecoveryPushBound {
			attributed, err := reconcileExactRecoveryPushReceiptState(
				sctx, pushURL, operation, remoteHead,
			)
			if err != nil {
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: %w", err)
			}
			if attributed {
				operation, err = database.GetExactRecoveryPushOperation(run.ID)
				if err != nil {
					return false, err
				}
			}
		}
		if operation != nil && operation.Phase == db.ExactRecoveryPushInvoked &&
			remoteHead == operation.StaleOID {
			observation, err := database.GetExactRecoveryRefObservation(run.ID)
			if err != nil {
				return false, err
			}
			now := exactRecoveryReconcileNow()
			if observation == nil {
				return false, fmt.Errorf("reconcile stale exact recovery Push custody: observation journal is missing")
			}
			if observation.DeadlineAt > now.Unix() {
				recorded, err := database.RecordExactRecoveryPendingPushObservation(run.ID, remoteHead)
				if err != nil {
					return false, err
				}
				if !recorded {
					continue
				}
				exactRecoveryReconcilePoint("invoked-pending")
				delay := exactRecoveryRefReconcilePollInterval
				remaining := time.Unix(observation.DeadlineAt, 0).Sub(now)
				if remaining < delay {
					delay = remaining
				}
				if delay > 0 {
					if err := exactRecoveryReconcileWait(ctx, delay); err != nil {
						return false, err
					}
				}
				continue
			}
		}
		nextDeadlineAt := exactRecoveryReconcileNow().Add(exactRecoveryRefReconcileWindow).Unix()
		reconciled, err := database.ReconcileStaleExactRecoveryPushCustody(run.ID, remoteHead, ref, run.HeadSHA, nextDeadlineAt, maxReplays, expected)
		if errors.Is(err, db.ErrExactRecoveryPushObservationPending) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !reconciled {
			return false, fmt.Errorf("reconcile stale exact recovery Push custody: custody was not active")
		}
		break
	}
	exactRecoveryReconcilePoint("database-reconciled")
	if _, err := sctx.BindSourceRef(); err != nil {
		return false, err
	}
	return true, nil
}
