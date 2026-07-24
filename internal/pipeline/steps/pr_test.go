package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/scm"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestPRStep_GhNotAvailable(t *testing.T) {
	t.Parallel()
	// Verify the step skips gracefully when the required provider CLI is missing.
	if _, err := exec.LookPath("gh"); err == nil {
		// gh is available on this machine, so we can't force the missing-CLI path here.
		t.Skip("gh is available, skipping unavailable test")
	}

	dir := t.TempDir()
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, "abc", "def", config.Commands{})

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatalf("expected skip when gh is unavailable, got: %v", err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval when PR step skips")
	}
	if !outcome.Skipped {
		t.Fatal("expected skipped outcome when PR step skips")
	}
}

func TestPRStep_AllowsExactBoundaryDeliveryWithStableIdentity(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	const prURL = "https://github.com/test/repo/pull/23"
	env, logFile := fakeGH(t, prURL)
	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Env = env
	storedPRURL := prURL
	sctx.Run.PRURL = &storedPRURL
	if err := sctx.DB.UpdateRunPRURL(sctx.Run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	if _, err := (&PRStep{}).Execute(sctx); err != nil {
		t.Fatal(err)
	}
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != prURL {
		t.Fatalf("PR identity changed: %#v", run.PRURL)
	}
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logData), "pr edit") {
		t.Fatalf("existing PR was not updated: %s", logData)
	}
}

type exactRecoveryPRHost struct {
	pr               *scm.PR
	content          scm.PRContent
	updateCalls      int
	snapshotCalls    int
	repository       string
	expectedRepo     string
	state            scm.PRState
	merged           bool
	headSHA          string
	headRef          string
	baseRef          string
	snapshotHook     func(int)
	updateHook       func()
	updateErr        error
	recoverySnapshot bool
	snapshotErr      error
	snapshotRequests []scm.PRSnapshotRequest
}

func (h *exactRecoveryPRHost) Provider() scm.Provider { return scm.ProviderGitHub }
func (h *exactRecoveryPRHost) Capabilities() scm.Capabilities {
	return scm.Capabilities{RecoverySnapshot: h.recoverySnapshot}
}
func (h *exactRecoveryPRHost) Available(context.Context) error { return nil }
func (h *exactRecoveryPRHost) FindPR(context.Context, string, string) (*scm.PR, error) {
	return h.pr, nil
}
func (h *exactRecoveryPRHost) CreatePR(context.Context, string, string, scm.PRContent) (*scm.PR, error) {
	return nil, fmt.Errorf("unexpected CreatePR")
}
func (h *exactRecoveryPRHost) UpdatePR(_ context.Context, pr *scm.PR, content scm.PRContent) (*scm.PR, error) {
	h.updateCalls++
	if h.updateErr != nil {
		return nil, h.updateErr
	}
	h.content = content
	if h.updateHook != nil {
		h.updateHook()
	}
	return pr, nil
}
func (h *exactRecoveryPRHost) GetPRContent(context.Context, *scm.PR) (scm.PRContent, error) {
	return h.content, nil
}
func (h *exactRecoveryPRHost) GetPRState(context.Context, *scm.PR) (scm.PRState, error) {
	return h.state, nil
}
func (h *exactRecoveryPRHost) ExpectedRepository() string { return h.expectedRepo }
func (h *exactRecoveryPRHost) GetPRSnapshot(_ context.Context, _ *scm.PR, request scm.PRSnapshotRequest) (scm.PRSnapshot, error) {
	h.snapshotCalls++
	h.snapshotRequests = append(h.snapshotRequests, request)
	if h.snapshotErr != nil {
		return scm.PRSnapshot{}, h.snapshotErr
	}
	if h.snapshotHook != nil {
		h.snapshotHook(h.snapshotCalls)
	}
	return scm.PRSnapshot{
		Repository: h.repository,
		Number:     h.pr.Number,
		URL:        h.pr.URL,
		State:      h.state,
		Merged:     h.merged,
		HeadSHA:    h.headSHA,
		HeadRef:    h.headRef,
		BaseRef:    h.baseRef,
		Title:      h.content.Title,
		Body:       h.content.Body,
	}, nil
}
func (h *exactRecoveryPRHost) GetChecks(context.Context, *scm.PR) ([]scm.Check, error) {
	return nil, nil
}
func (h *exactRecoveryPRHost) GetMergeableState(context.Context, *scm.PR) (scm.MergeableState, error) {
	return "", scm.ErrUnsupported
}
func (h *exactRecoveryPRHost) FetchFailedCheckLogs(context.Context, *scm.PR, string, string, []string) (string, error) {
	return "", scm.ErrUnsupported
}

func TestPRStep_ExactRecoveryJournalAvoidsDuplicateMutation(t *testing.T) {
	tests := []struct {
		name        string
		remote      scm.PRContent
		updateCalls int
	}{
		{name: "already applied", remote: scm.PRContent{Title: "intended title", Body: "intended body"}, updateCalls: 0},
		{name: "unchanged prior", remote: scm.PRContent{Title: "prior title", Body: "prior body"}, updateCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sctx, host := exactRecoveryPRStepContext(t, tc.remote)
			step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
				return host, ""
			}}
			if _, err := step.Execute(sctx); err != nil {
				t.Fatal(err)
			}
			if host.updateCalls != tc.updateCalls {
				t.Fatalf("UpdatePR calls = %d, want %d", host.updateCalls, tc.updateCalls)
			}
			if _, err := step.Execute(sctx); err != nil {
				t.Fatal(err)
			}
			if host.updateCalls != tc.updateCalls {
				t.Fatalf("replayed UpdatePR calls = %d, want %d", host.updateCalls, tc.updateCalls)
			}
			update, err := sctx.DB.GetExactRecoveryPRUpdate(sctx.Run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if update == nil || update.State != db.ExactRecoveryPRUpdateApplied || update.AppliedAt == nil {
				t.Fatalf("PR update provenance = %#v", update)
			}
		})
	}
}

func TestExactRecoveryPRAdmissionRejectsUnsupportedOrStaleStateBeforeDelivery(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*exactRecoveryPRHost)
	}{
		{name: "unavailable snapshot", mutate: func(h *exactRecoveryPRHost) { h.snapshotErr = errors.New("provider unavailable") }},
		{name: "unsupported capability", mutate: func(h *exactRecoveryPRHost) { h.recoverySnapshot = false }},
		{name: "incomplete title", mutate: func(h *exactRecoveryPRHost) { h.content.Title = "" }},
		{name: "incomplete body", mutate: func(h *exactRecoveryPRHost) { h.content.Body = "" }},
		{name: "closed", mutate: func(h *exactRecoveryPRHost) { h.state = scm.PRStateClosed }},
		{name: "merged", mutate: func(h *exactRecoveryPRHost) {
			h.state = scm.PRStateMerged
			h.merged = true
		}},
		{name: "wrong head", mutate: func(h *exactRecoveryPRHost) { h.headSHA = strings.Repeat("c", 40) }},
		{name: "wrong branch", mutate: func(h *exactRecoveryPRHost) { h.headRef = "other" }},
		{name: "wrong base", mutate: func(h *exactRecoveryPRHost) { h.baseRef = "release" }},
		{name: "wrong repository", mutate: func(h *exactRecoveryPRHost) { h.repository = "other/repo" }},
		{name: "unknown identity", mutate: func(h *exactRecoveryPRHost) {
			h.pr.Number = ""
			h.pr.URL = ""
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sctx, host := exactRecoveryDeliveryStepContext(
				t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
			)
			gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
			host.headSHA = sctx.Run.BaseSHA
			expectedURL := host.pr.URL
			tc.mutate(host)
			if _, err := validateExactRecoveryPRAdmission(
				context.Background(), sctx, host, host.pr, expectedURL, sctx.Run.BaseSHA,
			); err == nil {
				t.Fatal("unsafe recovery PR admission was accepted")
			}
			if host.updateCalls != 0 {
				t.Fatalf("admission mutated PR %d times", host.updateCalls)
			}
			if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
				t.Fatalf("admission published %s, want unchanged %s", got, sctx.Run.BaseSHA)
			}
		})
	}
}

func TestExactRecoveryPRSnapshotVisibilityBoundPersistsAcrossRestart(t *testing.T) {
	sctx, _ := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
	first, err := exactRecoveryPRSnapshotRequest(sctx, sctx.Run.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	restarted := *sctx
	restarted.Run = persisted
	second, err := exactRecoveryPRSnapshotRequest(&restarted, sctx.Run.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := time.Unix(*persisted.LastPushedAt, 0).Add(exactRecoveryRefReconcileWindow)
	if first.ExpectedHead != sctx.Run.HeadSHA || !first.ReconcileUntil.Equal(wantDeadline) ||
		second.ExpectedHead != first.ExpectedHead || second.AllowedStaleHead != first.AllowedStaleHead ||
		!second.ReconcileUntil.Equal(first.ReconcileUntil) {
		t.Fatalf("snapshot requests before/after restart = %#v / %#v, want deadline %s", first, second, wantDeadline)
	}
}

func TestExactRecoveryAzureRefJournalPersistsAcrossRestart(t *testing.T) {
	sctx, host := exactRecoveryDeliveryStepContext(
		t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
	)
	sctx.Repo.UpstreamURL = "https://dev.azure.com/org/project/_git/repo"
	deadline := time.Now().Add(30 * time.Second).Unix()
	if _, err := sctx.DB.PrepareExactRecoveryRefObservation(
		sctx.Run.ID, string(scm.ProviderAzureDevOps), "refs/heads/feature",
		sctx.Run.HeadSHA, sctx.Run.BaseSHA, deadline,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := exactRecoveryPRSnapshotRequest(sctx, sctx.Run.HeadSHA); err == nil {
		t.Fatal("Azure target was admitted before its Push operation was bound")
	}
	operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.MarkExactRecoveryPushInvoked(
		sctx.Run.ID, operation.OperationID, sctx.Run.BaseSHA,
	); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.BindExactRecoveryPushOperation(sctx.Run.ID, operation.OperationID, db.PushBinding{
		HeadSHA: sctx.Run.HeadSHA, TargetKind: "upstream",
		TargetFingerprint: branchsync.TargetFingerprint(resolvePushURL(sctx)),
		Ref:               "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	persisted, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	restarted := *sctx
	restarted.Run = persisted
	request, err := exactRecoveryPRSnapshotRequest(&restarted, persisted.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if request.AllowedStaleHead != persisted.BaseSHA ||
		!request.ReconcileUntil.Equal(time.Unix(deadline, 0)) ||
		request.RecordObservation == nil {
		t.Fatalf("restart request = %#v", request)
	}
	if err := request.RecordObservation(context.Background(), persisted.BaseSHA); err != nil {
		t.Fatal(err)
	}
	if err := request.RecordObservation(context.Background(), strings.Repeat("c", 40)); err == nil {
		t.Fatal("third OID was accepted after restart")
	}
	if _, err := exactRecoveryPRSnapshotRequest(&restarted, persisted.HeadSHA); err == nil {
		t.Fatal("restart forgot durable Azure ref ambiguity")
	}
	if _, err := validateExactRecoveryPRAdmission(
		context.Background(), &restarted, host, host.pr, host.pr.URL, persisted.HeadSHA,
	); err == nil {
		t.Fatal("ambiguous Azure journal admitted PR mutation")
	}
	if host.updateCalls != 0 || host.snapshotCalls != 0 {
		t.Fatalf("ambiguous Azure journal reached remote PR: updates=%d snapshots=%d", host.updateCalls, host.snapshotCalls)
	}
}

func TestPushStep_ExactRecoveryAzureRefJournalPreventsAmbiguousPublication(t *testing.T) {
	t.Run("old to expected remains idempotent", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(
			t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
		)
		upstream := resolvePushURL(sctx)
		gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		sctx.Repo.UpstreamURL = "https://dev.azure.com/org/project/_git/repo"
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, false); err != nil {
			t.Fatal(err)
		}
		run, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		sctx.Run = run
		step := &PushStep{recoveryRefObserved: func(_ *pipeline.StepContext, _ string) (string, error) {
			return gitCmd(t, upstream, "rev-parse", "refs/heads/feature"), nil
		}}
		if _, err := step.Execute(sctx); err != nil {
			t.Fatal(err)
		}
		if err := sctx.DB.RecordExactRecoveryRefObservation(sctx.Run.ID, sctx.Run.HeadSHA); err != nil {
			t.Fatal(err)
		}
		operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if operation.Phase != db.ExactRecoveryPushBound || operation.BoundAt == nil {
			t.Fatalf("successful Push operation = %#v", operation)
		}
		pushed, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		generation := *pushed.PushGeneration
		if _, err := step.Execute(sctx); err != nil {
			t.Fatal(err)
		}
		replayed, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if *replayed.PushGeneration != generation ||
			gitCmd(t, upstream, "rev-parse", "refs/heads/feature") != sctx.Run.HeadSHA {
			t.Fatalf("replayed Push changed delivery: %#v", replayed)
		}
	})
	t.Run("old to third refuses publication", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(
			t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
		)
		upstream := resolvePushURL(sctx)
		gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		third := supersedingExactRecoveryCommit(t, sctx)
		gitCmd(t, sctx.WorkDir, "push", "origin", third+":refs/heads/third")
		sctx.Repo.UpstreamURL = "https://dev.azure.com/org/project/_git/repo"
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, false); err != nil {
			t.Fatal(err)
		}
		run, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		sctx.Run = run
		step := &PushStep{
			beforeRemoteMutation: func() {
				gitCmd(t, upstream, "update-ref", "refs/heads/feature", third)
			},
			recoveryRefObserved: func(_ *pipeline.StepContext, _ string) (string, error) {
				return gitCmd(t, upstream, "rev-parse", "refs/heads/feature"), nil
			},
		}
		if _, err := step.Execute(sctx); err == nil {
			t.Fatal("ambiguous Azure ref was published")
		}
		if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != third {
			t.Fatalf("ambiguous Push moved remote to %s", got)
		}
		journal, err := sctx.DB.GetExactRecoveryRefObservation(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if journal == nil || journal.State != db.ExactRecoveryRefObservationAmbiguous ||
			journal.LastObservation != third {
			t.Fatalf("ambiguous Push journal = %#v", journal)
		}
		before, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.HeadSHA)
		if _, err := step.Execute(sctx); err == nil {
			t.Fatal("later expected OID erased remembered ambiguity")
		}
		after, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if *after.PushGeneration != *before.PushGeneration ||
			after.LastPushedSHA == nil || *after.LastPushedSHA != sctx.Run.BaseSHA {
			t.Fatalf("ambiguous retry changed push binding: %#v", after)
		}
	})
	t.Run("invoked operation retains attribution through late success", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(
			t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
		)
		upstream := resolvePushURL(sctx)
		gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		sctx.Repo.UpstreamURL = "https://dev.azure.com/org/project/_git/repo"
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, false); err != nil {
			t.Fatal(err)
		}
		run, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		sctx.Run = run
		step := &PushStep{
			recoveryRefObserved: func(_ *pipeline.StepContext, _ string) (string, error) {
				return gitCmd(t, upstream, "rev-parse", "refs/heads/feature"), nil
			},
			invocationMarked: func() error {
				return errors.New("simulated process crash")
			},
		}
		if _, err := step.Execute(sctx); err == nil {
			t.Fatal("simulated crash did not interrupt Push")
		}
		if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
			t.Fatalf("pre-mutation crash published %s", got)
		}
		crashed, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, true); err != nil {
			t.Fatal(err)
		}
		sctx.Run, err = sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		oldWait := exactRecoveryReconcileWait
		t.Cleanup(func() {
			exactRecoveryReconcileWait = oldWait
		})
		waitCalls := 0
		exactRecoveryReconcileWait = func(ctx context.Context, _ time.Duration) error {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			waitCalls++
			if waitCalls == 2 {
				gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.HeadSHA, sctx.Run.BaseSHA)
			}
			return nil
		}
		reconciled, err := ReconcileStaleExactFinalHeadPushCustody(
			context.Background(), sctx.DB, sctx.Run, sctx.Repo, sctx.WorkDir,
			3, types.AllSteps(),
		)
		if err != nil || !reconciled {
			t.Fatalf("reconcile late success = %v, %v", reconciled, err)
		}
		operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if crashed.LastPushedSHA == nil || *crashed.LastPushedSHA != sctx.Run.BaseSHA ||
			operation.Phase != db.ExactRecoveryPushBound || operation.Attempt != 1 ||
			waitCalls != 2 {
			t.Fatalf("late success attribution = %#v, waits=%d crashed=%#v", operation, waitCalls, crashed)
		}
		bound, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if bound.LastPushedSHA == nil || *bound.LastPushedSHA != sctx.Run.HeadSHA ||
			bound.PushGeneration == nil || *bound.PushGeneration != 2 {
			t.Fatalf("late success binding = %#v", bound)
		}
		observations, err := sctx.DB.ListExactRecoveryPushAttemptObservations(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(observations) < 3 {
			t.Fatalf("pending observation history = %#v", observations)
		}
	})
	t.Run("external success after invocation binds without duplicate push", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(
			t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
		)
		upstream := resolvePushURL(sctx)
		gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		sctx.Repo.UpstreamURL = "https://dev.azure.com/org/project/_git/repo"
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, false); err != nil {
			t.Fatal(err)
		}
		run, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		sctx.Run = run
		step := &PushStep{
			recoveryRefObserved: func(_ *pipeline.StepContext, _ string) (string, error) {
				return gitCmd(t, upstream, "rev-parse", "refs/heads/feature"), nil
			},
			invocationMarked: func() error {
				gitCmd(t, upstream, "update-ref", "refs/heads/feature", sctx.Run.HeadSHA, sctx.Run.BaseSHA)
				return errors.New("simulated crash after remote success")
			},
		}
		if _, err := step.Execute(sctx); err == nil {
			t.Fatal("simulated post-mutation crash did not interrupt Push")
		}
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, true); err != nil {
			t.Fatal(err)
		}
		reconciled, err := sctx.DB.ReconcileStaleExactRecoveryPushCustody(
			sctx.Run.ID, sctx.Run.HeadSHA, "refs/heads/feature", sctx.Run.HeadSHA,
			time.Now().Add(time.Minute).Unix(), 3, types.AllSteps(),
		)
		if err != nil || !reconciled {
			t.Fatalf("reconcile post-mutation crash = %v, %v", reconciled, err)
		}
		bound, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		generation := *bound.PushGeneration
		sctx.Run = bound
		step.invocationMarked = nil
		if _, err := step.Execute(sctx); err != nil {
			t.Fatal(err)
		}
		replayed, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if *replayed.PushGeneration != generation ||
			gitCmd(t, upstream, "rev-parse", "refs/heads/feature") != sctx.Run.HeadSHA {
			t.Fatalf("replayed recovered Push changed delivery: %#v", replayed)
		}
	})
}

func TestPRStep_ExactRecoveryJournalRefusesAmbiguousRemoteContent(t *testing.T) {
	sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "partial", Body: "superseded"})
	step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
		return host, ""
	}}
	if _, err := step.Execute(sctx); err == nil {
		t.Fatal("ambiguous remote PR content was accepted")
	}
	if host.updateCalls != 0 {
		t.Fatalf("ambiguous recovery mutated PR %d times", host.updateCalls)
	}
}

func TestPRStep_ExactRecoveryRefusesProviderIndependentRemoteAmbiguity(t *testing.T) {
	sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
	if err := sctx.DB.RecordExactRecoveryRemoteRefAmbiguity(sctx.Run.ID, db.ExactRecoveryRemoteRefIdentityMismatch); err != nil {
		t.Fatal(err)
	}
	step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
		return host, ""
	}}
	if _, err := step.Execute(sctx); err == nil {
		t.Fatal("provider-independent remote ambiguity admitted PR mutation")
	}
	if host.updateCalls != 0 {
		t.Fatalf("provider-independent remote ambiguity mutated PR %d times", host.updateCalls)
	}
}

func TestPRStep_ExactRecoveryRefusesLifecycleIdentityAndHeadDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*exactRecoveryPRHost)
	}{
		{name: "closed", mutate: func(h *exactRecoveryPRHost) { h.state = scm.PRStateClosed }},
		{name: "merged", mutate: func(h *exactRecoveryPRHost) {
			h.state = scm.PRStateMerged
			h.merged = true
		}},
		{name: "head drift", mutate: func(h *exactRecoveryPRHost) { h.headSHA = strings.Repeat("a", 40) }},
		{name: "source branch drift", mutate: func(h *exactRecoveryPRHost) { h.headRef = "other" }},
		{name: "base drift", mutate: func(h *exactRecoveryPRHost) { h.baseRef = "release" }},
		{name: "repository drift", mutate: func(h *exactRecoveryPRHost) { h.repository = "other/repo" }},
		{name: "identity drift", mutate: func(h *exactRecoveryPRHost) { h.pr.URL = "https://github.com/test/repo/pull/99" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
			tc.mutate(host)
			step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
				return host, ""
			}}
			if _, err := step.Execute(sctx); err == nil {
				t.Fatal("stale PR lifecycle or identity was accepted")
			}
			if host.updateCalls != 0 {
				t.Fatalf("stale PR was mutated %d times", host.updateCalls)
			}
		})
	}
}

func TestPRStep_ExactRecoveryVerifiesAuthoritativeStateAfterMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*exactRecoveryPRHost)
	}{
		{name: "closed", mutate: func(h *exactRecoveryPRHost) { h.state = scm.PRStateClosed }},
		{name: "merged", mutate: func(h *exactRecoveryPRHost) {
			h.state = scm.PRStateMerged
			h.merged = true
		}},
		{name: "head drift", mutate: func(h *exactRecoveryPRHost) { h.headSHA = strings.Repeat("b", 40) }},
		{name: "base drift", mutate: func(h *exactRecoveryPRHost) { h.baseRef = "release" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
			host.updateHook = func() { tc.mutate(host) }
			step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
				return host, ""
			}}
			if _, err := step.Execute(sctx); err == nil {
				t.Fatal("ambiguous PR after-state was accepted")
			}
			if host.updateCalls != 1 || host.snapshotCalls != 2 {
				t.Fatalf("update calls = %d, snapshot calls = %d", host.updateCalls, host.snapshotCalls)
			}
			if len(host.snapshotRequests) != 2 ||
				host.snapshotRequests[0].ExpectedHead != sctx.Run.HeadSHA ||
				host.snapshotRequests[1].ExpectedHead != host.snapshotRequests[0].ExpectedHead ||
				host.snapshotRequests[1].AllowedStaleHead != host.snapshotRequests[0].AllowedStaleHead ||
				!host.snapshotRequests[1].ReconcileUntil.Equal(host.snapshotRequests[0].ReconcileUntil) ||
				host.snapshotRequests[0].ReconcileUntil.IsZero() {
				t.Fatalf("before/after snapshot requests = %#v", host.snapshotRequests)
			}
			update, err := sctx.DB.GetExactRecoveryPRUpdate(sctx.Run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if update == nil || update.State != db.ExactRecoveryPRUpdatePrepared || update.AppliedAt != nil {
				t.Fatalf("ambiguous PR mutation was marked applied: %#v", update)
			}
		})
	}
}

func TestPRStep_ExactRecoveryOwnershipExcludesReceiveContention(t *testing.T) {
	sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
	superseding := supersedingExactRecoveryCommit(t, sctx)
	var competingErr error
	step := &PRStep{
		hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
			return host, ""
		},
		ownershipClaimed: func() {
			cmd := exec.Command("git", "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
			cmd.Dir = sctx.WorkDir
			_, competingErr = cmd.CombinedOutput()
		},
	}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}
	if competingErr == nil {
		t.Fatal("receive-side source update crossed PR ownership claim")
	}
	if got := gitCmd(t, sctx.WorkDir, "rev-parse", "refs/heads/feature"); got != sctx.Run.HeadSHA {
		t.Fatalf("source ref moved during PR update to %s", got)
	}
	gitCmd(t, sctx.WorkDir, "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
}

func TestPRStep_ExactRecoveryOwnershipReleasesAfterMutationFailure(t *testing.T) {
	sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
	superseding := supersedingExactRecoveryCommit(t, sctx)
	host.updateErr = errors.New("remote update failed")
	step := &PRStep{hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
		return host, ""
	}}
	if _, err := step.Execute(sctx); err == nil {
		t.Fatal("failed PR mutation returned nil")
	}
	gitCmd(t, sctx.WorkDir, "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
}

func exactRecoveryPRStepContext(t *testing.T, remote scm.PRContent) (*pipeline.StepContext, *exactRecoveryPRHost) {
	return exactRecoveryDeliveryStepContext(t, remote, false)
}

func exactRecoveryDeliveryStepContext(t *testing.T, remote scm.PRContent, stopAtPush bool) (*pipeline.StepContext, *exactRecoveryPRHost) {
	t.Helper()
	dir, baseSHA, headSHA := setupGitRepo(t)
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")
	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Repo.UpstreamURL = upstream
	const prURL = "https://github.com/test/repo/pull/23"
	sctx.Run.HeadSHA = headSHA
	exhaustHeadValidationCapacity(t, sctx, headSHA)
	if err := sctx.DB.RecordSuccessfulTestHead(sctx.Run.ID, headSHA); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateRunPRURL(sctx.Run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateRunPushBinding(sctx.Run.ID, db.PushBinding{
		HeadSHA: baseSHA, TargetKind: "upstream", TargetFingerprint: branchsync.TargetFingerprint(upstream), Ref: "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	results := make(map[types.StepName]*db.StepResult, len(types.AllSteps()))
	for _, name := range types.AllSteps() {
		result, err := sctx.DB.InsertStepResult(sctx.Run.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		results[name] = result
		switch {
		case name.Order() <= types.StepTest.Order():
			if err := sctx.DB.CompleteStep(result.ID, 0, 1, string(name)+".log"); err != nil {
				t.Fatal(err)
			}
		case name == types.StepDocument:
			if err := sctx.DB.StartStep(result.ID); err != nil {
				t.Fatal(err)
			}
			if err := sctx.DB.FailStep(result.ID, db.ExactFinalHeadCapacityStepError(3), 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := sctx.DB.UpdateRunErrorStatus(sctx.Run.ID, db.ExactFinalHeadCapacityRunError(3), types.RunFailed); err != nil {
		t.Fatal(err)
	}
	failure, err := sctx.DB.InspectExactFinalHeadCapacityFailure(sctx.Run.ID, 3, types.AllSteps())
	if err != nil {
		t.Fatal(err)
	}
	anchorRef, err := sourceprovenance.ExactRecoveryAnchorRef(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceprovenance.EnsureExactRecoveryAnchor(context.Background(), dir, anchorRef, headSHA); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.RestoreExactFinalHeadCapacityFailure(sctx.Run.ID, failure.EvidenceToken, 3, types.AllSteps()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []types.StepName{types.StepDocument, types.StepLint} {
		if err := sctx.DB.CompleteStep(results[name].ID, 0, 1, string(name)+".log"); err != nil {
			t.Fatal(err)
		}
	}
	if err := sctx.DB.CompleteHeadValidation(sctx.Run.ID, headSHA); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.StartStep(results[types.StepPush].ID); err != nil {
		t.Fatal(err)
	}
	host := &exactRecoveryPRHost{
		pr:               &scm.PR{Number: "23", URL: prURL},
		content:          remote,
		repository:       "test/repo",
		expectedRepo:     "test/repo",
		recoverySnapshot: true,
		state:            scm.PRStateOpen,
		headSHA:          headSHA,
		headRef:          "feature",
		baseRef:          "main",
	}
	if stopAtPush {
		if err := sctx.DB.SetRunPushActive(sctx.Run.ID, true); err != nil {
			t.Fatal(err)
		}
		run, err := sctx.DB.GetRun(sctx.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		sctx.Run = run
		sctx.StepResultID = results[types.StepPush].ID
		return sctx, host
	}
	if err := sctx.DB.UpdateRunPushBinding(sctx.Run.ID, db.PushBinding{
		HeadSHA: headSHA, TargetKind: "upstream", TargetFingerprint: branchsync.TargetFingerprint(upstream), Ref: "refs/heads/feature",
	}); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.CompleteStep(results[types.StepPush].ID, 0, 1, "push.log"); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.StartStep(results[types.StepPR].ID); err != nil {
		t.Fatal(err)
	}
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	sctx.Run = run
	sctx.StepResultID = results[types.StepPR].ID
	update, err := sctx.DB.PrepareExactRecoveryPRUpdate(
		run.ID, sctx.StepResultID, prURL, headSHA,
		"prior title", "prior body", "intended title", "intended body",
	)
	if err != nil {
		t.Fatal(err)
	}
	if update == nil {
		t.Fatal("PR update journal missing")
	}
	return sctx, host
}

func supersedingExactRecoveryCommit(t *testing.T, sctx *pipeline.StepContext) string {
	t.Helper()
	gitCmd(t, sctx.WorkDir, "checkout", "--detach", sctx.Run.HeadSHA)
	if err := os.WriteFile(filepath.Join(sctx.WorkDir, "superseding-source.txt"), []byte("new source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, sctx.WorkDir, "add", "superseding-source.txt")
	gitCmd(t, sctx.WorkDir, "commit", "-m", "superseding source")
	superseding := gitCmd(t, sctx.WorkDir, "rev-parse", "HEAD")
	gitCmd(t, sctx.WorkDir, "checkout", "--detach", sctx.Run.HeadSHA)
	return superseding
}

func TestPRStep_ExactRecoveryRefMoveBeforeUpdatePreventsMutation(t *testing.T) {
	sctx, host := exactRecoveryPRStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"})
	superseding := supersedingExactRecoveryCommit(t, sctx)
	step := &PRStep{
		hostFactory: func(*pipeline.StepContext, scm.Provider) (scm.Host, string) {
			return host, ""
		},
		beforeRemoteMutation: func() {
			gitCmd(t, sctx.WorkDir, "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
		},
	}
	if _, err := step.Execute(sctx); !errors.Is(err, pipeline.ErrSourceRefSuperseded) {
		t.Fatalf("Execute() error = %v, want superseded source refusal", err)
	}
	if host.updateCalls != 0 {
		t.Fatalf("superseded recovery mutated PR %d times", host.updateCalls)
	}
}

func TestExactRecoveryReconciliationRefMovesPreventPublication(t *testing.T) {
	oldPoint := exactRecoveryReconcilePoint
	t.Cleanup(func() {
		exactRecoveryReconcilePoint = oldPoint
	})
	for _, targetPoint := range []string{"remote-probed", "source-verified", "database-reconciled"} {
		t.Run(targetPoint, func(t *testing.T) {
			sctx, _ := exactRecoveryDeliveryStepContext(t, scm.PRContent{}, true)
			gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
			superseding := supersedingExactRecoveryCommit(t, sctx)
			exactRecoveryReconcilePoint = func(point string) {
				if point == targetPoint {
					gitCmd(t, sctx.WorkDir, "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
				}
			}
			if _, err := ReconcileStaleExactFinalHeadPushCustody(
				context.Background(), sctx.DB, sctx.Run, sctx.Repo, sctx.WorkDir, 3, types.AllSteps(),
			); !errors.Is(err, pipeline.ErrSourceRefSuperseded) {
				t.Fatalf("reconcile error = %v, want superseded source refusal", err)
			}
			if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
				t.Fatalf("reconciliation published %s, want unchanged %s", got, sctx.Run.BaseSHA)
			}
			exactRecoveryReconcilePoint = oldPoint
		})
	}
}

func TestExactRecoveryReconciliationPersistsProviderIndependentRemoteAmbiguity(t *testing.T) {
	oldLsRemote := exactRecoveryLsRemote
	t.Cleanup(func() {
		exactRecoveryLsRemote = oldLsRemote
	})
	sctx, _ := exactRecoveryDeliveryStepContext(t, scm.PRContent{}, true)
	gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
	if operation, err := sctx.DB.GetExactRecoveryPushOperation(sctx.Run.ID); err != nil || operation != nil {
		t.Fatalf("provider-neutral recovery unexpectedly has operation %#v, %v", operation, err)
	}
	exactRecoveryLsRemote = func(context.Context, string, string, string) (git.RemoteRefObservation, error) {
		return git.RemoteRefObservation{Invalid: git.RemoteRefDuplicate}, nil
	}
	if _, err := ReconcileStaleExactFinalHeadPushCustody(
		context.Background(), sctx.DB, sctx.Run, sctx.Repo, sctx.WorkDir, 3, types.AllSteps(),
	); err == nil {
		t.Fatal("duplicate remote-ref result was accepted")
	}
	ambiguity, err := sctx.DB.GetExactRecoveryRemoteRefAmbiguity(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ambiguity == nil || ambiguity.Classification != git.RemoteRefDuplicate {
		t.Fatalf("duplicate remote-ref result was not durably ambiguous: %#v", ambiguity)
	}
	restarted, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	sctx.Run = restarted
	exactRecoveryLsRemote = func(context.Context, string, string, string) (git.RemoteRefObservation, error) {
		return git.RemoteRefObservation{OID: sctx.Run.HeadSHA}, nil
	}
	if _, err := ReconcileStaleExactFinalHeadPushCustody(
		context.Background(), sctx.DB, sctx.Run, sctx.Repo, sctx.WorkDir, 3, types.AllSteps(),
	); err == nil {
		t.Fatal("later exact OID erased durable remote-ref ambiguity")
	}
	after, err := sctx.DB.GetExactRecoveryRemoteRefAmbiguity(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after == nil || *after != *ambiguity {
		t.Fatalf("later exact OID changed terminal ambiguity: before=%#v after=%#v", ambiguity, after)
	}
	if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
		t.Fatalf("ambiguous recovery published %s, want unchanged %s", got, sctx.Run.BaseSHA)
	}
}

func TestExactRecoveryReconcileWaitPreservesCancellationCause(t *testing.T) {
	cause := errors.New("cancel recovery probe")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if err := exactRecoveryReconcileWait(ctx, time.Hour); !errors.Is(err, cause) {
		t.Fatalf("wait error = %v, want %v", err, cause)
	}
}

func TestPRStep_UpdatesExistingPR(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "https://github.com/test/repo/pull/42")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Error("pr step should never need approval")
	}

	// Verify gh pr edit was called to update the PR body
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "pr edit") {
		t.Errorf("expected gh pr edit to be called, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "--body") {
		t.Errorf("expected --body flag in gh pr edit, got:\n%s", ghLog)
	}

	// Verify PR URL was stored
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != "https://github.com/test/repo/pull/42" {
		t.Errorf("PR URL = %v, want https://github.com/test/repo/pull/42", run.PRURL)
	}
}

func TestPRStep_RefusesToReplaceStoredPRIdentity(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	env, logFile := fakeGH(t, "https://github.com/test/repo/pull/42")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	stored := "https://github.com/test/repo/pull/1477"
	sctx.Run.PRURL = &stored
	if err := sctx.DB.UpdateRunPRURL(sctx.Run.ID, stored); err != nil {
		t.Fatal(err)
	}

	_, err := (&PRStep{}).Execute(sctx)
	if err == nil || !strings.Contains(err.Error(), "does not match stored identity") {
		t.Fatalf("Execute() error = %v, want stored-identity refusal", err)
	}
	logData, readErr := os.ReadFile(logFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(logData), "pr create") || strings.Contains(string(logData), "pr edit") {
		t.Fatalf("mismatched identity mutated a PR:\n%s", logData)
	}
}

func TestPRStep_BitbucketUpdatesExistingPR(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	api := newFakeBitbucketPRAPI(t, 42, "https://bitbucket.org/test/repo/pull-requests/42")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = fakeBitbucketEnv(api.server.URL)
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if api.listCalls != 1 {
		t.Fatalf("list calls = %d, want 1", api.listCalls)
	}
	if api.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", api.updateCalls)
	}
	if api.createCalls != 0 {
		t.Fatalf("create calls = %d, want 0", api.createCalls)
	}
	if api.lastAuthHeader == "" {
		t.Fatal("expected Authorization header for Bitbucket API")
	}
	if !strings.Contains(api.lastUpdateBody, "title") || !strings.Contains(api.lastUpdateBody, "description") {
		t.Fatalf("expected Bitbucket PR update payload to include title and description, got %q", api.lastUpdateBody)
	}

	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != "https://bitbucket.org/test/repo/pull-requests/42" {
		t.Fatalf("PR URL = %v, want Bitbucket PR URL", run.PRURL)
	}
}

func TestPRStep_BitbucketUpdatesExistingPRWithoutHTMLLink(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	api := newFakeBitbucketPRAPI(t, 42, "https://bitbucket.org/test/repo/pull-requests/42")
	api.existingPRURL = "https://bitbucket.org/test/repo/pull-requests/42"
	api.createdPRURL = ""

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = fakeBitbucketEnv(api.server.URL)
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.lastAuthHeader = r.Header.Get("Authorization")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/2.0/repositories/test/repo/pullrequests":
			api.listCalls++
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"values":[{"id":%d,"links":{"html":{"href":%q}}}]}`,
				api.existingPRID,
				api.existingPRURL,
			)
		case r.Method == http.MethodPut && r.URL.Path == fmt.Sprintf("/2.0/repositories/test/repo/pullrequests/%d", api.existingPRID):
			api.updateCalls++
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read update body: %v", err)
			}
			api.lastUpdateBody = string(body)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":%d}`,
				api.existingPRID,
			)
		default:
			t.Fatalf("unexpected Bitbucket PR API request: %s %s", r.Method, r.URL.String())
		}
	})

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if api.listCalls != 1 {
		t.Fatalf("list calls = %d, want 1", api.listCalls)
	}
	if api.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", api.updateCalls)
	}
	if api.createCalls != 0 {
		t.Fatalf("create calls = %d, want 0", api.createCalls)
	}
	if outcome.PRURL != api.existingPRURL {
		t.Fatalf("outcome PR URL = %q, want %q", outcome.PRURL, api.existingPRURL)
	}

	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != api.existingPRURL {
		t.Fatalf("PR URL = %v, want %q", run.PRURL, api.existingPRURL)
	}
}

func TestPRStep_ZeroBaseSHA(t *testing.T) {
	t.Parallel()
	// New branch scenario: baseSHA is all-zeros, commit log should still work
	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	os.WriteFile(filepath.Join(dir, "base.txt"), []byte("base"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "base commit")

	gitCmd(t, dir, "checkout", "-b", "feature")
	os.WriteFile(filepath.Join(dir, "feature.txt"), []byte("feature"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "add feature")
	headSHA := gitCmd(t, dir, "rev-parse", "HEAD")

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{name: "test"}
	zeroSHA := "0000000000000000000000000000000000000000"
	sctx := newTestContextWithDBRecords(t, ag, dir, zeroSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Error("pr step should never need approval")
	}

	// Verify gh pr create was called (not blocked by zero SHA)
	logData, _ := os.ReadFile(logFile)
	if !strings.Contains(string(logData), "pr create") {
		t.Errorf("expected gh pr create, got:\n%s", logData)
	}
}

func TestPRStep_CreatesNewPR(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	// No existing PR - pr view returns exit 1
	env, logFile := fakeGH(t, "")

	findings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"touches critical error handling"}`
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, findings); err != nil {
		t.Fatal(err)
	}

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Error("pr step should never need approval")
	}

	// Verify gh pr create was called
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "pr create") {
		t.Errorf("expected gh pr create to be called, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "--title add feature --") {
		t.Fatalf("expected fallback PR title to reject raw non-conventional commit summary, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "--title feat: add feature --body") {
		t.Fatalf("expected fallback PR title to use release-triggering conventional commit format, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "add feature\n\n## Risk Assessment\n\n⚠️ Medium: touches critical error handling") {
		t.Fatalf("expected fallback PR body to append risk note under Risk Assessment heading, got:\n%s", ghLog)
	}

	// Verify PR URL was stored
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != "https://github.com/test/repo/pull/99" {
		t.Errorf("PR URL = %v, want https://github.com/test/repo/pull/99", run.PRURL)
	}
}

func TestPRStep_GitHubForkCreatesParentPRWithForkHead(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: route fork prs","body":"## Summary\n\n- open fork PR against parent"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.Repo.UpstreamURL = "https://github.com/parent-owner/no-mistakes.git"
	sctx.Repo.ForkURL = "https://github.com/fork-owner/no-mistakes.git"
	sctx.Repo.BaseBranch = "release/v2"
	sctx.Run.BaseBranch = "staging"
	sctx.Run.Branch = "refs/heads/feature"

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "pr list --head feature --base staging --repo parent-owner/no-mistakes --state open --json number,url,headRefName,headRepositoryOwner") {
		t.Fatalf("expected PR lookup to use parent repo and bare head branch, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "pr list --head fork-owner:feature") {
		t.Fatalf("PR lookup used unsupported owner-qualified --head, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "pr create --head fork-owner:feature --base staging --repo parent-owner/no-mistakes") {
		t.Fatalf("expected PR create to target parent repo with fork owner head, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "--repo fork-owner/no-mistakes") {
		t.Fatalf("expected no self-PR against fork repo, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "pr create --head feature --") {
		t.Fatalf("expected PR create to avoid bare fork head, got:\n%s", ghLog)
	}
}

func TestPRStep_BitbucketCreatesNewPR(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	api := newFakeBitbucketPRAPI(t, 0, "")

	findings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"touches critical error handling"}`
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = fakeBitbucketEnv(api.server.URL)
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"
	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, findings); err != nil {
		t.Fatal(err)
	}

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if api.listCalls != 1 {
		t.Fatalf("list calls = %d, want 1", api.listCalls)
	}
	if api.createCalls != 1 {
		t.Fatalf("create calls = %d, want 1", api.createCalls)
	}
	if api.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", api.updateCalls)
	}
	if !strings.Contains(api.lastCreateBody, `"source"`) || !strings.Contains(api.lastCreateBody, `"destination"`) {
		t.Fatalf("expected Bitbucket PR create payload to include source and destination, got %q", api.lastCreateBody)
	}

	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != api.createdPRURL {
		t.Fatalf("PR URL = %v, want %q", run.PRURL, api.createdPRURL)
	}
}

func TestPRStep_BitbucketCreatesNewPRWithoutHTMLLink(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	api := newFakeBitbucketPRAPI(t, 0, "")
	api.createdPRURL = ""

	findings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"touches critical error handling"}`
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = fakeBitbucketEnv(api.server.URL)
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"
	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, findings); err != nil {
		t.Fatal(err)
	}
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.lastAuthHeader = r.Header.Get("Authorization")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/2.0/repositories/test/repo/pullrequests":
			api.listCalls++
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"values":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/2.0/repositories/test/repo/pullrequests":
			api.createCalls++
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read create body: %v", err)
			}
			api.lastCreateBody = string(body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"id":99}`)
		default:
			t.Fatalf("unexpected Bitbucket PR API request: %s %s", r.Method, r.URL.String())
		}
	})

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if outcome.PRURL != "https://bitbucket.org/test/repo/pull-requests/99" {
		t.Fatalf("PR URL = %q, want derived Bitbucket PR URL", outcome.PRURL)
	}

	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != "https://bitbucket.org/test/repo/pull-requests/99" {
		t.Fatalf("PR URL = %v, want derived Bitbucket PR URL", run.PRURL)
	}
}

func TestPRStep_BitbucketMissingEnvSkipsBeforeBuildingContent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	ag := &mockAgent{name: "test"}
	sctx := newTestContext(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if len(ag.calls) != 0 {
		t.Fatalf("expected Bitbucket PR step to skip before building content, got %d agent calls", len(ag.calls))
	}
}

func TestPRStep_BitbucketUsesProcessEnvWhenStepEnvIsNil(t *testing.T) {
	dir, baseSHA, headSHA := setupGitRepo(t)
	api := newFakeBitbucketPRAPI(t, 0, "")
	t.Setenv("NO_MISTAKES_BITBUCKET_EMAIL", "test@example.com")
	t.Setenv("NO_MISTAKES_BITBUCKET_API_TOKEN", "test-token")
	t.Setenv("NO_MISTAKES_BITBUCKET_API_BASE_URL", api.server.URL)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: process env bitbucket pr","body":"## Summary\n\n- create PR via process env"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = "https://bitbucket.org/test/repo.git"

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("bitbucket PR step should never need approval")
	}
	if outcome.PRURL != api.createdPRURL {
		t.Fatalf("PR URL = %q, want %q", outcome.PRURL, api.createdPRURL)
	}
	if api.createCalls != 1 {
		t.Fatalf("expected Bitbucket PR create API to be called once, got %d", api.createCalls)
	}
}

func TestPRStep_UsesAgentGeneratedTitleAndBody(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	findings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"touches critical error handling"}`

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: improve pipeline header UX","body":"## Summary\n\n- keep branch status readable\n- fix footer truncation"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, findings); err != nil {
		t.Fatal(err)
	}

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}
	if len(ag.calls) != 1 {
		t.Fatalf("expected 1 agent call, got %d", len(ag.calls))
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "--title fix: improve pipeline header UX") {
		t.Fatalf("expected generated PR title in gh call, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "keep branch status readable") {
		t.Fatalf("expected generated PR body in gh call, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "fix footer truncation\n\n## Risk Assessment\n\n⚠️ Medium: touches critical error handling") {
		t.Fatalf("expected risk note under Risk Assessment heading, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "--title feature") {
		t.Fatalf("expected PR title to avoid raw branch name, got:\n%s", ghLog)
	}
}

func TestPRStep_AppendsTestingSectionFromTestStep(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	reviewFindings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"touches critical error handling"}`
	testRound1 := `{"findings":[{"id":"test-1","severity":"error","file":"pkg/handler_test.go","line":42,"description":"expected 429 got 200"}],"summary":"1 failure"}`

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: improve pipeline header UX","body":"## Summary\n\n- keep branch status readable\n- fix footer truncation"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "configured-test-command"})
	sctx.Env = env

	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, reviewFindings); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.InsertStepRound(reviewStep.ID, 1, "initial", &reviewFindings, nil, 500); err != nil {
		t.Fatal(err)
	}

	testStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(testStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.InsertStepRound(testStep.ID, 1, "initial", &testRound1, nil, 800); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.InsertStepRound(testStep.ID, 2, "auto_fix", nil, nil, 600); err != nil {
		t.Fatal(err)
	}
	recordSuccessfulTestProof(t, sctx, headSHA)

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	wantOrder := "## Risk Assessment\n\n⚠️ Medium: touches critical error handling\n\n## Testing\n\n- 🔧 **Test** - 1 issue found → auto-fixed ✅\n\n## Pipeline"
	if !strings.Contains(ghLog, wantOrder) {
		t.Fatalf("expected testing section between risk assessment and pipeline, got:\n%s", ghLog)
	}
}

func TestPRStep_ConfiguredTestWithoutExactHeadProofNeverCallsGitHub(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "configured-test-command"})
	sctx.Env = env

	testStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(testStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}

	step := &PRStep{}
	if _, err := step.Execute(sctx); err == nil || !strings.Contains(err.Error(), "configured Test proof is missing") {
		t.Fatalf("expected missing exact-head proof rejection, got %v", err)
	}
	if len(ag.calls) != 0 {
		t.Fatalf("PR content agent called %d times without exact-head proof", len(ag.calls))
	}
	if _, err := os.Stat(logFile); !os.IsNotExist(err) {
		t.Fatalf("GitHub CLI log exists without exact-head proof: %v", err)
	}
}

func TestPRStep_UnwrapsNestedJSONBody(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	// Agent returns body as the serialized prContent JSON (the bug LLMs sometimes produce).
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: improve pipeline header UX","body":"{\"title\":\"fix: improve pipeline header UX\",\"body\":\"## Summary\\n\\n- keep branch status readable\\n- fix footer truncation\"}"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	// The guard should unwrap the nested body and use the real markdown.
	if !strings.Contains(ghLog, "keep branch status readable") {
		t.Fatalf("expected unwrapped PR body in gh call, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, `"title"`) {
		t.Fatalf("expected JSON wrapper to be stripped from PR body, got:\n%s", ghLog)
	}
}

func TestUnwrapNestedPRBody(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "empty string", body: "", want: ""},
		{name: "plain markdown", body: "## Summary\n\n- bullet one", want: "## Summary\n\n- bullet one"},
		{name: "invalid JSON starting with brace", body: "{not valid json", want: "{not valid json"},
		{name: "valid JSON but empty nested body", body: `{"title":"fix: stuff","body":""}`, want: `{"title":"fix: stuff","body":""}`},
		{name: "nested JSON body is unwrapped", body: `{"title":"fix: stuff","body":"## Summary\n\n- real body"}`, want: "## Summary\n\n- real body"},
		{name: "nested JSON body with whitespace", body: `{"title":"fix: stuff","body":"  ## Summary  "}`, want: "## Summary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unwrapNestedPRBody(tt.body)
			if got != tt.want {
				t.Errorf("unwrapNestedPRBody(%q) = %q, want %q", tt.body, got, tt.want)
			}
		})
	}
}

func TestAppendGeneratedSections_StripsAgentGeneratedSections(t *testing.T) {
	body := "## Summary\n\n- improve PR descriptions\n\n## Testing\n\n- model-added testing\n\n## Risk Assessment\n\nold risk\n\n## Pipeline\n\nold pipeline"

	got := appendGeneratedSections(
		body,
		"real risk",
		"## Testing\n\n- deterministic testing",
		"## Pipeline\n\n- deterministic pipeline",
	)

	if strings.Count(got, "## Testing") != 1 {
		t.Fatalf("expected one Testing section, got:\n%s", got)
	}
	if strings.Count(got, "## Risk Assessment") != 1 {
		t.Fatalf("expected one Risk Assessment section, got:\n%s", got)
	}
	if strings.Count(got, "## Pipeline") != 1 {
		t.Fatalf("expected one Pipeline section, got:\n%s", got)
	}
	if strings.Contains(got, "model-added testing") || strings.Contains(got, "old risk") || strings.Contains(got, "old pipeline") {
		t.Fatalf("expected generated sections to replace agent-provided ones, got:\n%s", got)
	}
}

func TestAssemblePRBody_NoLimitKeepsEverything(t *testing.T) {
	t.Parallel()
	sctx := &pipeline.StepContext{UserIntent: "wanted a Bar() helper"}
	testing := "## Testing\n\n```text\n" + strings.Repeat("log ", 2000) + "\n```"

	got := assemblePRBody(sctx, "## What Changed\n\n- add Bar()", "low risk", testing, "## Pipeline\n\n- ok", 0)

	for _, want := range []string{"## Intent", "## What Changed", "## Risk Assessment", "## Testing", "## Pipeline"} {
		if !strings.Contains(got, want) {
			t.Fatalf("unlimited body missing %q section:\n%s", want, got)
		}
	}
}

func TestAssemblePRBody_DropsTestingEmbedsToFitAzureCap(t *testing.T) {
	t.Parallel()
	sctx := &pipeline.StepContext{UserIntent: "wanted a Bar() helper for foo callers"}
	limit := scm.MaxPRBodyChars(scm.ProviderAzureDevOps) // 4000

	// A Testing section whose embedded logs alone blow well past the cap.
	testing := "## Testing\n\n```text\n" + strings.Repeat("verbose test log line\n", 600) + "```"

	got := assemblePRBody(sctx,
		"## What Changed\n\n- add Bar() helper",
		"behavior preserved; low risk",
		testing,
		"## Pipeline\n\n- review: pass\n- tests: pass",
		limit,
	)

	if scm.PRBodyLen(got) > limit {
		t.Fatalf("assembled body = %d units, want <= %d", scm.PRBodyLen(got), limit)
	}
	// The Intent / What Changed / Risk / Pipeline narrative survives...
	for _, want := range []string{"## Intent", "wanted a Bar() helper", "## What Changed", "## Risk Assessment", "## Pipeline"} {
		if !strings.Contains(got, want) {
			t.Fatalf("budgeted body dropped required content %q:\n%s", want, got)
		}
	}
	// ...and the unbounded Testing log dump is what got shed, not hard-truncated mid-section.
	if strings.Contains(got, "verbose test log line") {
		t.Fatalf("expected embedded test logs to be shed under the cap, got:\n%s", got)
	}
	if strings.HasSuffix(got, prTruncationTail()) {
		t.Fatalf("did not expect a hard clamp when dropping Testing was enough:\n%s", got)
	}
}

func TestAssemblePRBody_ClampsWhenCoreAloneExceedsCap(t *testing.T) {
	t.Parallel()
	// An Intent so long that even Intent + What Changed overruns the cap, with no
	// Testing section to drop. The clamp backstop must keep it within budget.
	sctx := &pipeline.StepContext{UserIntent: strings.Repeat("x", 6000)}
	limit := scm.MaxPRBodyChars(scm.ProviderAzureDevOps)

	got := assemblePRBody(sctx, "## What Changed\n\n- add Bar()", "low risk", "", "## Pipeline\n\n- ok", limit)

	if scm.PRBodyLen(got) > limit {
		t.Fatalf("clamped body = %d units, want <= %d", scm.PRBodyLen(got), limit)
	}
}

func prTruncationTail() string {
	// Mirror of scm's truncation marker for assertions; kept here so the test
	// reads naturally without exporting the constant.
	return "(description truncated)"
}

func TestPRBodyBudgetPromptSection(t *testing.T) {
	t.Parallel()
	if got := prBodyBudgetPromptSection(0); got != "" {
		t.Fatalf("prBodyBudgetPromptSection(0) = %q, want empty", got)
	}
	got := prBodyBudgetPromptSection(4000)
	if !strings.Contains(got, "4000 characters") || !strings.Contains(got, "What Changed") {
		t.Fatalf("prBodyBudgetPromptSection(4000) missing budget guidance: %q", got)
	}
}

func TestAppendGeneratedSections_StripsCommonHeadingVariants(t *testing.T) {
	body := "## Summary\n\n- improve PR descriptions\n\n## tests:\n\n- model-added testing\n\n## risk assessment\n\nold risk\n\n## Pipeline:\n\nold pipeline"

	got := appendGeneratedSections(
		body,
		"real risk",
		"## Testing\n\n- deterministic testing",
		"## Pipeline\n\n- deterministic pipeline",
	)

	if strings.Contains(got, "model-added testing") || strings.Contains(got, "old risk") || strings.Contains(got, "old pipeline") {
		t.Fatalf("expected generated heading variants to be replaced, got:\n%s", got)
	}
	if strings.Count(got, "## Testing") != 1 {
		t.Fatalf("expected one normalized Testing section, got:\n%s", got)
	}
	if strings.Count(got, "## Risk Assessment") != 1 {
		t.Fatalf("expected one normalized Risk Assessment section, got:\n%s", got)
	}
	if strings.Count(got, "## Pipeline") != 1 {
		t.Fatalf("expected one normalized Pipeline section, got:\n%s", got)
	}
}

func TestAppendGeneratedSections_LeavesUnderLimitBodyByteIdentical(t *testing.T) {
	body := "## What Changed\n\n- improve PR descriptions"
	riskLine := "✅ Low: deterministic PR body assembly only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"
	pipelineMD := pipelineMarkdownForTest("review round 001 stayed small", "review round 002 stayed small")

	got := appendGeneratedSections(body, riskLine, testingMD, pipelineMD)
	want := body + "\n\n## Risk Assessment\n\n" + riskLine + "\n\n" + testingMD + "\n\n" + pipelineMD

	if got != want {
		t.Fatalf("expected under-limit body to be byte-identical\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestAppendGeneratedSections_TruncatesPipelineUpdatesBeforeGitHubLimit(t *testing.T) {
	body := "## What Changed\n\n- essential summary survives\n\n" + strings.Repeat("essential details stay intact\n", 350)
	riskLine := "✅ Low: generated PR body length guard only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"
	rounds := make([]string, 0, 160)
	for i := 1; i <= 160; i++ {
		rounds = append(rounds, fmt.Sprintf("review round %03d - %s", i, strings.Repeat("x", 700)))
	}
	pipelineMD := pipelineMarkdownForTest(rounds...)

	got := appendGeneratedSections(body, riskLine, testingMD, pipelineMD)

	assertGitHubBodyLimitForTest(t, got)
	if !strings.Contains(got, "essential summary survives") || !strings.Contains(got, riskLine) || !strings.Contains(got, testingMD) {
		t.Fatalf("expected essential sections to survive intact, got:\n%s", got)
	}
	if !strings.Contains(got, "earlier update rounds omitted to keep the PR body within GitHub's 65536-char limit") {
		t.Fatalf("expected pipeline omission marker, got:\n%s", got)
	}
	if strings.Contains(got, "review round 001") {
		t.Fatalf("expected oldest pipeline update to be omitted, got:\n%s", got)
	}
	if !strings.Contains(got, "review round 160") {
		t.Fatalf("expected newest pipeline update to be retained, got:\n%s", got)
	}
	assertNoPartialRoundLinesForTest(t, got, rounds)
	if strings.Count(got, "<details>") != strings.Count(got, "</details>") {
		t.Fatalf("expected details tags to remain balanced, got:\n%s", got)
	}
}

func TestAppendGeneratedSections_ExtremePipelineOverflowStillFitsLimit(t *testing.T) {
	body := "## What Changed\n\n- essential summary survives"
	rounds := make([]string, 0, 1000)
	for i := 1; i <= 1000; i++ {
		rounds = append(rounds, fmt.Sprintf("review round %04d - %s", i, strings.Repeat("x", 2000)))
	}

	got := appendGeneratedSections(body, "", "", pipelineMarkdownForTest(rounds...))

	assertGitHubBodyLimitForTest(t, got)
	if !strings.Contains(got, "essential summary survives") {
		t.Fatalf("expected essential summary to survive, got:\n%s", got)
	}
	if !strings.Contains(got, "earlier update rounds omitted") {
		t.Fatalf("expected omission marker in extreme overflow case, got:\n%s", got)
	}
	assertNoPartialRoundLinesForTest(t, got, rounds)
}

func TestAppendGeneratedSections_TruncatesOversizedLatestPipelineUpdate(t *testing.T) {
	body := "## What Changed\n\n- essential summary survives"
	latest := "review round 003 - newest oversized update\n" + strings.Repeat("latest detail line stays whole\n", 3000)

	got := appendGeneratedSections(
		body,
		"✅ Low: generated PR body length guard only",
		"## Testing\n\n- go test ./internal/pipeline/steps",
		pipelineMarkdownForTest(
			"review round 001 - older update",
			"review round 002 - older update",
			latest,
		),
	)

	assertGitHubBodyLimitForTest(t, got)
	if !strings.Contains(got, "2 earlier update rounds omitted") {
		t.Fatalf("expected only earlier pipeline updates to be omitted, got:\n%s", got)
	}
	if strings.Contains(got, "3 earlier update rounds omitted") {
		t.Fatalf("expected latest pipeline update to be retained, got:\n%s", got)
	}
	if strings.Contains(got, "review round 001") || strings.Contains(got, "review round 002") {
		t.Fatalf("expected older pipeline updates to be omitted, got:\n%s", got)
	}
	if !strings.Contains(got, "review round 003 - newest oversized update") {
		t.Fatalf("expected newest pipeline update heading to survive, got:\n%s", got)
	}
	if !strings.Contains(got, "latest pipeline update truncated") {
		t.Fatalf("expected latest pipeline update truncation marker, got:\n%s", got)
	}
	if strings.Count(got, "<details>") != strings.Count(got, "</details>") {
		t.Fatalf("expected details tags to remain balanced, got:\n%s", got)
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, "latest detail") && line != "latest detail line stays whole" {
			t.Fatalf("latest update was truncated mid-line: %q", line)
		}
	}

	single := appendGeneratedSections(body, "", "", pipelineMarkdownForTest(latest))
	assertGitHubBodyLimitForTest(t, single)
	if strings.Contains(single, "earlier update") {
		t.Fatalf("expected single latest update not to be labeled as omitted earlier history, got:\n%s", single)
	}
	if !strings.Contains(single, "review round 003 - newest oversized update") {
		t.Fatalf("expected single latest pipeline update heading to survive, got:\n%s", single)
	}
	if !strings.Contains(single, "latest pipeline update truncated") {
		t.Fatalf("expected single latest pipeline update truncation marker, got:\n%s", single)
	}
}

func TestAppendGeneratedSections_TruncatesSingleLineLatestPipelineUpdate(t *testing.T) {
	body := "## What Changed\n\n- essential summary survives"
	latest := "review round 001 - newest single-line oversized update " + strings.Repeat("x", maxPullRequestBodyBytes)

	got := appendGeneratedSections(body, "", "", pipelineMarkdownForTest(latest))

	assertGitHubBodyLimitForTest(t, got)
	if strings.Contains(got, "earlier update") {
		t.Fatalf("expected single latest update not to be labeled as omitted earlier history, got:\n%s", got)
	}
	if !strings.Contains(got, "review round 001 - newest single-line oversized update") {
		t.Fatalf("expected bounded latest pipeline update excerpt to survive, got:\n%s", got)
	}
	if !strings.Contains(got, "latest pipeline update truncated") {
		t.Fatalf("expected latest pipeline update truncation marker, got:\n%s", got)
	}
}

func TestAppendGeneratedSections_TrimsBodyToKeepPipelineOmissionMarker(t *testing.T) {
	baseBody := "## What Changed\n\n- essential summary survives\n\n"
	riskLine := "✅ Low: generated PR body length guard only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"
	generatedSections := generatedEssentialSections(riskLine, testingMD)
	targetPrefixLen := maxPullRequestBodyBytes - len("\n\n") - 10
	fillerLen := targetPrefixLen - len(baseBody) - len(generatedSections)
	if fillerLen <= 0 {
		t.Fatalf("test setup produced invalid filler length %d", fillerLen)
	}
	body := baseBody + strings.Repeat("x", fillerLen)
	rounds := make([]string, 0, 200)
	for i := 1; i <= 200; i++ {
		rounds = append(rounds, fmt.Sprintf("review round %03d - %s", i, strings.Repeat("x", 700)))
	}

	got := appendGeneratedSections(body, riskLine, testingMD, pipelineMarkdownForTest(rounds...))

	assertGitHubBodyLimitForTest(t, got)
	for _, want := range []string{
		"essential summary survives",
		"body truncated to keep the PR body within GitHub's 65536-char limit",
		"## Risk Assessment",
		riskLine,
		"## Testing",
		"go test ./internal/pipeline/steps",
		"## Pipeline",
		"earlier update rounds omitted",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected truncated PR body to contain %q, got:\n%s", want, got)
		}
	}
}

func TestAppendGeneratedSections_TrimsBodyToKeepLatestPipelineUpdate(t *testing.T) {
	baseBody := "## What Changed\n\n- essential summary survives\n\n"
	riskLine := "✅ Low: generated PR body length guard only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"
	generatedSections := generatedEssentialSections(riskLine, testingMD)
	minPipeline := minimumPipelineOmissionSection(pipelineMarkdownForTest(
		"review round 001 - older update",
		"review round 002 - newest update "+strings.Repeat("x", 2000),
	))
	targetPrefixLen := maxPullRequestBodyBytes - len("\n\n") - len(minPipeline)
	fillerLen := targetPrefixLen + 500 - len(baseBody) - len(generatedSections)
	if fillerLen <= 0 {
		t.Fatalf("test setup produced invalid filler length %d", fillerLen)
	}
	body := baseBody + strings.Repeat("filler line keeps body truncatable\n", fillerLen/len("filler line keeps body truncatable\n")+1)

	got := appendGeneratedSections(
		body,
		riskLine,
		testingMD,
		pipelineMarkdownForTest(
			"review round 001 - older update",
			"review round 002 - newest update "+strings.Repeat("x", 2000),
		),
	)

	assertGitHubBodyLimitForTest(t, got)
	if strings.Contains(got, "2 earlier update rounds omitted") {
		t.Fatalf("expected latest pipeline update not to be counted as omitted, got:\n%s", got)
	}
	if !strings.Contains(got, "1 earlier update round omitted") {
		t.Fatalf("expected only earlier pipeline update to be omitted, got:\n%s", got)
	}
	if !strings.Contains(got, "review round 002 - newest update") {
		t.Fatalf("expected newest pipeline update excerpt to survive, got:\n%s", got)
	}
	if !strings.Contains(got, "latest pipeline update truncated") {
		t.Fatalf("expected latest pipeline update truncation marker, got:\n%s", got)
	}
	for _, want := range []string{
		"essential summary survives",
		"## Risk Assessment",
		riskLine,
		"## Testing",
		"go test ./internal/pipeline/steps",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected truncated PR body to contain %q, got:\n%s", want, got)
		}
	}
}

func TestBuildPRBody_TrimsOversizedLaterSectionWithoutDroppingSmallEssentials(t *testing.T) {
	sctx := newTestContext(t, &mockAgent{name: "test"}, t.TempDir(), "", "", config.Commands{})
	sctx.UserIntent = "Keep the release notes readable."
	body := strings.Join([]string{
		"## What Changed",
		"",
		"- essential summary survives",
		"",
		"## Validation Notes",
		"",
		strings.Repeat("validation output stays truncatable\n", 3000),
	}, "\n")
	riskLine := "✅ Low: generated PR body length guard only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"

	got := buildPRBody(body, riskLine, testingMD, "", sctx)

	assertGitHubBodyLimitForTest(t, got)
	for _, want := range []string{
		"## Intent",
		"Keep the release notes readable.",
		"## What Changed",
		"essential summary survives",
		"## Validation Notes",
		"body truncated to keep the PR body within GitHub's 65536-char limit",
		"## Risk Assessment",
		riskLine,
		"## Testing",
		"go test ./internal/pipeline/steps",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected oversized PR body to contain %q, got:\n%s", want, got)
		}
	}
}

func TestAppendGeneratedSections_TruncatesUTF8OnValidBoundary(t *testing.T) {
	marker := essentialPRBodyTruncationMarker()
	got := truncateTextAtLineBoundary(strings.Repeat("界", 10), len("\n\n")+len(marker)+1, marker)
	if !utf8.ValidString(got) {
		t.Fatalf("expected direct body truncation to remain valid UTF-8")
	}

	marker = pipelineLatestUpdateTruncationMarker()
	got = truncatePipelineUpdateAtLineBoundary(strings.Repeat("界", 10), len("\n\n")+len(marker)+1, marker)
	if !utf8.ValidString(got) {
		t.Fatalf("expected direct pipeline update truncation to remain valid UTF-8")
	}

	body := "## What Changed\n\n- essential summary survives\n\n" + strings.Repeat("界", maxPullRequestBodyBytes)

	got = appendGeneratedSections(body, "", "", "")

	assertGitHubBodyLimitForTest(t, got)
	if !utf8.ValidString(got) {
		t.Fatalf("expected truncated PR body to remain valid UTF-8")
	}

	latest := "review round 001 - newest update " + strings.Repeat("界", maxPullRequestBodyBytes)
	got = appendGeneratedSections("## What Changed\n\n- essential summary survives", "", "", pipelineMarkdownForTest(latest))

	assertGitHubBodyLimitForTest(t, got)
	if !utf8.ValidString(got) {
		t.Fatalf("expected truncated pipeline update to remain valid UTF-8")
	}
	if !strings.Contains(got, "review round 001 - newest update") {
		t.Fatalf("expected newest pipeline update excerpt to survive, got:\n%s", got)
	}
}

func TestBuildPRBody_TruncatesOversizedIntentBeforeGeneratedSections(t *testing.T) {
	sctx := newTestContext(t, &mockAgent{name: "test"}, t.TempDir(), "", "", config.Commands{})
	sctx.UserIntent = "Keep generated sections visible.\n" + strings.Repeat("oversized intent context line\n", 2500)
	body := "## What Changed\n\n- essential summary survives"
	riskLine := "✅ Low: generated PR body length guard only"
	testingMD := "## Testing\n\n- go test ./internal/pipeline/steps"

	got := buildPRBody(body, riskLine, testingMD, pipelineMarkdownForTest("review round 001"), sctx)

	assertGitHubBodyLimitForTest(t, got)
	for _, want := range []string{
		"## Intent",
		"Keep generated sections visible.",
		"body truncated to keep the PR body within GitHub's 65536-char limit",
		"## What Changed",
		"essential summary survives",
		"## Risk Assessment",
		riskLine,
		"## Testing",
		"go test ./internal/pipeline/steps",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected oversized PR body to contain %q, got:\n%s", want, got)
		}
	}
}

func TestPRStep_CreateKeepsGeneratedSectionsAfterOversizedIntent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: keep generated pr bodies postable","body":"## What Changed\n\n- essential summary survives"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = "Keep generated sections visible.\n" + strings.Repeat("oversized intent context line\n", 2500)

	reviewFindings := `{"findings":[],"summary":"clean","risk_level":"medium","risk_rationale":"validates generated PR body length handling"}`
	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.SetStepFindings(reviewStep.ID, reviewFindings); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.InsertStepRound(reviewStep.ID, 1, "initial", &reviewFindings, nil, 100); err != nil {
		t.Fatal(err)
	}

	testFindings := `{"findings":[],"summary":"","testing_summary":"Validated generated PR body length handling.","tested":["go test ./internal/pipeline/steps"]}`
	testStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(testStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := sctx.DB.InsertStepRound(testStep.ID, 1, "initial", &testFindings, nil, 100); err != nil {
		t.Fatal(err)
	}

	if _, err := (&PRStep{}).Execute(sctx); err != nil {
		t.Fatal(err)
	}

	body := readFakeGHBodyArg(t, logFile)
	assertGitHubBodyLimitForTest(t, body)
	for _, want := range []string{
		"## Intent",
		"Keep generated sections visible.",
		"body truncated to keep the PR body within GitHub's 65536-char limit",
		"## What Changed",
		"essential summary survives",
		"## Risk Assessment",
		"validates generated PR body length handling",
		"## Testing",
		"Validated generated PR body length handling.",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected created PR body to contain %q, got:\n%s", want, body)
		}
	}
}

func TestPRStep_BuildPRContentTruncatesGeneratedPipelineUpdates(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: keep generated pr bodies postable","body":"## What Changed\n\n- essential summary survives"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.UserIntent = "Keep PR creation postable when long validation runs accumulate many pipeline update rounds."

	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 140; i++ {
		findings := fmt.Sprintf(`{"findings":[{"id":"review-%03d","severity":"warning","file":"internal/pipeline/steps/pr.go","line":%d,"description":"review round %03d %s"}],"summary":"1 warning"}`, i, i, i, strings.Repeat("x", 600))
		trigger := "auto_fix"
		if i == 1 {
			trigger = "initial"
		}
		if _, err := sctx.DB.InsertStepRound(reviewStep.ID, i, trigger, &findings, nil, 100); err != nil {
			t.Fatal(err)
		}
	}

	content, err := (&PRStep{}).buildPRContent(sctx, "feature", baseSHA, 0)
	if err != nil {
		t.Fatal(err)
	}

	assertGitHubBodyLimitForTest(t, content.Body)
	if !strings.Contains(content.Body, "Keep PR creation postable") || !strings.Contains(content.Body, "essential summary survives") {
		t.Fatalf("expected intent and summary to survive, got:\n%s", content.Body)
	}
	if !strings.Contains(content.Body, "earlier update rounds omitted") {
		t.Fatalf("expected omission marker, got:\n%s", content.Body)
	}
	if strings.Contains(content.Body, "review round 001") {
		t.Fatalf("expected old pipeline update to be omitted, got:\n%s", content.Body)
	}
	if !strings.Contains(content.Body, "review round 140") {
		t.Fatalf("expected latest pipeline update to be retained, got:\n%s", content.Body)
	}
}

func TestPRStep_CreateCapsBodyAfterPrependedIntent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload, err := json.Marshal(prContent{
				Title: "fix: keep generated pr bodies postable",
				Body:  "## What Changed\n\n- essential summary survives",
			})
			if err != nil {
				t.Fatal(err)
			}
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = "Keep PR creation postable.\n" + strings.Repeat("intent context line stays visible\n", 900)

	reviewStep, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := sctx.DB.UpdateStepStatus(reviewStep.ID, types.StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 140; i++ {
		findings := fmt.Sprintf(`{"findings":[{"id":"review-%03d","severity":"warning","file":"internal/pipeline/steps/pr.go","line":%d,"description":"review round %03d %s"}],"summary":"1 warning"}`, i, i, i, strings.Repeat("x", 600))
		trigger := "auto_fix"
		if i == 1 {
			trigger = "initial"
		}
		if _, err := sctx.DB.InsertStepRound(reviewStep.ID, i, trigger, &findings, nil, 100); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := (&PRStep{}).Execute(sctx); err != nil {
		t.Fatal(err)
	}

	body := readFakeGHBodyArg(t, logFile)
	assertGitHubBodyLimitForTest(t, body)
	for _, want := range []string{
		"## Intent",
		"Keep PR creation postable.",
		"intent context line stays visible",
		"essential summary survives",
		"earlier update rounds omitted",
		"review round 140",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected final PR body to contain %q", want)
		}
	}
	if strings.Contains(body, "review round 001") {
		t.Fatal("expected oldest pipeline update to be omitted")
	}
}

func TestFallbackPRContentCapsBodyAfterPrependedIntent(t *testing.T) {
	t.Parallel()
	sctx := newTestContext(t, &mockAgent{name: "test"}, t.TempDir(), "", "", config.Commands{})
	sctx.UserIntent = "Fallback intent survives.\n" + strings.Repeat("fallback intent context line\n", 900)

	rounds := make([]string, 0, 140)
	for i := 1; i <= 140; i++ {
		rounds = append(rounds, fmt.Sprintf("review round %03d - %s", i, strings.Repeat("x", 700)))
	}

	content := fallbackPRContent(
		sctx,
		"feature",
		"abc123 add feature",
		"✅ Low: generated PR body length guard only",
		"## Testing\n\n- go test ./internal/pipeline/steps",
		pipelineMarkdownForTest(rounds...),
		0,
	)

	assertGitHubBodyLimitForTest(t, content.Body)
	for _, want := range []string{
		"## Intent",
		"Fallback intent survives.",
		"## What Changed",
		"add feature",
		"## Risk Assessment",
		"## Testing",
		"earlier update rounds omitted",
		"review round 140",
	} {
		if !strings.Contains(content.Body, want) {
			t.Fatalf("expected fallback PR body to contain %q", want)
		}
	}
	if strings.Contains(content.Body, "review round 001") {
		t.Fatal("expected oldest pipeline update to be omitted")
	}
}

func pipelineMarkdownForTest(rounds ...string) string {
	var b strings.Builder
	b.WriteString("## Pipeline\n\nUpdates from [git push no-mistakes](https://github.com/kunchenguid/no-mistakes)\n\n")
	b.WriteString("<details>\n")
	b.WriteString("<summary>🔧 **Review** - update rounds</summary>\n\n")
	for _, round := range rounds {
		b.WriteString(round)
		b.WriteString("\n\n")
	}
	b.WriteString("</details>\n")
	return b.String()
}

func readFakeGHBodyArg(t *testing.T, logFile string) string {
	t.Helper()
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	const marker = " --body "
	log := string(logData)
	idx := strings.LastIndex(log, marker)
	if idx < 0 {
		t.Fatalf("expected fake gh log to include --body, got:\n%s", log)
	}
	return strings.TrimSuffix(log[idx+len(marker):], "\n")
}

func assertGitHubBodyLimitForTest(t *testing.T, body string) {
	t.Helper()
	if got := len(body); got >= githubPullRequestBodyHardLimitChars {
		t.Fatalf("body length = %d, want below GitHub hard limit %d", got, githubPullRequestBodyHardLimitChars)
	}
	if got := len(body); got > maxPullRequestBodyBytes {
		t.Fatalf("body length = %d, want safety buffer below %d", got, maxPullRequestBodyBytes)
	}
}

func assertNoPartialRoundLinesForTest(t *testing.T, body string, rounds []string) {
	t.Helper()
	full := make(map[string]struct{}, len(rounds))
	for _, round := range rounds {
		full[round] = struct{}{}
	}
	for _, line := range strings.Split(body, "\n") {
		if !strings.Contains(line, "review round ") {
			continue
		}
		if _, ok := full[line]; !ok {
			t.Fatalf("pipeline update was truncated mid-line: %q", line)
		}
	}
}

func TestPRStep_PrependsIntentSectionWhenIntentSet(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"feat: add bar","body":"## What Changed\n\n- add Bar()"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = "user wanted to add a Bar() helper for foo callers"

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	intentIdx := strings.Index(ghLog, "## Intent")
	whatChangedIdx := strings.Index(ghLog, "## What Changed")
	if intentIdx < 0 {
		t.Fatalf("expected ## Intent section in PR body, got:\n%s", ghLog)
	}
	if whatChangedIdx < 0 {
		t.Fatalf("expected ## What Changed section in PR body, got:\n%s", ghLog)
	}
	if intentIdx > whatChangedIdx {
		t.Fatalf("expected ## Intent before ## What Changed, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "user wanted to add a Bar() helper for foo callers") {
		t.Fatalf("expected intent text in PR body, got:\n%s", ghLog)
	}
}

func TestPRStep_OmitsIntentSectionWhenIntentEmpty(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"feat: add bar","body":"## What Changed\n\n- add Bar()"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = ""

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	if strings.Contains(ghLog, "## Intent") {
		t.Fatalf("expected no ## Intent section when intent is empty, got:\n%s", ghLog)
	}
}

func TestPRStep_StripsAgentEmittedIntentBeforePrepend(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"feat: add bar","body":"## Intent\n\n- agent paraphrase\n\n## What Changed\n\n- add Bar()"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = "real user intent string"

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	if strings.Count(ghLog, "## Intent") != 1 {
		t.Fatalf("expected exactly one ## Intent section, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "agent paraphrase") {
		t.Fatalf("expected agent-emitted Intent body to be stripped, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "real user intent string") {
		t.Fatalf("expected deterministic intent text, got:\n%s", ghLog)
	}
}

func TestPRStep_PromptUsesWhatChanged(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, _ := fakeGH(t, "")

	var capturedPrompt string
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			capturedPrompt = opts.Prompt
			payload := json.RawMessage(`{"title":"feat: add bar","body":"## What Changed\n\n- add Bar()"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(capturedPrompt, "## What Changed") {
		t.Errorf("expected prompt to instruct agent to write ## What Changed, got:\n%s", capturedPrompt)
	}
}

func TestPRStep_FallbackUsesWhatChangedAndIntent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			return nil, fmt.Errorf("simulated agent failure")
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.UserIntent = "fallback intent text"

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)

	if !strings.Contains(ghLog, "## What Changed") {
		t.Fatalf("expected fallback PR body to use ## What Changed heading, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "## Summary") {
		t.Fatalf("expected fallback PR body to no longer use ## Summary heading, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "## Intent") {
		t.Fatalf("expected fallback PR body to include ## Intent section, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "fallback intent text") {
		t.Fatalf("expected fallback PR body to include intent text, got:\n%s", ghLog)
	}

	intentIdx := strings.Index(ghLog, "## Intent")
	whatChangedIdx := strings.Index(ghLog, "## What Changed")
	if intentIdx > whatChangedIdx {
		t.Fatalf("expected ## Intent before ## What Changed in fallback, got:\n%s", ghLog)
	}
}

func TestPRStep_GitLabCreatesNewMR(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGlab(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"feat: improve gitlab flow","body":"## Summary\n\n- add gitlab support\n\n## Testing\n\n- go test ./..."}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env
	sctx.Repo.UpstreamURL = "https://gitlab.com/test/repo.git"

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "mr create") {
		t.Fatalf("expected glab mr create to be called, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "--title feat: improve gitlab flow") {
		t.Fatalf("expected generated title in glab call, got:\n%s", ghLog)
	}
}

func TestPRStep_SkipsWhenProviderCLIUnavailable(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = "https://gitlab.com/test/repo.git"
	sctx.Env = []string{"PATH=" + t.TempDir()}

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatalf("expected skip instead of failure, got: %v", err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval when PR step skips")
	}
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL != nil {
		t.Fatalf("expected no PR URL when provider CLI unavailable, got %q", *run.PRURL)
	}
}

func TestPRStep_SkipsBeforeBuildingContentWhenProviderCLIUnavailable(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			t.Fatal("expected PR content generation to be skipped when CLI is unavailable")
			return nil, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = "https://gitlab.com/test/repo.git"
	sctx.Env = []string{"PATH=" + t.TempDir()}

	step := &PRStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatalf("expected skip instead of failure, got: %v", err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval when PR step skips")
	}
	if len(ag.calls) != 0 {
		t.Fatalf("expected no agent calls when provider CLI unavailable, got %d", len(ag.calls))
	}
}

func TestPRStep_ExistingBranchUsesMergeBaseCommitLog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	os.WriteFile(filepath.Join(dir, "base.txt"), []byte("base\n"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "base commit")

	gitCmd(t, dir, "checkout", "-b", "feature")
	os.WriteFile(filepath.Join(dir, "first.txt"), []byte("first\n"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "first feature commit")
	oldRemoteSHA := gitCmd(t, dir, "rev-parse", "HEAD")

	os.WriteFile(filepath.Join(dir, "second.txt"), []byte("second\n"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "second feature commit")
	headSHA := gitCmd(t, dir, "rev-parse", "HEAD")

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, oldRemoteSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "first feature commit") {
		t.Errorf("expected PR body to include first feature commit, got:\n%s", ghLog)
	}
	if !strings.Contains(ghLog, "second feature commit") {
		t.Errorf("expected PR body to include second feature commit, got:\n%s", ghLog)
	}
}

func TestPRStep_AgentNonConventionalTitleFallsBack(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"Improve pipeline header UX","body":"## Summary\n\n- improvements"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	// The title should be prefixed with a release-triggering type, not the raw agent output.
	if strings.Contains(ghLog, "--title Improve pipeline header UX --") {
		t.Fatal("non-conventional agent title should have been rejected")
	}
	if !strings.Contains(ghLog, "fix: Improve pipeline header UX") {
		t.Fatal("expected user-facing agent title to be prefixed with fix:, got: " + ghLog)
	}
	// The agent's body should be preserved, not replaced with fallback
	if !strings.Contains(ghLog, "## Summary") {
		t.Fatal("expected agent body to be preserved, got: " + ghLog)
	}
}

func TestPRStep_AgentScopedBreakingTitlePassesThrough(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	const title = "feat(api)!: require auth token"
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"feat(api)!: require auth token","body":"## Summary\n\n- require auth token on all API requests"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "--title "+title+" --body") {
		t.Fatalf("expected scoped conventional breaking-change title to pass through unchanged, got:\n%s", ghLog)
	}
	if strings.Contains(ghLog, "--title chore: "+title+" --body") {
		t.Fatalf("expected scoped conventional breaking-change title to avoid fallback prefix, got:\n%s", ghLog)
	}
}

func TestPRStep_AgentConventionalNonReleaseTitlePassesThrough(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, logFile := fakeGH(t, "")

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"refactor(cli): improve CLI output","body":"## Summary\n\n- improve user-visible command output"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	ghLog := string(logData)
	if !strings.Contains(ghLog, "--title refactor(cli): improve CLI output --body") {
		t.Fatalf("expected conventional agent PR title to pass through unchanged, got:\n%s", ghLog)
	}
}

func TestPRStep_PromptRequiresReleaseTypesForProductImpact(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			payload := json.RawMessage(`{"title":"fix: improve CLI output","body":"## What Changed\n\n- improve output"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})

	step := &PRStep{}
	if _, err := step.buildPRContent(sctx, "feature", baseSHA, 0); err != nil {
		t.Fatal(err)
	}
	if len(ag.calls) != 1 {
		t.Fatalf("agent calls = %d, want 1", len(ag.calls))
	}
	prompt := ag.calls[0].Prompt
	if !strings.Contains(prompt, "user-facing product impact") {
		t.Fatalf("prompt should mention user-facing product impact rule, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "must use feat or fix") {
		t.Fatalf("prompt should require feat or fix for product impact, got:\n%s", prompt)
	}
}

// TestPRStep_PromptGuidesScopeToRealModule verifies the PR prompt instructs
// the agent to pick a scope that is a real, primary, not-too-granular
// module/package name in the codebase.
func TestPRStep_PromptGuidesScopeToRealModule(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	env, _ := fakeGH(t, "")

	var capturedPrompt string
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			capturedPrompt = opts.Prompt
			payload := json.RawMessage(`{"title":"fix(daemon): tidy logs","body":"## Summary\n\n- tidy"}`)
			return &agent.Result{Output: payload}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Env = env

	step := &PRStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(capturedPrompt, "real package/module name that exists in the codebase") {
		t.Errorf("expected PR prompt to require scope be a real package/module name in the codebase, got:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "primary module affected") {
		t.Errorf("expected PR prompt to require scope be the primary module affected, got:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "not too granular") {
		t.Errorf("expected PR prompt to warn scope should not be too granular, got:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "fewer than 10 distinct") {
		t.Errorf("expected PR prompt to convey typical module count heuristic, got:\n%s", capturedPrompt)
	}
}
