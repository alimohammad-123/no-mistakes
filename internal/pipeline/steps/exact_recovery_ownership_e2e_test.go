//go:build e2e

package steps

import (
	"errors"
	"os/exec"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/scm"
)

func TestExactRecoveryDeliveryOwnershipInterleavingsE2E(t *testing.T) {
	t.Run("unsupported snapshot admission", func(t *testing.T) {
		sctx, host := exactRecoveryDeliveryStepContext(
			t, scm.PRContent{Title: "prior title", Body: "prior body"}, true,
		)
		gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		host.headSHA = sctx.Run.BaseSHA
		host.recoverySnapshot = false
		if _, err := validateExactRecoveryPRAdmission(
			sctx.Ctx, sctx, host, host.pr, host.pr.URL, sctx.Run.BaseSHA,
		); err == nil {
			t.Fatal("unsupported provider was admitted")
		}
		if host.updateCalls != 0 {
			t.Fatalf("unsupported admission mutated PR %d times", host.updateCalls)
		}
		if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
			t.Fatalf("unsupported admission published %s", got)
		}
	})

	t.Run("superseded before claim", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"}, true)
		gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		superseding := supersedingExactRecoveryCommit(t, sctx)
		step := &PushStep{beforeRemoteMutation: func() {
			gitCmd(t, sctx.WorkDir, "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
		}}
		if _, err := step.Execute(sctx); !errors.Is(err, pipeline.ErrSourceRefSuperseded) {
			t.Fatalf("Push error = %v, want superseded refusal", err)
		}
		if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.BaseSHA {
			t.Fatalf("superseded Push published %s", got)
		}
		event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
		if err != nil {
			t.Fatal(err)
		}
		if event == nil || gitCmd(t, sctx.WorkDir, "rev-parse", event.AnchorRef) != sctx.Run.HeadSHA {
			t.Fatalf("superseded candidate anchor = %#v", event)
		}
	})

	t.Run("push", func(t *testing.T) {
		sctx, _ := exactRecoveryDeliveryStepContext(t, scm.PRContent{Title: "prior title", Body: "prior body"}, true)
		gitCmd(t, sctx.Repo.UpstreamURL, "update-ref", "refs/heads/feature", sctx.Run.BaseSHA)
		superseding := supersedingExactRecoveryCommit(t, sctx)
		var competingErr error
		step := &PushStep{ownershipClaimed: func() {
			cmd := exec.Command("git", "update-ref", "refs/heads/feature", superseding, sctx.Run.HeadSHA)
			cmd.Dir = sctx.WorkDir
			_, competingErr = cmd.CombinedOutput()
		}}
		if _, err := step.Execute(sctx); err != nil {
			t.Fatal(err)
		}
		if competingErr == nil {
			t.Fatal("receive-side source update crossed Push ownership claim")
		}
		if got := gitCmd(t, sctx.Repo.UpstreamURL, "rev-parse", "refs/heads/feature"); got != sctx.Run.HeadSHA {
			t.Fatalf("Push published %s, want %s", got, sctx.Run.HeadSHA)
		}
	})

	t.Run("pr", func(t *testing.T) {
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
		if host.updateCalls != 1 || host.snapshotCalls != 2 {
			t.Fatalf("PR update calls = %d, snapshot calls = %d", host.updateCalls, host.snapshotCalls)
		}
	})
}
