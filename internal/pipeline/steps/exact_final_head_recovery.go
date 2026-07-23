package steps

import (
	"context"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/scm"
)

// ValidateExactFinalHeadRecoveryExternalState proves that the previously
// published branch and PR still have the identities frozen on the failed run.
// Recovery may then resume the unpublished exact candidate without replacing
// the PR or claiming that the candidate was already delivered.
func ValidateExactFinalHeadRecoveryExternalState(ctx context.Context, run *db.Run, repo *db.Repo, workDir string, cfg *config.Config) error {
	if run == nil || repo == nil || cfg == nil || run.PRURL == nil || run.LastPushedSHA == nil {
		return fmt.Errorf("exact final-head recovery external state is incomplete")
	}
	if strings.TrimSpace(*run.PRURL) == "" || strings.TrimSpace(*run.LastPushedSHA) == "" || *run.LastPushedSHA == run.HeadSHA {
		return fmt.Errorf("exact final-head recovery has no distinct earlier published head and PR")
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		return err
	}
	sctx := &pipeline.StepContext{Ctx: ctx, Run: run, Repo: repo, WorkDir: workDir, Config: cfg}
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
	publishedHead, err := git.LsRemote(ctx, workDir, pushURL, ref)
	if err != nil {
		return fmt.Errorf("read exact final-head recovery published head: %w", err)
	}
	if publishedHead != *run.LastPushedSHA {
		return fmt.Errorf("published branch head changed from the recorded earlier head")
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
	state, err := host.GetPRState(ctx, existing)
	if err != nil {
		return fmt.Errorf("read exact final-head recovery PR state: %w", err)
	}
	if state != scm.PRStateOpen {
		return fmt.Errorf("stored PR is no longer open: %s", state)
	}
	return nil
}
