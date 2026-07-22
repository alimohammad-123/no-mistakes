package daemon

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
)

const repoPolicyPath = ".no-mistakes.yaml"

var getBootstrapOriginURL = git.GetRemoteURL

// resolveBootstrapTestAuthorization authorizes a new run only when a freshly
// read pipeline base proves the trusted policy absent. The submitted policy is
// used only as exact-byte and exact-command evidence; the command returned for
// execution always comes from the user-owned global binding.
func resolveBootstrapTestAuthorization(
	ctx context.Context,
	global *config.GlobalConfig,
	repo *db.Repo,
	run *db.Run,
	workDir string,
	trustedPolicy *trustedRepoPolicy,
) (*db.BootstrapTestAuthorization, error) {
	if trustedPolicy == nil {
		return nil, fmt.Errorf("bootstrap Test authorization requires an authoritative pipeline-base policy result")
	}
	if trustedPolicy.Present {
		if trustedPolicy.Config == nil {
			return nil, fmt.Errorf("bootstrap Test authorization received an incomplete trusted-policy result")
		}
		return nil, nil
	}
	if trustedPolicy.Config != nil {
		return nil, fmt.Errorf("bootstrap Test authorization received an ambiguous trusted-policy result")
	}
	if global == nil || len(global.Bootstrap.Test) == 0 {
		return nil, nil
	}
	if repo == nil || run == nil {
		return nil, fmt.Errorf("bootstrap Test authorization requires a repository and run")
	}
	if err := config.ValidateBootstrapTestBindings(global.Bootstrap.Test); err != nil {
		return nil, err
	}
	identity, err := bootstrapRepositoryIdentity(ctx, repo, workDir)
	if err != nil {
		return nil, err
	}
	baseBranch := run.EffectiveBaseBranch(repo)
	matches := make([]config.BootstrapTestBinding, 0, 1)
	for _, binding := range global.Bootstrap.Test {
		if binding.Repository == identity && binding.BaseBranch == baseBranch {
			matches = append(matches, binding)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("bootstrap Test authorization does not match repository %q and pipeline base %q", identity, baseBranch)
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("bootstrap Test authorization is ambiguous for repository %q and pipeline base %q", identity, baseBranch)
	}
	binding := matches[0]
	content, parsed, err := submittedRepoPolicy(ctx, workDir, run)
	if err != nil {
		return nil, fmt.Errorf("verify bootstrap Test policy: %w", err)
	}
	if parsed.Commands.Test != binding.Command {
		return nil, fmt.Errorf("bootstrap Test command does not exactly match the submitted policy")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	if digest != binding.PolicySHA256 {
		return nil, fmt.Errorf("bootstrap Test policy digest mismatch")
	}
	return &db.BootstrapTestAuthorization{
		Repository:   binding.Repository,
		BaseBranch:   binding.BaseBranch,
		Command:      binding.Command,
		PolicySHA256: binding.PolicySHA256,
	}, nil
}

// applyFrozenBootstrapTestAuthorization reconstructs a recovered run from its
// immutable DB snapshot. Mutable global bootstrap configuration is not an
// input. A base policy that appeared after authorization makes the old
// bootstrap run stale and recovery refuses rather than switching commands.
func applyFrozenBootstrapTestAuthorization(
	ctx context.Context,
	cfg *config.Config,
	run *db.Run,
	repo *db.Repo,
	workDir string,
	trustedPolicy *trustedRepoPolicy,
) error {
	auth, err := run.FrozenBootstrapTestAuthorization()
	if err != nil {
		return err
	}
	if auth == nil {
		return nil
	}
	if trustedPolicy == nil {
		return fmt.Errorf("frozen bootstrap Test authorization requires an authoritative pipeline-base policy result")
	}
	if trustedPolicy.Present {
		return fmt.Errorf("bootstrap Test authorization is stale because the pipeline base now contains trusted repository policy")
	}
	if trustedPolicy.Config != nil {
		return fmt.Errorf("frozen bootstrap Test authorization received an ambiguous trusted-policy result")
	}
	if repo == nil || run == nil {
		return fmt.Errorf("frozen bootstrap Test authorization requires a repository and run")
	}
	identity, err := bootstrapRepositoryIdentity(ctx, repo, workDir)
	if err != nil {
		return err
	}
	if identity != auth.Repository || run.EffectiveBaseBranch(repo) != auth.BaseBranch {
		return fmt.Errorf("frozen bootstrap Test authorization no longer matches the repository and pipeline base")
	}
	content, parsed, err := submittedRepoPolicy(ctx, workDir, run)
	if err != nil {
		return fmt.Errorf("verify frozen bootstrap Test policy: %w", err)
	}
	if parsed.Commands.Test != auth.Command {
		return fmt.Errorf("frozen bootstrap Test command does not match the submitted policy")
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(content)); digest != auth.PolicySHA256 {
		return fmt.Errorf("frozen bootstrap Test policy digest mismatch")
	}
	cfg.Commands.Test = auth.Command
	return nil
}

func bootstrapRepositoryIdentity(ctx context.Context, repo *db.Repo, workDir string) (string, error) {
	recorded, err := repoidentity.Canonical(repo.UpstreamURL)
	if err != nil {
		return "", fmt.Errorf("resolve recorded bootstrap repository identity: %w", err)
	}
	originURL, err := getBootstrapOriginURL(ctx, workDir, "origin")
	if err != nil {
		return "", fmt.Errorf("resolve bootstrap fetch origin: %w", err)
	}
	origin, err := repoidentity.Canonical(originURL)
	if err != nil {
		return "", fmt.Errorf("resolve bootstrap fetch origin identity: %w", err)
	}
	if origin != recorded {
		return "", fmt.Errorf("bootstrap fetch origin %q does not match recorded repository identity %q", origin, recorded)
	}
	return recorded, nil
}

func submittedRepoPolicy(ctx context.Context, workDir string, run *db.Run) ([]byte, *config.RepoConfig, error) {
	sha := run.HeadSHA
	if run.SubmittedHeadSHA != nil && *run.SubmittedHeadSHA != "" {
		sha = *run.SubmittedHeadSHA
	}
	if sha == "" {
		return nil, nil, fmt.Errorf("submitted commit is missing")
	}
	content, err := git.ShowFileBytes(ctx, workDir, sha, repoPolicyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("submitted %s is missing or unreadable: %w", repoPolicyPath, err)
	}
	parsed, err := config.LoadRepoFromBytes(content)
	if err != nil {
		return nil, nil, fmt.Errorf("submitted %s is malformed: %w", repoPolicyPath, err)
	}
	return content, parsed, nil
}
