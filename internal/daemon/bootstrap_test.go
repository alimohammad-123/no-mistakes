package daemon

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type bootstrapFixture struct {
	workDir    string
	bareDir    string
	policy     []byte
	featureSHA string
	repo       *db.Repo
	run        *db.Run
}

func newBootstrapFixture(t *testing.T, policy []byte) bootstrapFixture {
	t.Helper()
	workDir := filepath.Join(t.TempDir(), "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, workDir, "init", "--initial-branch=staging")
	gitCmd(t, workDir, "config", "user.email", "test@test.com")
	gitCmd(t, workDir, "config", "user.name", "Test")
	gitCmd(t, workDir, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# bootstrap\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, workDir, "add", ".")
	gitCmd(t, workDir, "commit", "-m", "staging without policy")
	baseSHA := gitOutput(t, workDir, "rev-parse", "HEAD")

	bareDir := filepath.Join(t.TempDir(), "origin.git")
	gitCmd(t, "", "init", "--bare", bareDir)
	gitCmd(t, workDir, "remote", "add", "origin", bareDir)
	gitCmd(t, workDir, "push", "origin", "staging")

	gitCmd(t, workDir, "checkout", "-b", "feature/policy")
	if err := os.WriteFile(filepath.Join(workDir, ".no-mistakes.yaml"), policy, 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, workDir, "add", ".no-mistakes.yaml")
	gitCmd(t, workDir, "commit", "-m", "add policy")
	featureSHA := gitOutput(t, workDir, "rev-parse", "HEAD")
	gitCmd(t, workDir, "push", "origin", "feature/policy")
	gitCmd(t, workDir, "remote", "set-url", "origin", "https://github.com/owner/repo.git")

	repo := &db.Repo{ID: "repo", WorkingPath: workDir, UpstreamURL: "https://github.com/owner/repo.git", DefaultBranch: "main", BaseBranch: "staging"}
	submitted := featureSHA
	run := &db.Run{ID: "run", RepoID: repo.ID, Branch: "feature/policy", HeadSHA: featureSHA, BaseSHA: baseSHA, BaseBranch: "staging", SubmittedHeadSHA: &submitted}
	return bootstrapFixture{workDir: workDir, bareDir: bareDir, policy: policy, featureSHA: featureSHA, repo: repo, run: run}
}

func bootstrapBindingFor(policy []byte) config.BootstrapTestBinding {
	digest := sha256.Sum256(policy)
	return config.BootstrapTestBinding{
		Repository:   "github.com/owner/repo",
		BaseBranch:   "staging",
		Command:      "go test ./...",
		PolicySHA256: fmt.Sprintf("%x", digest),
	}
}

func bootstrapAbsenceProof() *trustedRepoPolicy {
	return &trustedRepoPolicy{RepositoryIdentity: "github.com/owner/repo"}
}

type bootstrapCaptureStep struct {
	seen chan *config.Config
}

func (s *bootstrapCaptureStep) Name() types.StepName { return types.StepTest }
func (s *bootstrapCaptureStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	s.seen <- sctx.Config
	return &pipeline.StepOutcome{}, nil
}

func TestPushReceivedUsesOnlyBootstrapTestCommandFromFeaturePolicy(t *testing.T) {
	step := &bootstrapCaptureStep{seen: make(chan *config.Config, 1)}
	p, database := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
	repo, _ := setupTestGitRepo(t, p, database, "bootstrap-integration")
	originReads := make(chan struct{}, 2)
	oldGetOriginURL := getBootstrapOriginURL
	getBootstrapOriginURL = func(context.Context, string, string) (string, error) {
		originReads <- struct{}{}
		if len(originReads) > 1 {
			return "https://github.com/other/repo.git", nil
		}
		return repo.UpstreamURL, nil
	}
	t.Cleanup(func() { getBootstrapOriginURL = oldGetOriginURL })
	fetchedSources := make(chan string, 1)
	oldFetch := fetchInitialTrustedRemoteBranch
	fetchInitialTrustedRemoteBranch = func(ctx context.Context, workDir, remote, branch string) error {
		fetchedSources <- remote
		return gitpkg.FetchRemoteBranchToRef(ctx, workDir, p.RepoDir(repo.ID), branch, "refs/remotes/origin/"+branch)
	}
	t.Cleanup(func() { fetchInitialTrustedRemoteBranch = oldFetch })

	gitCmd(t, repo.WorkingPath, "checkout", "-b", "staging")
	gitCmd(t, repo.WorkingPath, "rm", ".no-mistakes.yaml")
	gitCmd(t, repo.WorkingPath, "commit", "-m", "staging without policy")
	gitCmd(t, repo.WorkingPath, "push", "gate", "staging")
	gitCmd(t, repo.WorkingPath, "checkout", "-b", "feature/bootstrap")
	policy := []byte("agent: codex\ncommands:\n  test: go test ./...\n  lint: hostile-feature-lint\n  format: hostile-feature-format\n")
	if err := os.WriteFile(filepath.Join(repo.WorkingPath, ".no-mistakes.yaml"), policy, 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, repo.WorkingPath, "add", ".no-mistakes.yaml")
	gitCmd(t, repo.WorkingPath, "commit", "-m", "adopt policy")
	headSHA := gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD")
	gitCmd(t, repo.WorkingPath, "push", "gate", "feature/bootstrap")
	if _, err := database.UpdateRepoMetadataAndBase(repo.ID, repo.UpstreamURL, "main", "staging"); err != nil {
		t.Fatal(err)
	}

	configData, err := os.ReadFile(p.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(policy)
	configData = append(configData, []byte(fmt.Sprintf("bootstrap:\n  test:\n    - repository: github.com/test/repo\n      base_branch: staging\n      command: go test ./...\n      policy_sha256: %x\n", digest))...)
	if err := os.WriteFile(p.ConfigFile(), configData, 0o644); err != nil {
		t.Fatal(err)
	}

	client, err := ipc.Dial(p.Socket())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var result ipc.PushReceivedResult
	if err := client.Call(ipc.MethodPushReceived, &ipc.PushReceivedParams{
		Gate: p.RepoDir(repo.ID), Ref: "refs/heads/feature/bootstrap", New: headSHA,
	}, &result); err != nil {
		t.Fatal(err)
	}
	waitForRunTerminalState(t, database, result.RunID)
	select {
	case cfg := <-step.seen:
		if cfg.Commands.Test != "go test ./..." {
			t.Fatalf("test command = %q", cfg.Commands.Test)
		}
		if cfg.Commands.Lint != "" || cfg.Commands.Format != "" {
			t.Fatalf("feature commands escaped trust boundary: %+v", cfg.Commands)
		}
		if cfg.Agent != types.AgentClaude {
			t.Fatalf("feature agent override escaped trust boundary: %q", cfg.Agent)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap capture step did not execute")
	}
	run, err := database.GetRun(result.RunID)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := run.FrozenBootstrapTestAuthorization()
	if err != nil || auth == nil || auth.Command != "go test ./..." {
		t.Fatalf("run authorization = %+v, err=%v", auth, err)
	}
	if got := <-fetchedSources; got != repo.UpstreamURL {
		t.Fatalf("trusted fetch source = %q, want captured %q", got, repo.UpstreamURL)
	}
	if got := len(originReads); got != 1 {
		t.Fatalf("bootstrap origin read %d times, want exactly once before fetch", got)
	}
}

func TestPushReceivedTrustedPolicyIgnoresUnrelatedBootstrapBinding(t *testing.T) {
	step := &bootstrapCaptureStep{seen: make(chan *config.Config, 1)}
	p, database := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
	repo, _ := setupTestGitRepo(t, p, database, "bootstrap-trusted-policy")

	policy := []byte("commands:\n  test: trusted-base-test\n")
	if err := os.WriteFile(filepath.Join(repo.WorkingPath, ".no-mistakes.yaml"), policy, 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, repo.WorkingPath, "add", ".no-mistakes.yaml")
	gitCmd(t, repo.WorkingPath, "commit", "-m", "install trusted policy")
	headSHA := gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD")
	gitCmd(t, repo.WorkingPath, "push", "gate", "HEAD:refs/heads/main")
	gitCmd(t, repo.WorkingPath, "push", "gate", "HEAD:refs/heads/feature/trusted-policy")

	configData, err := os.ReadFile(p.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(policy)
	configData = append(configData, []byte(fmt.Sprintf("bootstrap:\n  test:\n    - repository: github.com/other/repo\n      base_branch: main\n      command: stale-bootstrap-test\n      policy_sha256: %x\n", digest))...)
	if err := os.WriteFile(p.ConfigFile(), configData, 0o644); err != nil {
		t.Fatal(err)
	}

	client, err := ipc.Dial(p.Socket())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var result ipc.PushReceivedResult
	if err := client.Call(ipc.MethodPushReceived, &ipc.PushReceivedParams{
		Gate: p.RepoDir(repo.ID), Ref: "refs/heads/feature/trusted-policy", New: headSHA,
	}, &result); err != nil {
		t.Fatal(err)
	}
	waitForRunTerminalState(t, database, result.RunID)
	select {
	case cfg := <-step.seen:
		if cfg.Commands.Test != "trusted-base-test" {
			t.Fatalf("test command = %q, want trusted base command", cfg.Commands.Test)
		}
	case <-time.After(time.Second):
		t.Fatal("trusted policy capture step did not execute")
	}
	run, err := database.GetRun(result.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if auth, err := run.FrozenBootstrapTestAuthorization(); err != nil || auth != nil {
		t.Fatalf("trusted-policy run gained bootstrap authorization: auth=%+v err=%v", auth, err)
	}
}

func TestResolveBootstrapTestAuthorizationSuccess(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	global := config.DefaultGlobalConfig()
	global.Bootstrap.Test = []config.BootstrapTestBinding{bootstrapBindingFor(policy)}

	auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, bootstrapAbsenceProof())
	if err != nil {
		t.Fatalf("resolveBootstrapTestAuthorization: %v", err)
	}
	if auth == nil || auth.Command != "go test ./..." || auth.Repository != "github.com/owner/repo" || auth.BaseBranch != "staging" {
		t.Fatalf("authorization = %+v", auth)
	}
}

func TestResolveBootstrapTestAuthorizationRequiresProvenBaseAbsence(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	global := config.DefaultGlobalConfig()
	global.Bootstrap.Test = []config.BootstrapTestBinding{bootstrapBindingFor(policy)}

	if auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, nil); err == nil {
		t.Fatalf("authorization without absence proof = %+v", auth)
	}
}

func TestResolveBootstrapTestAuthorizationBindsExactPolicyBytes(t *testing.T) {
	policyWithoutFinalNewline := []byte("commands:\n  test: go test ./...")
	fixture := newBootstrapFixture(t, policyWithoutFinalNewline)
	global := config.DefaultGlobalConfig()
	global.Bootstrap.Test = []config.BootstrapTestBinding{bootstrapBindingFor(append(append([]byte(nil), policyWithoutFinalNewline...), '\n'))}

	if auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, bootstrapAbsenceProof()); err == nil {
		t.Fatalf("semantically equal policy with different bytes authorized: %+v", auth)
	}
}

func TestBootstrapFetchSourceRejectsMismatchedOrigin(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	gitCmd(t, fixture.workDir, "remote", "set-url", "origin", "https://github.com/other/repo.git")

	if source, identity, err := bootstrapFetchSource(context.Background(), fixture.repo, fixture.workDir); err == nil {
		t.Fatalf("mismatched fetch origin accepted: source=%q identity=%q", source, identity)
	}
}

func TestResolveBootstrapTestAuthorizationRefusals(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	valid := bootstrapBindingFor(policy)
	for _, tc := range []struct {
		name     string
		bindings []config.BootstrapTestBinding
	}{
		{name: "command mismatch", bindings: []config.BootstrapTestBinding{func() config.BootstrapTestBinding { b := valid; b.Command = "go test ./internal/..."; return b }()}},
		{name: "digest mismatch", bindings: []config.BootstrapTestBinding{func() config.BootstrapTestBinding { b := valid; b.PolicySHA256 = strings.Repeat("0", 64); return b }()}},
		{name: "ambiguous duplicate", bindings: []config.BootstrapTestBinding{valid, valid}},
		{name: "partial binding", bindings: []config.BootstrapTestBinding{{Repository: valid.Repository, BaseBranch: valid.BaseBranch, Command: valid.Command}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			global := config.DefaultGlobalConfig()
			global.Bootstrap.Test = tc.bindings
			if auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, bootstrapAbsenceProof()); err == nil {
				t.Fatalf("authorization %+v accepted", auth)
			}
		})
	}
}

func TestResolveBootstrapTestAuthorizationIgnoresUnrelatedBindings(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	valid := bootstrapBindingFor(policy)
	for _, binding := range []config.BootstrapTestBinding{
		func() config.BootstrapTestBinding { b := valid; b.Repository = "github.com/other/repo"; return b }(),
		func() config.BootstrapTestBinding { b := valid; b.BaseBranch = "main"; return b }(),
	} {
		global := config.DefaultGlobalConfig()
		global.Bootstrap.Test = []config.BootstrapTestBinding{binding}
		auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, &trustedRepoPolicy{})
		if err != nil {
			t.Fatalf("unrelated binding blocked ordinary policy path: %v", err)
		}
		if auth != nil {
			t.Fatalf("unrelated binding authorized bootstrap: %+v", auth)
		}
	}
}

func TestResolveBootstrapTestAuthorizationRefusesMissingSubmittedPolicy(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	global := config.DefaultGlobalConfig()
	global.Bootstrap.Test = []config.BootstrapTestBinding{bootstrapBindingFor(policy)}
	run := *fixture.run
	run.SubmittedHeadSHA = &run.BaseSHA

	if auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, &run, fixture.workDir, bootstrapAbsenceProof()); err == nil {
		t.Fatalf("missing submitted policy authorized: %+v", auth)
	}
}

func TestResolveBootstrapTestAuthorizationStepsAsideWhenTrustedPolicyExists(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	global := config.DefaultGlobalConfig()
	binding := bootstrapBindingFor(policy)
	binding.Command = "stale bootstrap command"
	global.Bootstrap.Test = []config.BootstrapTestBinding{binding}

	auth, err := resolveBootstrapTestAuthorization(context.Background(), global, fixture.repo, fixture.run, fixture.workDir, &trustedRepoPolicy{Present: true, Config: &config.RepoConfig{Commands: config.Commands{Test: "trusted command"}}})
	if err != nil {
		t.Fatalf("trusted policy should make bootstrap step aside: %v", err)
	}
	if auth != nil {
		t.Fatalf("authorization = %+v, want nil", auth)
	}
}

func TestLoadRecoveredConfigUsesFrozenBootstrapAfterGlobalMutation(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.ConfigFile(), []byte("agent: auto\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepoWithIDAndForkAndBase("repo", fixture.workDir, fixture.repo.UpstreamURL, "", "main", "staging")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRunWithBaseBranch(repo.ID, "feature/policy", fixture.featureSHA, fixture.run.BaseSHA, "staging")
	if err != nil {
		t.Fatal(err)
	}
	binding := bootstrapBindingFor(policy)
	if err := database.SetRunBootstrapTestAuthorization(run.ID, db.BootstrapTestAuthorization(binding)); err != nil {
		t.Fatal(err)
	}
	run, err = database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldFetch := fetchRecoveredRemoteBranch
	fetchRecoveredRemoteBranch = func(ctx context.Context, workDir, remote, branch string) error {
		if remote != fixture.repo.UpstreamURL {
			return fmt.Errorf("recovery fetch source = %q, want captured %q", remote, fixture.repo.UpstreamURL)
		}
		return gitpkg.FetchRemoteBranchToRef(ctx, workDir, fixture.bareDir, branch, "refs/remotes/origin/"+branch)
	}
	t.Cleanup(func() { fetchRecoveredRemoteBranch = oldFetch })

	mgr := NewRunManager(database, p, nil)
	cfg, err := mgr.loadRecoveredConfig(context.Background(), run, repo, fixture.workDir)
	if err != nil {
		t.Fatalf("loadRecoveredConfig: %v", err)
	}
	if cfg.Commands.Test != "go test ./..." {
		t.Fatalf("recovered test command = %q, want frozen command", cfg.Commands.Test)
	}
}

func TestRecoverOnStartupRunsTestFromFrozenBootstrapAfterBindingRemoval(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dtest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	p := paths.WithRoot(tmpDir)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	mockClaude := writeMockClaude(t, t.TempDir())
	// The bootstrap binding has been removed before recovery. Only the frozen
	// run snapshot may supply Test now.
	if err := os.WriteFile(p.ConfigFile(), []byte("agent: claude\nagent_path_override:\n  claude: "+mockClaude+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, _ := setupTestGitRepo(t, p, database, "bootstrap-recovery")
	oldGetOriginURL := getBootstrapOriginURL
	getBootstrapOriginURL = func(context.Context, string, string) (string, error) { return repo.UpstreamURL, nil }
	t.Cleanup(func() { getBootstrapOriginURL = oldGetOriginURL })
	oldFetch := fetchRecoveredRemoteBranch
	fetchRecoveredRemoteBranch = func(ctx context.Context, workDir, remote, branch string) error {
		if remote != repo.UpstreamURL {
			return fmt.Errorf("recovery fetch source = %q, want captured %q", remote, repo.UpstreamURL)
		}
		return gitpkg.FetchRemoteBranchToRef(ctx, workDir, p.RepoDir(repo.ID), branch, "refs/remotes/origin/"+branch)
	}
	t.Cleanup(func() { fetchRecoveredRemoteBranch = oldFetch })

	gitCmd(t, repo.WorkingPath, "checkout", "-b", "staging")
	gitCmd(t, repo.WorkingPath, "rm", ".no-mistakes.yaml")
	gitCmd(t, repo.WorkingPath, "commit", "-m", "staging without policy")
	baseSHA := gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD")
	gitCmd(t, repo.WorkingPath, "push", "gate", "staging")
	gitCmd(t, repo.WorkingPath, "checkout", "-b", "feature/bootstrap-recovery")
	policy := []byte("commands:\n  test: go test ./...\n")
	if err := os.WriteFile(filepath.Join(repo.WorkingPath, repoPolicyPath), policy, 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, repo.WorkingPath, "add", repoPolicyPath)
	gitCmd(t, repo.WorkingPath, "commit", "-m", "adopt policy")
	headSHA := gitOutput(t, repo.WorkingPath, "rev-parse", "HEAD")
	gitCmd(t, repo.WorkingPath, "push", "gate", "feature/bootstrap-recovery")
	repo, err = database.UpdateRepoMetadataAndBase(repo.ID, repo.UpstreamURL, "main", "staging")
	if err != nil {
		t.Fatal(err)
	}

	run, err := database.InsertRunWithBaseBranch(repo.ID, "feature/bootstrap-recovery", headSHA, baseSHA, "staging")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(policy)
	if err := database.SetRunBootstrapTestAuthorization(run.ID, db.BootstrapTestAuthorization{
		Repository: "github.com/test/repo", BaseBranch: "staging", Command: "go test ./...", PolicySHA256: fmt.Sprintf("%x", digest),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	worktree := p.WorktreeDir(repo.ID, run.ID)
	if err := gitpkg.WorktreeAdd(context.Background(), p.RepoDir(repo.ID), worktree, headSHA); err != nil {
		t.Fatal(err)
	}
	review, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStep(review.ID); err != nil {
		t.Fatal(err)
	}
	findings := `{"findings":[{"id":"review-1","severity":"warning","description":"needs approval","action":"ask-user"}],"summary":"needs approval"}`
	if err := database.SetStepFindings(review.ID, findings); err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertStepRound(review.ID, 1, "initial", &findings, nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatusWithDuration(review.ID, types.StepStatusAwaitingApproval, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.SetRunAwaitingAgent(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertStepResult(run.ID, types.StepTest); err != nil {
		t.Fatal(err)
	}

	capture := &bootstrapCaptureStep{seen: make(chan *config.Config, 1)}
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunWithOptions(p, database, func() []pipeline.Step {
			return []pipeline.Step{&mockApprovalStep{name: types.StepReview}, capture}
		})
	}()
	defer func() {
		client, dialErr := ipc.Dial(p.Socket())
		if dialErr == nil {
			_ = client.Call(ipc.MethodShutdown, &ipc.ShutdownParams{}, nil)
			_ = client.Close()
		}
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		client, dialErr := ipc.Dial(p.Socket())
		if dialErr == nil {
			var response ipc.RespondResult
			dialErr = client.Call(ipc.MethodRespond, &ipc.RespondParams{RunID: run.ID, Step: types.StepReview, Action: types.ActionApprove}, &response)
			_ = client.Close()
			if dialErr == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered bootstrap run never accepted approval: %v", dialErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case cfg := <-capture.seen:
		if cfg.Commands.Test != "go test ./..." {
			t.Fatalf("recovered Test command = %q, want frozen command", cfg.Commands.Test)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovered Test step did not execute")
	}
	completed := waitForRunTerminalState(t, database, run.ID)
	if completed.Status != types.RunCompleted {
		t.Fatalf("recovered run status = %s, want completed", completed.Status)
	}
}

func TestLoadRecoveredConfigRejectsBootstrapAfterBasePolicyAppears(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	binding := bootstrapBindingFor(policy)
	auth := db.BootstrapTestAuthorization(binding)
	fixture.run.BootstrapTestRepository = &auth.Repository
	fixture.run.BootstrapTestBaseBranch = &auth.BaseBranch
	fixture.run.BootstrapTestCommand = &auth.Command
	fixture.run.BootstrapTestPolicySHA256 = &auth.PolicySHA256
	cfg := config.Merge(config.DefaultGlobalConfig(), &config.RepoConfig{})

	if err := applyFrozenBootstrapTestAuthorization(context.Background(), cfg, fixture.run, fixture.repo, fixture.workDir, &trustedRepoPolicy{Present: true, Config: &config.RepoConfig{Commands: config.Commands{Test: "trusted"}}}); err == nil {
		t.Fatal("recovery accepted stale bootstrap authorization after base policy appeared")
	}
}

func TestApplyFrozenBootstrapTestAuthorizationRejectsMismatchedFetchOrigin(t *testing.T) {
	policy := []byte("commands:\n  test: go test ./...\n")
	fixture := newBootstrapFixture(t, policy)
	binding := bootstrapBindingFor(policy)
	auth := db.BootstrapTestAuthorization(binding)
	fixture.run.BootstrapTestRepository = &auth.Repository
	fixture.run.BootstrapTestBaseBranch = &auth.BaseBranch
	fixture.run.BootstrapTestCommand = &auth.Command
	fixture.run.BootstrapTestPolicySHA256 = &auth.PolicySHA256
	cfg := config.Merge(config.DefaultGlobalConfig(), &config.RepoConfig{})

	if err := applyFrozenBootstrapTestAuthorization(context.Background(), cfg, fixture.run, fixture.repo, fixture.workDir, &trustedRepoPolicy{RepositoryIdentity: "github.com/other/repo"}); err == nil {
		t.Fatal("recovery accepted bootstrap authorization from a mismatched fetch origin")
	}
}
