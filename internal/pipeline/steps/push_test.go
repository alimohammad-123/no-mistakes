package steps

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/config"
)

func TestPushStep_ReconcilesStaleDatabaseHeadSHA(t *testing.T) {
	t.Parallel()
	// When push retries after a prior UpdateRunHeadSHA failure, there are no
	// uncommitted changes. The step must still reconcile the DB if HeadSHA is stale.
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	os.WriteFile(filepath.Join(dir, "init.txt"), []byte("init"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "initial")
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")

	gitCmd(t, dir, "checkout", "-b", "feature")
	os.WriteFile(filepath.Join(dir, "feature.txt"), []byte("feature"), 0o644)
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "feature")
	actualHeadSHA := gitCmd(t, dir, "rev-parse", "HEAD")
	baseSHA := gitCmd(t, dir, "rev-parse", "main")
	gitCmd(t, dir, "push", "origin", "feature")

	// Create context with a stale HeadSHA (simulates prior DB write failure)
	staleHeadSHA := baseSHA // intentionally wrong
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, staleHeadSHA, config.Commands{})
	sctx.Repo.UpstreamURL = upstream

	step := &PushStep{}
	_, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}

	// In-memory HeadSHA must match actual HEAD
	if sctx.Run.HeadSHA != actualHeadSHA {
		t.Errorf("Run.HeadSHA = %s, want %s", sctx.Run.HeadSHA, actualHeadSHA)
	}

	// DB record must also be updated
	dbRun, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dbRun.HeadSHA != actualHeadSHA {
		t.Errorf("DB HeadSHA = %s, want %s", dbRun.HeadSHA, actualHeadSHA)
	}
	if dbRun.LastPushedSHA == nil || *dbRun.LastPushedSHA != actualHeadSHA || dbRun.PushGeneration == nil || *dbRun.PushGeneration != 1 {
		t.Fatalf("already-up-to-date push binding = %#v", dbRun)
	}
	if dbRun.PushActive {
		t.Fatal("push-active marker remained set after successful step")
	}
}

func TestPushStep_FormattingCommitYieldsBeforePublishingStaleTestHead(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "init.txt"), []byte("init"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "initial")
	baseSHA := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(dir, "feature.txt"), []byte("feature"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "feature")
	testedHead := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "push", "origin", "feature")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, testedHead, config.Commands{
		Test:   "true",
		Format: "printf formatted > formatted.txt",
	})
	sctx.Repo.UpstreamURL = upstream
	sctx.Run.Branch = "feature"
	recordSuccessfulTestProof(t, sctx, testedHead)

	outcome, err := (&PushStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.ReplayValidation {
		t.Fatal("Push did not yield for configured-Test replay after formatting commit")
	}
	if sctx.Run.HeadSHA == testedHead {
		t.Fatal("formatting did not advance the local pipeline candidate")
	}
	if remote := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); remote != testedHead {
		t.Fatalf("stale-Test candidate was published: remote %s, want %s", remote, testedHead)
	}
	dbRun, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dbRun.HeadSHA != sctx.Run.HeadSHA || dbRun.LastPushedSHA != nil {
		t.Fatalf("local candidate/push provenance = %#v", dbRun)
	}
}

func TestPushStep_RefusesExhaustedReplayBeforeLocalOrRemoteMutation(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")

	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{
		Test:   "true",
		Format: "printf formatted > formatted.txt",
	})
	sctx.Repo.UpstreamURL = upstream
	const prURL = "https://github.com/test/repo/pull/17"
	if err := sctx.DB.UpdateRunPRURL(sctx.Run.ID, prURL); err != nil {
		t.Fatal(err)
	}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	if _, err := (&PushStep{}).Execute(sctx); err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("Execute() error = %v, want replay exhaustion", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "formatted.txt")); !os.IsNotExist(err) {
		t.Fatalf("formatter output exists or stat failed: %v", err)
	}
	if got := gitCmd(t, dir, "rev-parse", "HEAD"); got != headSHA {
		t.Fatalf("local HEAD = %s, want %s", got, headSHA)
	}
	if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != headSHA {
		t.Fatalf("remote HEAD = %s, want %s", got, headSHA)
	}
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PRURL == nil || *run.PRURL != prURL || run.PushActive {
		t.Fatalf("PR or push identity changed: %#v", run)
	}
}

func TestPushStep_AllowsExactBoundaryDeliveryWithoutLocalMutation(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")

	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Repo.UpstreamURL = upstream
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	if _, err := (&PushStep{}).Execute(sctx); err != nil {
		t.Fatal(err)
	}
	if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != headSHA {
		t.Fatalf("remote HEAD = %s, want %s", got, headSHA)
	}
	run, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.LastPushedSHA == nil || *run.LastPushedSHA != headSHA || run.PushActive {
		t.Fatalf("exact boundary push state = %#v", run)
	}
}

func TestPushStep_ClassifiesInRepoEvidenceBeforeMutationPreflight(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, dir, evidenceDir string)
		wantOK bool
	}{
		{
			name:   "clean",
			mutate: func(*testing.T, string, string) {},
			wantOK: true,
		},
		{
			name: "modified",
			mutate: func(t *testing.T, _, evidenceDir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(evidenceDir, "proof.txt"), []byte("modified"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "deleted",
			mutate: func(t *testing.T, _, evidenceDir string) {
				t.Helper()
				if err := os.Remove(filepath.Join(evidenceDir, "proof.txt")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "untracked",
			mutate: func(t *testing.T, _, evidenceDir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(evidenceDir, "new.txt"), []byte("new"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "staged",
			mutate: func(t *testing.T, dir, evidenceDir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(evidenceDir, "proof.txt"), []byte("staged"), 0o644); err != nil {
					t.Fatal(err)
				}
				gitCmd(t, dir, "add", filepath.Join("evidence", "feature", "proof.txt"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, baseSHA, headSHA, evidenceDir := setupCommittedEvidenceRepo(t)
			sctx := newTestContextWithDBRecords(
				t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
			)
			sctx.Run.Branch = "feature"
			sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}
			completeHeadValidationAtCapacity(t, sctx, headSHA)
			tt.mutate(t, dir, evidenceDir)

			err := (&PushStep{}).stageInRepoEvidence(sctx)
			if tt.wantOK {
				if err != nil {
					t.Fatal(err)
				}
				if status := gitStatusPorcelain(t, dir); status != "" {
					t.Fatalf("clean evidence changed index: %q", status)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "did not converge") {
				t.Fatalf("stageInRepoEvidence() error = %v, want replay exhaustion", err)
			}
		})
	}
}

func TestPushStep_AllowsBoundaryDeliveryWithCleanInRepoEvidence(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")
	dir, baseSHA, headSHA, _ := setupCommittedEvidenceRepo(t)
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")

	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Repo.UpstreamURL = upstream
	sctx.Run.Branch = "feature"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	if _, err := (&PushStep{}).Execute(sctx); err != nil {
		t.Fatal(err)
	}
	if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != headSHA {
		t.Fatalf("remote HEAD = %s, want %s", got, headSHA)
	}
}

func TestPushStep_BoundaryEvidenceRaceFallsBackToWholeWorktreePreflight(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")
	dir, baseSHA, headSHA, evidenceDir := setupCommittedEvidenceRepo(t)
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")

	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Repo.UpstreamURL = upstream
	sctx.Run.Branch = "feature"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	step := &PushStep{
		afterEvidenceClassification: func(pending bool) {
			if pending {
				t.Fatal("clean evidence classified as pending")
			}
			if err := os.WriteFile(filepath.Join(evidenceDir, "proof.txt"), []byte("raced"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
	}
	if _, err := step.Execute(sctx); err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("Execute() error = %v, want replay exhaustion", err)
	}
	if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != headSHA {
		t.Fatalf("remote HEAD = %s, want %s", got, headSHA)
	}
}

func TestPushStep_UnrelatedIndexChangeDoesNotReclassifyCleanEvidence(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA, _ := setupCommittedEvidenceRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "unrelated.txt"), []byte("staged"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "unrelated.txt")

	sctx := newTestContextWithDBRecords(
		t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.Run.Branch = "feature"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	if err := (&PushStep{}).stageInRepoEvidence(sctx); err != nil {
		t.Fatal(err)
	}
}

func TestPushStep_RefusesSupersededSourceRefAtUnchangedTestedHead(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir, baseSHA, testedHead := setupGitRepo(t)
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", "origin", "feature")
	gitCmd(t, dir, "checkout", "--detach", testedHead)
	if err := os.WriteFile(filepath.Join(dir, "superseding.txt"), []byte("new push\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "superseding.txt")
	gitCmd(t, dir, "commit", "-m", "superseding push")
	supersedingHead := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "checkout", "--detach", testedHead)
	gitCmd(t, dir, "update-ref", "refs/heads/feature", supersedingHead)

	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, testedHead, config.Commands{Test: "true"})
	sctx.Repo.UpstreamURL = upstream
	recordSuccessfulTestProof(t, sctx, testedHead)

	_, err := (&PushStep{}).Execute(sctx)
	if err == nil || !strings.Contains(err.Error(), "source ref") {
		t.Fatalf("Execute() error = %v, want superseding source-ref refusal", err)
	}
	if got := gitCmd(t, dir, "rev-parse", "refs/heads/feature"); got != supersedingHead {
		t.Fatalf("gate source ref = %s, want superseding head %s", got, supersedingHead)
	}
	if got := gitCmd(t, upstream, "rev-parse", "refs/heads/feature"); got != testedHead {
		t.Fatalf("upstream head = %s, want unchanged tested head %s", got, testedHead)
	}
}

func TestPushStep_ForceAddsInRepoEvidenceArtifacts(t *testing.T) {
	t.Parallel()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--bare")

	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("*.png\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "init.txt"), []byte("init"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "initial")
	gitCmd(t, dir, "remote", "add", "origin", upstream)
	gitCmd(t, dir, "push", "origin", "main")

	gitCmd(t, dir, "checkout", "-b", "feature")
	baseSHA := gitCmd(t, dir, "rev-parse", "main")
	headSHA := gitCmd(t, dir, "rev-parse", "HEAD")
	evidenceDir := filepath.Join(dir, "evidence", "feature")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceDir, "checkout.png"), []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = upstream
	sctx.Run.Branch = "feature"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}

	step := &PushStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	clone := t.TempDir()
	gitCmd(t, clone, "clone", "--branch", "feature", upstream, ".")
	if _, err := os.Stat(filepath.Join(clone, "evidence", "feature", "checkout.png")); err != nil {
		t.Fatalf("expected ignored evidence artifact to be pushed: %v", err)
	}
}

func TestPushStep_TargetsForkWhenConfigured(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	fork := t.TempDir()
	gitCmd(t, parent, "init", "--bare")
	gitCmd(t, fork, "init", "--bare")

	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "init.txt"), []byte("init"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "initial")
	baseSHA := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "remote", "add", "origin", parent)
	gitCmd(t, dir, "push", "origin", "main")
	gitCmd(t, dir, "push", fork, "main")

	gitCmd(t, dir, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(dir, "feature.txt"), []byte("feature"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "-A")
	gitCmd(t, dir, "commit", "-m", "feature")
	headSHA := gitCmd(t, dir, "rev-parse", "HEAD")

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = parent
	sctx.Repo.ForkURL = fork
	sctx.Run.Branch = "feature"

	step := &PushStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	forkSHA := gitCmd(t, fork, "rev-parse", "refs/heads/feature")
	if forkSHA != headSHA {
		t.Fatalf("fork branch SHA = %s, want %s", forkSHA, headSHA)
	}
	if out, err := exec.Command("git", "-C", parent, "rev-parse", "--verify", "refs/heads/feature").CombinedOutput(); err == nil {
		t.Fatalf("parent unexpectedly received feature branch at %s", strings.TrimSpace(string(out)))
	}
	dbRun, err := sctx.DB.GetRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dbRun.LastPushedSHA == nil || *dbRun.LastPushedSHA != headSHA || dbRun.PushTargetKind == nil || *dbRun.PushTargetKind != "fork" || dbRun.PushRef == nil || *dbRun.PushRef != "refs/heads/feature" {
		t.Fatalf("fork push binding = %#v", dbRun)
	}
	if dbRun.PushTargetFingerprint == nil || strings.Contains(*dbRun.PushTargetFingerprint, fork) {
		t.Fatalf("push target fingerprint persisted a URL: %#v", dbRun.PushTargetFingerprint)
	}
}

func TestPushStep_RedactsForkURLInGitErrors(t *testing.T) {
	dir, baseSHA, headSHA := setupGitRepo(t)

	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	binDir := fakeCLIBinDir(t)
	linkTestBinary(t, binDir, "git")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_CLI_MODE", "git-remote-error")
	t.Setenv("FAKE_CLI_REAL_GIT", realGit)

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.Repo.UpstreamURL = "https://github.com/parent/project.git"
	sctx.Repo.ForkURL = "https://user:secret@example.com/fork/project.git"
	sctx.Run.Branch = "refs/heads/feature"

	step := &PushStep{}
	_, err = step.Execute(sctx)
	if err == nil {
		t.Fatal("expected push error")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("expected error to redact fork credentials, got %v", err)
	}
	if !strings.Contains(err.Error(), "https://redacted@example.com/fork/project.git") {
		t.Fatalf("expected redacted fork URL in error, got %v", err)
	}
}

func TestPushStep_DoesNotForceAddIgnoredEvidenceDirectory(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("evidence/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", ".gitignore")
	gitCmd(t, dir, "commit", "-m", "ignore evidence")
	headSHA = gitCmd(t, dir, "rev-parse", "HEAD")
	evidenceDir := filepath.Join(dir, "evidence", "feature")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceDir, "stale.png"), []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}

	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "true"})
	sctx.Run.Branch = "feature"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}
	completeHeadValidationAtCapacity(t, sctx, headSHA)

	step := &PushStep{}
	if err := step.stageInRepoEvidence(sctx); err != nil {
		t.Fatal(err)
	}
	if status := gitStatusPorcelain(t, dir); status != "" {
		t.Fatalf("ignored evidence directory was staged: %q", status)
	}
}

func setupCommittedEvidenceRepo(t *testing.T) (dir, baseSHA, headSHA, evidenceDir string) {
	t.Helper()
	dir, baseSHA, _ = setupGitRepo(t)
	evidenceDir = filepath.Join(dir, "evidence", "feature")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceDir, "proof.txt"), []byte("proof"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "evidence")
	gitCmd(t, dir, "commit", "-m", "add evidence")
	headSHA = gitCmd(t, dir, "rev-parse", "HEAD")
	return dir, baseSHA, headSHA, evidenceDir
}
