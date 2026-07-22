package sourceprovenance

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalSourceRefFromBranch(t *testing.T) {
	t.Parallel()
	ref, err := CanonicalSourceRefFromBranch("fm/arena-no-mistakes-beta")
	if err != nil {
		t.Fatal(err)
	}
	if ref != "refs/heads/fm/arena-no-mistakes-beta" {
		t.Fatalf("ref = %q", ref)
	}

	if full, err := CanonicalSourceRefFromBranch("refs/heads/feature"); err != nil || full != "refs/heads/feature" {
		t.Fatalf("full head ref = %q, err=%v", full, err)
	}

	for _, branch := range []string{"feature!", "feature$", "feature%", "feature;next", "feature&next", "feature(test)"} {
		if _, err := CanonicalSourceRefFromBranch(branch); err != nil {
			t.Fatalf("Git-valid branch %q rejected: %v", branch, err)
		}
	}

	invalid := []string{"", "HEAD", "(detached)", "@", "@{-1}", "refs/heads/@{-1}", "refs/tags/v1", "refs/remotes/origin/feature", "refs/notes/x", "feature..bad", "feature@{bad", "/feature", "feature/", ".feature", "fm/.feature", "feature.lock", "-feature", "feature bad"}
	for _, branch := range invalid {
		branch := branch
		t.Run(strings.ReplaceAll(branch, "/", "_"), func(t *testing.T) {
			t.Parallel()
			if _, err := CanonicalSourceRefFromBranch(branch); err == nil {
				t.Fatalf("CanonicalSourceRefFromBranch(%q) succeeded", branch)
			}
		})
	}
}

func TestValidateFrozenSourceRefRejectsNonHeadAndBranchMismatch(t *testing.T) {
	t.Parallel()
	for _, ref := range []string{"", "HEAD", "refs/tags/feature", "refs/remotes/origin/feature", "refs/notes/feature", "refs/heads/other"} {
		if err := ValidateFrozenSourceRef(ref, "feature"); err == nil {
			t.Fatalf("ValidateFrozenSourceRef(%q) succeeded", ref)
		}
	}
	if err := ValidateFrozenSourceRef("refs/heads/feature", "feature"); err != nil {
		t.Fatal(err)
	}
}

func TestAuthoritativeEnvOverridesSpoofedValueLast(t *testing.T) {
	env := AuthoritativeEnv([]string{"A=1", EnvironmentVariable + "=refs/heads/spoof", "B=2"}, "refs/heads/feature")
	if got := env[len(env)-1]; got != EnvironmentVariable+"=refs/heads/feature" {
		t.Fatalf("last env = %q", got)
	}
	count := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, EnvironmentVariable+"=") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("source-ref env count = %d: %v", count, env)
	}
}

func TestBindCandidateDetachedMultipleCommitsDoesNotMutateRemote(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	primary := filepath.Join(root, "primary")
	pipeline := filepath.Join(root, "pipeline")
	gitTest(t, root, "init", "--bare", remote)
	gitTest(t, root, "clone", remote, primary)
	gitTest(t, primary, "config", "user.name", "test")
	gitTest(t, primary, "config", "user.email", "test@example.com")
	gitTest(t, primary, "checkout", "-b", "fm/source-ref")
	if err := os.WriteFile(filepath.Join(primary, "file.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitTest(t, primary, "add", ".")
	gitTest(t, primary, "commit", "-m", "one")
	gitTest(t, primary, "push", "origin", "HEAD:refs/heads/fm/source-ref")
	original := gitTest(t, primary, "rev-parse", "HEAD")
	gitTest(t, root, "clone", remote, pipeline)
	gitTest(t, pipeline, "checkout", "--detach", original)
	gitTest(t, pipeline, "config", "user.name", "test")
	gitTest(t, pipeline, "config", "user.email", "test@example.com")

	ref := "refs/heads/fm/source-ref"
	if err := BindCandidate(ctx, pipeline, ref, original); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := os.WriteFile(filepath.Join(pipeline, "file.txt"), []byte(strings.Repeat("next\n", i+1)), 0o644); err != nil {
			t.Fatal(err)
		}
		gitTest(t, pipeline, "add", ".")
		gitTest(t, pipeline, "commit", "-m", "pipeline fix")
		candidate := gitTest(t, pipeline, "rev-parse", "HEAD")
		if err := BindCandidate(ctx, pipeline, ref, candidate); err != nil {
			t.Fatal(err)
		}
		if got := gitTest(t, pipeline, "rev-parse", ref); got != candidate {
			t.Fatalf("bound ref = %s, want %s", got, candidate)
		}
	}
	if got := gitTest(t, primary, "rev-parse", "HEAD"); got != original {
		t.Fatalf("primary moved to %s", got)
	}
	if got := gitTest(t, pipeline, "ls-remote", "origin", ref); !strings.HasPrefix(got, original+"\t") {
		t.Fatalf("remote moved: %s", got)
	}
}

func TestBindCandidateRefusesHeadMismatch(t *testing.T) {
	dir := t.TempDir()
	gitTest(t, dir, "init")
	gitTest(t, dir, "config", "user.name", "test")
	gitTest(t, dir, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitTest(t, dir, "add", ".")
	gitTest(t, dir, "commit", "-m", "x")
	if err := BindCandidate(context.Background(), dir, "refs/heads/feature", strings.Repeat("0", 40)); err == nil {
		t.Fatal("expected mismatch refusal")
	}
}

func gitTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
