//go:build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestFirstPolicyBootstrapJourney(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude", Scenario: cleanReviewScenario(t)})
	ctx := context.Background()
	httpRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(httpRoot, "test"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(h.UpstreamDir, filepath.Join(httpRoot, "test", "repo.git")); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	daemonCtx, cancelDaemon := context.WithCancel(context.Background())
	daemon := exec.CommandContext(daemonCtx, "git", "daemon", "--reuseaddr", "--export-all", "--enable=receive-pack", "--listen=127.0.0.1", fmt.Sprintf("--port=%d", port), "--base-path="+httpRoot, httpRoot)
	if err := daemon.Start(); err != nil {
		cancelDaemon()
		t.Fatalf("start git daemon: %v", err)
	}
	t.Cleanup(func() {
		cancelDaemon()
		_ = daemon.Wait()
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("git daemon did not become ready: %v", dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	parentURL := fmt.Sprintf("git://127.0.0.1:%d/test/repo.git", port)
	parentIdentity, err := repoidentity.Canonical(parentURL)
	if err != nil {
		t.Fatal(err)
	}

	if out, err := h.runGit(ctx, h.WorkDir, "rm", ".no-mistakes.yaml"); err != nil {
		t.Fatalf("remove initial policy: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "commit", "-m", "prepare first policy adoption"); err != nil {
		t.Fatalf("commit policy removal: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "push", "origin", "main"); err != nil {
		t.Fatalf("push policy-free base: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.UpstreamDir, "update-server-info"); err != nil {
		t.Fatalf("prepare dumb HTTP repository: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "remote", "set-url", "origin", parentURL); err != nil {
		t.Fatalf("set canonical parent origin: %v\n%s", err, out)
	}

	initOutput, err := h.Run("init")
	if err != nil {
		t.Fatalf("init: %v\n%s", err, initOutput)
	}

	branch := "feature/first-policy"
	if out, err := h.runGit(ctx, h.WorkDir, "checkout", "-b", branch, "main"); err != nil {
		t.Fatalf("create policy branch: %v\n%s", err, out)
	}
	bootstrapMarker := filepath.Join(t.TempDir(), "bootstrap-test-ran")
	bootstrapCommand := fmt.Sprintf("printf bootstrap-authorized > %s", shellQuote(bootstrapMarker))
	policy := []byte(fmt.Sprintf("allow_repo_commands: true\ncommands:\n  test: %s\n", bootstrapCommand))
	if err := os.WriteFile(filepath.Join(h.WorkDir, ".no-mistakes.yaml"), policy, 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "add", ".no-mistakes.yaml"); err != nil {
		t.Fatalf("stage first policy: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "commit", "-m", "adopt repository policy"); err != nil {
		t.Fatalf("commit first policy: %v\n%s", err, out)
	}

	digest := sha256.Sum256(policy)
	globalPath := filepath.Join(h.NMHome, "config.yaml")
	global, err := os.ReadFile(globalPath)
	if err != nil {
		t.Fatal(err)
	}
	binding := fmt.Sprintf("bootstrap:\n  test:\n    - repository: %s\n      base_branch: main\n      command: %s\n      policy_sha256: %x\n", parentIdentity, bootstrapCommand, digest)
	if err := os.WriteFile(globalPath, append(global, []byte(binding)...), 0o644); err != nil {
		t.Fatal(err)
	}

	h.PushToGate(branch)
	run := h.WaitForRun(branch, 90*time.Second)
	if run.Status != types.RunCompleted {
		t.Fatalf("bootstrap run status = %s, error=%v", run.Status, deref(run.Error))
	}
	marker, err := os.ReadFile(bootstrapMarker)
	if err != nil {
		t.Fatalf("bootstrap Test command did not run: %v", err)
	}
	if got := string(marker); got != "bootstrap-authorized" {
		t.Fatalf("bootstrap marker = %q", got)
	}

	database, err := db.Open(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := database.GetRun(run.ID)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	auth, err := persisted.FrozenBootstrapTestAuthorization()
	_ = database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if auth == nil || auth.Repository != parentIdentity || auth.BaseBranch != "main" || auth.Command != bootstrapCommand || auth.PolicySHA256 != fmt.Sprintf("%x", digest) {
		t.Fatalf("frozen bootstrap authorization = %+v", auth)
	}

	if out, err := h.runGit(ctx, h.WorkDir, "checkout", "main"); err != nil {
		t.Fatalf("checkout main: %v\n%s", err, out)
	}
	trustedMarker := filepath.Join(t.TempDir(), "trusted-test-ran")
	trustedCommand := fmt.Sprintf("printf trusted-base > %s", shellQuote(trustedMarker))
	trustedPolicy := fmt.Sprintf("allow_repo_commands: true\ncommands:\n  test: %s\n", trustedCommand)
	if err := os.WriteFile(filepath.Join(h.WorkDir, ".no-mistakes.yaml"), []byte(trustedPolicy), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "add", ".no-mistakes.yaml"); err != nil {
		t.Fatalf("stage trusted policy: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "commit", "-m", "install trusted repository policy"); err != nil {
		t.Fatalf("commit trusted policy: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "push", h.UpstreamDir, "main"); err != nil {
		t.Fatalf("push trusted policy: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.UpstreamDir, "update-server-info"); err != nil {
		t.Fatalf("refresh dumb HTTP repository: %v\n%s", err, out)
	}

	trustedBranch := "feature/after-policy"
	h.CommitChange(trustedBranch, "after-policy.txt", "trusted policy owns commands\n", "verify trusted policy ownership")
	h.PushToGate(trustedBranch)
	trustedRun := h.WaitForRun(trustedBranch, 90*time.Second)
	if trustedRun.Status != types.RunCompleted {
		t.Fatalf("trusted-policy run status = %s, error=%v", trustedRun.Status, deref(trustedRun.Error))
	}
	trustedMarkerData, err := os.ReadFile(trustedMarker)
	if err != nil {
		t.Fatalf("trusted base Test command did not run: %v", err)
	}
	if got := string(trustedMarkerData); got != "trusted-base" {
		t.Fatalf("trusted marker = %q", got)
	}

	statusOutput, err := h.Run("axi", "status")
	if err != nil {
		t.Fatalf("axi status: %v\n%s", err, statusOutput)
	}
	t.Logf("USER JOURNEY EVIDENCE\ninit: %s\nbootstrap push: status=%s marker=%s frozen_repository=%s frozen_base=%s frozen_digest=%s\ntrusted-base push: status=%s marker=%s\naxi status: %s",
		strings.TrimSpace(initOutput), run.Status, marker, auth.Repository, auth.BaseBranch, auth.PolicySHA256,
		trustedRun.Status, trustedMarkerData, strings.TrimSpace(statusOutput))
}
