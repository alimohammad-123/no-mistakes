package pipeline

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/db"
)

type sourceRefCaptureAgent struct{ opts agent.RunOpts }

func (a *sourceRefCaptureAgent) Name() string { return "capture" }
func (a *sourceRefCaptureAgent) Run(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
	a.opts = opts
	return &agent.Result{}, nil
}
func (a *sourceRefCaptureAgent) Close() error                { return nil }
func (a *sourceRefCaptureAgent) SupportsSessionResume() bool { return true }
func (a *sourceRefCaptureAgent) SupportsSessionProvider(provider string) bool {
	return provider == "capture"
}
func (a *sourceRefCaptureAgent) ReportsAgentAttempts() bool        { return true }
func (a *sourceRefCaptureAgent) NeutralizesGateInstructions() bool { return true }

func TestSourceRefAgentOverridesSpoofAndPreservesCapabilities(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(dir+"/candidate", []byte("candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "candidate"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(dir+"/candidate", []byte("advanced candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "advanced candidate"}, {"checkout", "--detach"}, {"update-ref", "refs/heads/fm/feature", "HEAD^"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	headCmd := exec.Command("git", "rev-parse", "HEAD")
	headCmd.Dir = dir
	headOut, err := headCmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	headSHA := strings.TrimSpace(string(headOut))
	ref := "refs/heads/fm/feature"
	inner := &sourceRefCaptureAgent{}
	wrapped := &sourceRefAgent{inner: inner, run: &db.Run{Branch: "fm/feature", HeadSHA: headSHA, SourceRef: &ref}, workDir: dir}
	_, err = wrapped.Run(context.Background(), agent.RunOpts{Env: []string{
		"NO_MISTAKES_SOURCE_REF=refs/heads/spoof",
		"OTHER=value",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := inner.opts.Env[len(inner.opts.Env)-1]; got != "NO_MISTAKES_SOURCE_REF="+ref {
		t.Fatalf("last env = %q", got)
	}
	count := 0
	for _, entry := range inner.opts.Env {
		if len(entry) >= len("NO_MISTAKES_SOURCE_REF=") && entry[:len("NO_MISTAKES_SOURCE_REF=")] == "NO_MISTAKES_SOURCE_REF=" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("source ref entries = %d: %v", count, inner.opts.Env)
	}
	refCmd := exec.Command("git", "rev-parse", ref)
	refCmd.Dir = dir
	refOut, err := refCmd.Output()
	if err != nil || strings.TrimSpace(string(refOut)) != headSHA {
		t.Fatalf("source ref was not bound to candidate: %s, %v", refOut, err)
	}
	if !agent.SupportsSessionResume(wrapped) || !agent.SupportsSessionProvider(wrapped, "capture") || !agent.ReportsAgentAttempts(wrapped) || !agent.NeutralizesGateInstructions(wrapped) {
		t.Fatal("source-ref wrapper hid agent capabilities")
	}
}
