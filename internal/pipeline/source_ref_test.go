package pipeline

import (
	"context"
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
	ref := "refs/heads/fm/feature"
	inner := &sourceRefCaptureAgent{}
	wrapped := &sourceRefAgent{inner: inner, run: &db.Run{Branch: "fm/feature", SourceRef: &ref}}
	_, err := wrapped.Run(context.Background(), agent.RunOpts{Env: []string{
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
	if !agent.SupportsSessionResume(wrapped) || !agent.SupportsSessionProvider(wrapped, "capture") || !agent.ReportsAgentAttempts(wrapped) || !agent.NeutralizesGateInstructions(wrapped) {
		t.Fatal("source-ref wrapper hid agent capabilities")
	}
}
