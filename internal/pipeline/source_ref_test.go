package pipeline

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type sourceRefCaptureAgent struct {
	opts  agent.RunOpts
	calls int
}

func (a *sourceRefCaptureAgent) Name() string { return "capture" }
func (a *sourceRefCaptureAgent) Run(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
	a.opts = opts
	a.calls++
	return &agent.Result{}, nil
}
func (a *sourceRefCaptureAgent) Close() error                { return nil }
func (a *sourceRefCaptureAgent) SupportsSessionResume() bool { return true }
func (a *sourceRefCaptureAgent) SupportsSessionProvider(provider string) bool {
	return provider == "capture"
}
func (a *sourceRefCaptureAgent) ReportsAgentAttempts() bool        { return true }
func (a *sourceRefCaptureAgent) NeutralizesGateInstructions() bool { return true }

func TestSourceRefAgentSuppressesProvenanceOnlyDuringActiveRebase(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("recorded\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "recorded"}, {"branch", "fm/feature"}} {
		gitOutput(t, dir, args...)
	}
	recorded := gitOutput(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("partial\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "partial"}, {"checkout", "--detach"}} {
		gitOutput(t, dir, args...)
	}
	rebaseDir := gitOutput(t, dir, "rev-parse", "--git-path", "rebase-merge")
	if !filepath.IsAbs(rebaseDir) {
		rebaseDir = filepath.Join(dir, rebaseDir)
	}
	if err := os.MkdirAll(rebaseDir, 0o755); err != nil {
		t.Fatal(err)
	}

	ref := "refs/heads/fm/feature"
	inner := &sourceRefCaptureAgent{}
	wrapped := &sourceRefAgent{inner: inner, run: &db.Run{Branch: "fm/feature", HeadSHA: recorded, SourceRef: &ref}, workDir: dir}
	if _, err := wrapped.Run(context.Background(), agent.RunOpts{}); err == nil {
		t.Fatal("stable agent launch accepted a partial rebase head")
	}
	ctx := RebaseConflictAgentContext(context.Background())
	if _, err := wrapped.Run(ctx, agent.RunOpts{Env: []string{"NO_MISTAKES_SOURCE_REF=refs/heads/spoof"}}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range inner.opts.Env {
		if strings.HasPrefix(entry, "NO_MISTAKES_SOURCE_REF=") {
			t.Fatalf("transient rebase agent received source ref: %v", inner.opts.Env)
		}
	}
	if len(inner.opts.UnsetEnv) != 1 || inner.opts.UnsetEnv[0] != "NO_MISTAKES_SOURCE_REF" {
		t.Fatalf("unset env = %v", inner.opts.UnsetEnv)
	}
	if got := gitOutput(t, dir, "rev-parse", ref); got != recorded {
		t.Fatalf("source ref moved during rebase: got %s, want %s", got, recorded)
	}
	if err := os.RemoveAll(rebaseDir); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Run(ctx, agent.RunOpts{}); err == nil {
		t.Fatal("transient suppression succeeded without active rebase")
	}
	if inner.calls != 1 {
		t.Fatalf("inner agent calls = %d, want 1", inner.calls)
	}
}

func TestSourceRefAgentRefusesSupersedingRefWithoutRewind(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "first"}} {
		gitOutput(t, dir, args...)
	}
	first := gitOutput(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("superseding\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "superseding"}, {"branch", "fm/feature"}, {"checkout", "--detach", first}} {
		gitOutput(t, dir, args...)
	}
	superseding := gitOutput(t, dir, "rev-parse", "fm/feature")
	ref := "refs/heads/fm/feature"
	inner := &sourceRefCaptureAgent{}
	wrapped := &sourceRefAgent{inner: inner, run: &db.Run{Branch: "fm/feature", HeadSHA: first, SourceRef: &ref}, workDir: dir}

	if _, err := wrapped.Run(context.Background(), agent.RunOpts{}); err == nil {
		t.Fatal("stale agent launch rewound a superseding source ref")
	}
	if got := gitOutput(t, dir, "rev-parse", ref); got != superseding {
		t.Fatalf("source ref = %s, want superseding candidate %s", got, superseding)
	}
	if inner.calls != 0 {
		t.Fatalf("stale agent launched inner agent %d times", inner.calls)
	}
}

func TestAdvanceHeadSHARefusesSupersedingRefWithoutRewind(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	writeCandidate := func(content, message string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		gitOutput(t, dir, "add", "candidate")
		gitOutput(t, dir, "commit", "-m", message)
		return gitOutput(t, dir, "rev-parse", "HEAD")
	}
	first := writeCandidate("first\n", "first")
	candidate := writeCandidate("candidate\n", "candidate")
	superseding := writeCandidate("superseding\n", "superseding")
	gitOutput(t, dir, "branch", "fm/feature")
	gitOutput(t, dir, "checkout", "--detach", candidate)

	database, err := db.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(dir, "https://github.com/test/repo", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "fm/feature", first, first)
	if err != nil {
		t.Fatal(err)
	}
	sctx := &StepContext{Ctx: context.Background(), Run: run, Repo: repo, WorkDir: dir, DB: database}

	if err := sctx.AdvanceHeadSHA(candidate); err == nil {
		t.Fatal("stale candidate advanced across a superseding source ref")
	}
	if got := gitOutput(t, dir, "rev-parse", "refs/heads/fm/feature"); got != superseding {
		t.Fatalf("source ref = %s, want superseding candidate %s", got, superseding)
	}
	persisted, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.HeadSHA != first {
		t.Fatalf("durable run head = %s, want unchanged %s", persisted.HeadSHA, first)
	}
}

func TestRecoverRunHeadTransitionFinalizesExactMovedRefOnce(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, dir, "add", "candidate")
	gitOutput(t, dir, "commit", "-m", "first")
	first := gitOutput(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, dir, "add", "candidate")
	gitOutput(t, dir, "commit", "-m", "candidate")
	candidate := gitOutput(t, dir, "rev-parse", "HEAD")
	gitOutput(t, dir, "checkout", "--detach", candidate)
	gitOutput(t, dir, "update-ref", "refs/heads/fm/feature", first)

	database, err := db.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(dir, "https://github.com/test/repo", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "fm/feature", first, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, first); err != nil {
		t.Fatal(err)
	}
	testResult, err := database.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteStep(testResult.ID, 0, 1, "test.log"); err != nil {
		t.Fatal(err)
	}
	documentResult, err := database.InsertStepResult(run.ID, types.StepDocument)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStep(documentResult.ID); err != nil {
		t.Fatal(err)
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		t.Fatal(err)
	}
	transition, err := database.BeginRunHeadAdvance(
		run.ID, ref, first, candidate, true, 3, db.HeadAdvancePipeline,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceprovenance.AdvanceCandidate(context.Background(), dir, ref, candidate, first); err != nil {
		t.Fatal(err)
	}
	run, err = database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.HeadAdvanceGeneration != transition.OwnershipGeneration {
		t.Fatalf("run generation = %d, want %d", run.HeadAdvanceGeneration, transition.OwnershipGeneration)
	}

	driftedSteps := []Step{newPassStep(types.StepTest), newPassStep(types.StepLint)}
	if recovered, err := RecoverRunHeadTransition(context.Background(), database, run, dir, driftedSteps); err == nil || recovered {
		t.Fatalf("drifted topology recovery = %v, err %v", recovered, err)
	}
	beforeRecovery, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if beforeRecovery.HeadSHA != first || beforeRecovery.ValidationTargetSHA != nil {
		t.Fatalf("drifted topology changed run state: %#v", beforeRecovery)
	}
	steps := []Step{newPassStep(types.StepTest), newPassStep(types.StepDocument)}
	recovered, err := RecoverRunHeadTransition(context.Background(), database, run, dir, steps)
	if err != nil || !recovered {
		t.Fatalf("recover moved-ref boundary = %v, err %v", recovered, err)
	}
	recovered, err = RecoverRunHeadTransition(context.Background(), database, run, dir, steps)
	if err != nil || recovered {
		t.Fatalf("repeat recovery = %v, err %v", recovered, err)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadSHA != candidate || got.ValidationTargetSHA == nil || *got.ValidationTargetSHA != candidate ||
		got.ValidationReplayCount != 1 {
		t.Fatalf("recovered run state = %#v", got)
	}
	if gotRef := gitOutput(t, dir, "rev-parse", ref); gotRef != candidate {
		t.Fatalf("source ref = %s, want %s", gotRef, candidate)
	}
}

func TestRecoverRunHeadTransitionNeverRewindsSupersedingRef(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	writeCandidate := func(content, message string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		gitOutput(t, dir, "add", "candidate")
		gitOutput(t, dir, "commit", "-m", message)
		return gitOutput(t, dir, "rev-parse", "HEAD")
	}
	first := writeCandidate("first\n", "first")
	candidate := writeCandidate("candidate\n", "candidate")
	superseding := writeCandidate("superseding\n", "superseding")
	gitOutput(t, dir, "checkout", "--detach", candidate)
	gitOutput(t, dir, "update-ref", "refs/heads/fm/feature", superseding)

	database, err := db.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(dir, "https://github.com/test/repo", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "fm/feature", first, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, first); err != nil {
		t.Fatal(err)
	}
	testResult, err := database.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteStep(testResult.ID, 0, 1, "test.log"); err != nil {
		t.Fatal(err)
	}
	documentResult, err := database.InsertStepResult(run.ID, types.StepDocument)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStep(documentResult.ID); err != nil {
		t.Fatal(err)
	}
	ref, err := run.FrozenSourceRef()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginRunHeadAdvance(run.ID, ref, first, candidate, true, 3, db.HeadAdvancePipeline); err != nil {
		t.Fatal(err)
	}
	run, err = database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}

	steps := []Step{newPassStep(types.StepTest), newPassStep(types.StepDocument)}
	if recovered, err := RecoverRunHeadTransition(context.Background(), database, run, dir, steps); err == nil || recovered {
		t.Fatalf("superseding ref recovery = %v, err %v", recovered, err)
	}
	if gotRef := gitOutput(t, dir, "rev-parse", ref); gotRef != superseding {
		t.Fatalf("recovery rewound source ref to %s, want %s", gotRef, superseding)
	}
	got, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadSHA != first || got.ValidationTargetSHA != nil {
		t.Fatalf("superseding recovery claimed proof state: %#v", got)
	}
	if pending, err := database.GetRunHeadTransition(run.ID); err != nil || pending == nil {
		t.Fatalf("superseding recovery cleared transition: %#v, err %v", pending, err)
	}
}

func TestRecoverRunHeadTransitionRejectsInvalidCandidateBeforeRefMovement(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
		gitOutput(t, dir, args...)
	}
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, dir, "add", "candidate")
	gitOutput(t, dir, "commit", "-m", "first")
	first := gitOutput(t, dir, "rev-parse", "HEAD")
	gitOutput(t, dir, "checkout", "--detach")
	ref := "refs/heads/fm/feature"
	gitOutput(t, dir, "update-ref", ref, first)

	database, err := db.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(dir, "https://github.com/test/repo", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "fm/feature", first, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordSuccessfulTestHead(run.ID, first); err != nil {
		t.Fatal(err)
	}
	testResult, err := database.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteStep(testResult.ID, 0, 1, "test.log"); err != nil {
		t.Fatal(err)
	}
	documentResult, err := database.InsertStepResult(run.ID, types.StepDocument)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStep(documentResult.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginRunHeadAdvance(run.ID, ref, first, "not-a-commit", true, 3, db.HeadAdvancePipeline); err != nil {
		t.Fatal(err)
	}
	run, err = database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}

	steps := []Step{newPassStep(types.StepTest), newPassStep(types.StepDocument)}
	if recovered, err := RecoverRunHeadTransition(context.Background(), database, run, dir, steps); err == nil || recovered {
		t.Fatalf("invalid candidate recovery = %v, err %v", recovered, err)
	}
	if got := gitOutput(t, dir, "rev-parse", ref); got != first {
		t.Fatalf("invalid candidate moved source ref to %s", got)
	}
}

func TestRecoverRunHeadTransitionRestoresActiveTestRetargetWithoutRewind(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		name := "exact candidate"
		if supersede {
			name = "superseding ref"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for _, args := range [][]string{{"init"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.com"}} {
				gitOutput(t, dir, args...)
			}
			writeCandidate := func(content, message string) string {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
				gitOutput(t, dir, "add", "candidate")
				gitOutput(t, dir, "commit", "-m", message)
				return gitOutput(t, dir, "rev-parse", "HEAD")
			}
			first := writeCandidate("first\n", "first")
			candidate := writeCandidate("candidate\n", "candidate")
			superseding := writeCandidate("superseding\n", "superseding")
			gitOutput(t, dir, "checkout", "--detach", candidate)
			ref := "refs/heads/fm/feature"
			gitOutput(t, dir, "update-ref", ref, first)

			database, err := db.Open(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			repo, err := database.InsertRepo(dir, "https://github.com/test/repo", "main")
			if err != nil {
				t.Fatal(err)
			}
			run, err := database.InsertRun(repo.ID, "fm/feature", first, first)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
				t.Fatal(err)
			}
			testResult, err := database.InsertStepResult(run.ID, types.StepTest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.InsertStepResult(run.ID, types.StepDocument); err != nil {
				t.Fatal(err)
			}
			if _, err := database.ScheduleHeadValidationReplay(run.ID, 3); err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateStepStatus(testResult.ID, types.StepStatusFixing); err != nil {
				t.Fatal(err)
			}
			transition, err := database.BeginRunHeadAdvance(run.ID, ref, first, candidate, true, 3, db.HeadAdvancePipeline)
			if err != nil {
				t.Fatal(err)
			}
			if supersede {
				gitOutput(t, dir, "update-ref", ref, superseding)
			} else if err := sourceprovenance.AdvanceCandidate(context.Background(), dir, ref, candidate, first); err != nil {
				t.Fatal(err)
			}
			run, err = database.GetRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			steps := []Step{newPassStep(types.StepTest), newPassStep(types.StepDocument)}
			recovered, recoverErr := RecoverRunHeadTransition(context.Background(), database, run, dir, steps)
			if supersede {
				if recoverErr == nil || recovered {
					t.Fatalf("superseding recovery = %v, err %v", recovered, recoverErr)
				}
				if got := gitOutput(t, dir, "rev-parse", ref); got != superseding {
					t.Fatalf("recovery rewound source ref to %s", got)
				}
				if pending, err := database.GetRunHeadTransition(run.ID); err != nil || pending == nil {
					t.Fatalf("superseding recovery cleared transition: %#v, err %v", pending, err)
				}
				return
			}
			if recoverErr != nil || !recovered {
				t.Fatalf("recover Test retarget = %v, err %v", recovered, recoverErr)
			}
			if recovered, err := RecoverRunHeadTransition(context.Background(), database, run, dir, steps); err != nil || recovered {
				t.Fatalf("repeat Test retarget recovery = %v, err %v", recovered, err)
			}
			got, err := database.GetRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.HeadSHA != candidate || got.ValidationTargetSHA == nil || *got.ValidationTargetSHA != candidate ||
				got.ValidationReplayCount != 2 || got.TestHeadSHA != nil ||
				got.HeadAdvanceGeneration != transition.OwnershipGeneration {
				t.Fatalf("recovered Test retarget state = %#v", got)
			}
		})
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

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
	for _, args := range [][]string{{"add", "candidate"}, {"commit", "-m", "advanced candidate"}, {"checkout", "--detach"}, {"update-ref", "refs/heads/fm/feature", "HEAD"}} {
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
