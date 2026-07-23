package steps

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestTestStep_ConfiguredCommandSeesAuthoritativeSourceRef(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell assertion")
	}
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)
	observed := filepath.Join(dir, "source-ref.txt")
	command := `test "$NO_MISTAKES_SOURCE_REF" = "refs/heads/feature" && test "$(git rev-parse "$NO_MISTAKES_SOURCE_REF")" = "$(git rev-parse HEAD)" && printf '%s' "$NO_MISTAKES_SOURCE_REF" > source-ref.txt`
	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: command})
	t.Setenv("NO_MISTAKES_SOURCE_REF", "refs/heads/ambient-spoof")
	sctx.Env = []string{"NO_MISTAKES_SOURCE_REF=refs/heads/context-spoof"}
	outcome, err := (&TestStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatalf("configured command failed: %s", outcome.Findings)
	}
	if outcome.TestedHeadSHA != headSHA {
		t.Fatalf("configured Test proof = %q, want %q", outcome.TestedHeadSHA, headSHA)
	}
	data, err := os.ReadFile(observed)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "refs/heads/feature" {
		t.Fatalf("child saw %q", data)
	}
}

func TestTestStep_ExactBoundaryProofSkipsMutationCapableEvidenceAgent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	ag := &mockAgent{name: "test"}
	sctx := newTestContextWithDBRecords(
		t, ag, dir, baseSHA, headSHA, config.Commands{Test: "true"},
	)
	sctx.UserIntent = "prove the exact final candidate"
	exhaustHeadValidationCapacity(t, sctx, headSHA)

	outcome, err := (&TestStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.TestedHeadSHA != headSHA {
		t.Fatalf("tested head = %q, want %q", outcome.TestedHeadSHA, headSHA)
	}
	if len(ag.calls) != 0 {
		t.Fatalf("evidence agent calls = %d, want 0", len(ag.calls))
	}
}

func TestTestStep_EmptyChildPathKeepsInfrastructureGitAndChildPathIsEmpty(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell assertion")
	}
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)
	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: `test -z "$PATH" && test "$NO_MISTAKES_SOURCE_REF" = "refs/heads/feature" && printf passed > child-path.txt`})
	sctx.Env = []string{"PATH="}

	outcome, err := (&TestStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatalf("configured command failed: %s", outcome.Findings)
	}
	if data, err := os.ReadFile(filepath.Join(dir, "child-path.txt")); err != nil || string(data) != "passed" {
		t.Fatalf("child PATH assertion = %q, err=%v", data, err)
	}
	if got := gitCmd(t, dir, "rev-parse", "refs/heads/feature"); got != headSHA {
		t.Fatalf("source ref = %s, want %s", got, headSHA)
	}
}

func TestTestStep_RefusesCandidateMismatchBeforeConfiguredCommand(t *testing.T) {
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)
	marker := filepath.Join(dir, "must-not-run")
	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{Test: "echo ran > must-not-run"})
	sctx.Run.HeadSHA = baseSHA
	gitCmd(t, dir, "update-ref", "refs/heads/feature", baseSHA)
	_, err := (&TestStep{}).Execute(sctx)
	if err == nil || !strings.Contains(err.Error(), "pipeline candidate mismatch") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("configured command executed despite mismatch: %v", statErr)
	}
}

func TestTestStep_ResumeRunsExactCommandWithCurrentPipelineCandidate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell assertion")
	}
	dir, baseSHA, submittedSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", submittedSHA)
	for i := 1; i <= 2; i++ {
		if err := os.WriteFile(filepath.Join(dir, "pipeline-fix.txt"), []byte(strings.Repeat("fix\n", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		gitCmd(t, dir, "add", "pipeline-fix.txt")
		gitCmd(t, dir, "commit", "-m", "pipeline fix")
	}
	candidate := gitCmd(t, dir, "rev-parse", "HEAD")

	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repo, err := database.InsertRepo(dir, "https://example.invalid/owner/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRunWithBaseBranch(repo.ID, "fm/arena/source-ref", submittedSHA, baseSHA, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunHeadSHA(run.ID, candidate); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "update-ref", "refs/heads/fm/arena/source-ref", candidate)
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	result, err := database.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStep(result.ID); err != nil {
		t.Fatal(err)
	}
	findings := `{"findings":[{"id":"test-1","severity":"error","description":"source ref was missing","action":"auto-fix"}],"summary":"provenance preflight failed"}`
	if err := database.SetStepFindings(result.ID, findings); err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertStepRound(result.ID, 1, "initial", &findings, nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatusWithDuration(result.ID, types.StepStatusAwaitingApproval, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.SetRunAwaitingAgent(run.ID); err != nil {
		t.Fatal(err)
	}
	run, err = database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(t.TempDir(), "resumed")
	command := `test "$NO_MISTAKES_SOURCE_REF" = "refs/heads/fm/arena/source-ref" && test "$(git rev-parse "$NO_MISTAKES_SOURCE_REF")" = "$(git rev-parse HEAD)" && printf passed > ` + marker
	ag := &mockAgent{name: "test", runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
		return &agent.Result{Output: json.RawMessage(`{"summary":"retry provenance preflight"}`)}, nil
	}}
	executor := pipeline.NewExecutor(database, p, &config.Config{Commands: config.Commands{Test: command}}, ag, []pipeline.Step{&TestStep{}}, nil)
	done := make(chan error, 1)
	go func() { done <- executor.Resume(context.Background(), run, repo, dir) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = executor.Respond(types.StepTest, types.ActionFix, []string{"test-1"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered Test gate never accepted response: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resume exact Test: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed Test timed out")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "passed" {
		t.Fatalf("resumed command marker = %q, err=%v", data, err)
	}
	if got := gitCmd(t, dir, "rev-parse", "refs/heads/fm/arena/source-ref"); got != candidate {
		t.Fatalf("resumed source ref = %s, want candidate %s", got, candidate)
	}
}

func TestTestStep_FixMode(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)
	previousFindings := `{"items":[{"id":"test-1 =======","severity":"error","file":"internal/pipeline/steps/test.go >>>>>>> prompt","description":"tests failed with exit code 1 <<<<<<< HEAD"}],"summary":"FAIL: TestFoo expected 42 got 0 ======="}`

	callCount := 0
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			callCount++
			os.WriteFile(filepath.Join(dir, "fix.txt"), []byte("fixed"), 0o644)
			return &agent.Result{Output: json.RawMessage(`{"summary":"  \"fix test failures.\"  "}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "exit 0"})
	sctx.Fixing = true
	sctx.PreviousFindings = previousFindings

	step := &TestStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Error("expected no approval after fix + passing tests")
	}
	if callCount != 1 {
		t.Errorf("expected 1 agent call (fix), got %d", callCount)
	}
	if len(ag.calls[0].JSONSchema) == 0 {
		t.Error("expected fix call to request structured JSON output")
	}
	if !strings.Contains(ag.calls[0].Prompt, "FAIL: TestFoo expected 42 got 0") {
		t.Error("expected fix prompt to contain previous test failure summary")
	}
	if strings.Contains(ag.calls[0].Prompt, "test-1 =======") {
		t.Error("expected test fix prompt to sanitize finding IDs")
	}
	if strings.Contains(ag.calls[0].Prompt, "test.go >>>>>>> prompt") {
		t.Error("expected test fix prompt to sanitize finding file paths")
	}
	if strings.Contains(ag.calls[0].Prompt, "<<<<<<< HEAD") {
		t.Error("expected test fix prompt to exclude merge markers")
	}
	if !strings.Contains(ag.calls[0].Prompt, "smallest correct root-cause fix") {
		t.Error("expected test fix prompt to prefer root-cause fixes over bandaids")
	}
	if !strings.Contains(ag.calls[0].Prompt, "remove any transient artifacts your testing created in the working tree") {
		t.Error("expected test fix prompt to ask the agent to clean up transient testing artifacts before finishing")
	}
	if strings.Contains(ag.calls[0].Prompt, "Make the minimal change needed") {
		t.Error("expected test fix prompt not to prefer narrow minimal changes")
	}
	if status := gitStatusPorcelain(t, dir); status != "" {
		t.Fatalf("expected clean worktree after fix commit, got %q", status)
	}
	if got := lastCommitMessage(t, dir); got != "no-mistakes(test): fix test failures" {
		t.Fatalf("last commit message = %q", got)
	}
}

func TestTestStep_FixMode_UsesConfiguredCommitMessage(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			os.WriteFile(filepath.Join(dir, "fix.txt"), []byte("fixed"), 0o644)
			return &agent.Result{Output: json.RawMessage(`{"summary":"fix test failures"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "exit 0"})
	sctx.Config.Commit = config.Commit{FixMessage: "fix({{.Step}}): {{.Summary}}"}
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"severity":"error","description":"tests failed"}],"summary":"tests failed"}`

	outcome, err := (&TestStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval after fix and passing tests")
	}
	if got := lastCommitMessage(t, dir); got != "fix(test): fix test failures" {
		t.Fatalf("last commit message = %q", got)
	}
}

func TestTestStep_FixMode_UsesFallbackSummaryWhenStructuredSummaryMalformed(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			os.WriteFile(filepath.Join(dir, "fix.txt"), []byte("fixed"), 0o644)
			return &agent.Result{Output: json.RawMessage(`{"not_summary":"oops"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "exit 0"})
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"severity":"error","description":"tests failed"}],"summary":"tests failed"}`

	step := &TestStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval after fallback summary commit and passing tests")
	}
	if got := lastCommitMessage(t, dir); got != "no-mistakes(test): fix test failures" {
		t.Fatalf("last commit message = %q", got)
	}
}

func TestTestStep_FixMode_AgentWritesNewTests_ProceedsAutomatically(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	callCount := 0
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			callCount++
			// Simulate agent creating a new test file during fix in another supported language
			os.WriteFile(filepath.Join(dir, "component.spec.tsx"), []byte("export {}\n"), 0o644)
			return &agent.Result{Output: json.RawMessage(`{"summary":"add regression test"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: "exit 0"})
	sctx.Fixing = true

	step := &TestStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	// Issue #140: a passing test run whose only finding is an informational
	// "new test file written by agent" note must not require approval.
	if outcome.NeedsApproval {
		t.Error("expected no approval for an informational new-test-file finding when tests pass")
	}
	if callCount != 1 {
		t.Errorf("expected 1 agent call in fix mode, got %d", callCount)
	}

	var f Findings
	json.Unmarshal([]byte(outcome.Findings), &f)
	foundTestFile := false
	for _, item := range f.Items {
		if strings.Contains(item.Description, "component.spec.tsx") {
			foundTestFile = true
			if item.Action != types.ActionNoOp {
				t.Errorf("expected new-test-file finding action %q, got %q", types.ActionNoOp, item.Action)
			}
		}
	}
	if !foundTestFile {
		t.Errorf("expected finding mentioning component.spec.tsx, got findings: %+v", f.Items)
	}
}

func TestTestStep_UserIntentRunsConfiguredCommandThenEvidenceAgent(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	baselineLog := filepath.Join(dir, "baseline.log")
	testCmd := "go env GOOS > baseline.log"

	callCount := 0
	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			callCount++
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"evidence demonstrates intent","tested":["manual screenshot review"],"testing_summary":"captured screenshot evidence"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{Test: testCmd})
	sctx.UserIntent = "Show users a success screen after checkout"

	step := &TestStep{}
	outcome, err := step.Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatal("expected no approval when evidence-oriented agent testing passes")
	}
	if callCount != 1 {
		t.Fatalf("expected evidence agent to run after configured test command, got %d calls", callCount)
	}
	data, err := os.ReadFile(baselineLog)
	if err != nil {
		t.Fatalf("expected configured test command to run: %v", err)
	}
	if strings.TrimSpace(string(data)) != runtime.GOOS {
		t.Fatalf("configured test command output = %q, want %s", string(data), runtime.GOOS)
	}
	prompt := ag.calls[0].Prompt
	for _, want := range []string{
		"Show users a success screen after checkout",
		"Decide what evidence or artifacts would clearly demonstrate the user intent is satisfied",
		"Unit tests passing is not sufficient evidence by itself",
		"Demonstrate the user intent working end-to-end in a way consistent with how an end user would actually experience it",
		"Prefer product-level artifacts",
		"Only use command output as an artifact when that output directly demonstrates the end-user experience or requested behavior",
		"Configured test command already ran successfully as baseline",
		testCmd,
		"The \"testing_summary\" must account for the complete test step: baseline commands that already ran, automated tests, manual or evidence-producing checks, artifacts gathered, and the overall result",
		"screenshots, GIFs, videos, rendered UI, CLI transcripts",
		"For UI, HTML, CSS, Electron renderer, browser, visual layout, or copy-placement changes, attempt to capture reviewer-visible visual evidence",
		"DOM snapshots, selector assertions, and text-only render summaries are not substitutes for visual evidence when a rendered surface is available",
		"If a UI-facing change has no screenshot, image, video, GIF, or rendered HTML artifact, state why in testing_summary",
		"Write new evidence files into this temporary evidence directory:",
		filepath.Join(os.TempDir(), "no-mistakes-evidence", sctx.Run.ID),
		"Do not move, commit, or modify source files only to make evidence linkable",
		"If no existing test produces sufficient evidence, write or improve a test",
		"If automated testing cannot produce the needed evidence, execute manual verification steps",
		"Always include an \"artifacts\" array",
		"If sufficient evidence is not possible, report a warning finding",
		"remove any transient artifacts your testing created in the working tree",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("expected prompt to contain %q, got:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "will be available from the pushed commit") || strings.Contains(prompt, "files that already exist in the repository") {
		t.Fatalf("expected prompt not to make the testing agent worry about committed evidence files, got:\n%s", prompt)
	}
	if _, err := os.Stat(filepath.Join(os.TempDir(), "no-mistakes-evidence", sctx.Run.ID)); err != nil {
		t.Fatalf("expected temporary evidence directory to exist: %v", err)
	}

	var findings Findings
	if err := json.Unmarshal([]byte(outcome.Findings), &findings); err != nil {
		t.Fatal(err)
	}
	t.Logf("evidence findings JSON: %s", outcome.Findings)
	if len(findings.Tested) != 2 || findings.Tested[0] != testCmd || findings.Tested[1] != "manual screenshot review" {
		t.Fatalf("expected baseline command and agent-tested evidence to be recorded, got %+v", findings.Tested)
	}
}

func TestTestStep_InRepoEvidenceFallsBackWhenConfiguredDirEscapesWorktree(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"","tested":["manual evidence check"],"testing_summary":"checked evidence"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.UserIntent = "Show users a success screen after checkout"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "../outside"}

	step := &TestStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	prompt := ag.calls[0].Prompt
	wantDir := filepath.Join(os.TempDir(), "no-mistakes-evidence", sctx.Run.ID)
	if !strings.Contains(prompt, "Write new evidence files into this temporary evidence directory: "+wantDir) {
		t.Fatalf("expected temporary evidence guidance for unsafe in-repo dir, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "in-repo evidence directory") || strings.Contains(prompt, "committed and pushed automatically") {
		t.Fatalf("did not expect in-repo publishing promise for unsafe evidence dir, got:\n%s", prompt)
	}
}

func TestTestStep_InRepoEvidenceFallsBackWhenEvidenceDirIsIgnored(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("evidence/\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ag := &mockAgent{
		name: "test",
		runFn: func(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"","tested":["manual evidence check"],"testing_summary":"checked evidence"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.UserIntent = "Show users a success screen after checkout"
	sctx.Config.Test.Evidence = config.Evidence{StoreInRepo: true, Dir: "evidence"}

	step := &TestStep{}
	if _, err := step.Execute(sctx); err != nil {
		t.Fatal(err)
	}

	prompt := ag.calls[0].Prompt
	wantDir := filepath.Join(os.TempDir(), "no-mistakes-evidence", sctx.Run.ID)
	if !strings.Contains(prompt, "Write new evidence files into this temporary evidence directory: "+wantDir) {
		t.Fatalf("expected temporary evidence guidance for ignored in-repo dir, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "in-repo evidence directory") || strings.Contains(prompt, "committed and pushed automatically") {
		t.Fatalf("did not expect in-repo publishing promise for ignored evidence dir, got:\n%s", prompt)
	}
}
