package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestExecutor_ContextCancellation(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := t.TempDir()

	// Step that blocks until context is cancelled
	step := &adaptiveCallStep{
		name: types.StepReview,
		fn: func(sctx *StepContext) (*StepOutcome, error) {
			<-sctx.Ctx.Done()
			return nil, sctx.Ctx.Err()
		},
	}

	exec := NewExecutor(database, p, nil, nil, []Step{step}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- exec.Execute(ctx, run, repo, workDir)
	}()

	// Give executor time to start
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from cancellation, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor timed out")
	}

	updated, _ := database.GetRun(run.ID)
	if updated.Status != types.RunCancelled {
		t.Errorf("expected run status %q, got %q", types.RunCancelled, updated.Status)
	}
	if updated.Error == nil || *updated.Error != context.Canceled.Error() {
		t.Errorf("expected ordinary cancellation error, got %v", updated.Error)
	}
}

func TestExecutor_ContextCancelCause(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := t.TempDir()

	// Two steps: first passes, second blocks until context is cancelled.
	// This tests that the cause propagates even when detected between steps.
	step1 := newPassStep(types.StepReview)
	step2 := &adaptiveCallStep{
		name: types.StepTest,
		fn: func(sctx *StepContext) (*StepOutcome, error) {
			<-sctx.Ctx.Done()
			return nil, context.Cause(sctx.Ctx)
		},
	}

	exec := NewExecutor(database, p, nil, nil, []Step{step1, step2}, nil)

	cancelReason := fmt.Errorf("cancelled: superseded by new push")
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- exec.Execute(ctx, run, repo, workDir)
	}()

	// Give executor time to start step2
	time.Sleep(50 * time.Millisecond)
	cancel(cancelReason)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from cancellation, got nil")
		}
		if !strings.Contains(err.Error(), "superseded by new push") {
			t.Errorf("expected error to contain 'superseded by new push', got %q", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor timed out")
	}

	updated, _ := database.GetRun(run.ID)
	if updated.Status != types.RunCancelled {
		t.Errorf("expected run status %q, got %q", types.RunCancelled, updated.Status)
	}
	if updated.Error == nil || !strings.Contains(*updated.Error, "superseded by new push") {
		var got string
		if updated.Error != nil {
			got = *updated.Error
		}
		t.Errorf("expected run error to contain 'superseded by new push', got %q", got)
	}
}

func TestExecutor_ContextCancelCauseBetweenSteps(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := t.TempDir()

	// First step passes and signals, cancel fires before second step starts.
	started := make(chan struct{})
	step1 := &adaptiveCallStep{
		name: types.StepReview,
		fn: func(sctx *StepContext) (*StepOutcome, error) {
			close(started)
			// Small delay so cancel can fire before step2 starts
			time.Sleep(50 * time.Millisecond)
			return &StepOutcome{ExitCode: 0}, nil
		},
	}
	step2 := newPassStep(types.StepTest)

	exec := NewExecutor(database, p, nil, nil, []Step{step1, step2}, nil)

	cancelReason := fmt.Errorf("cancelled: superseded by new push")
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- exec.Execute(ctx, run, repo, workDir)
	}()

	<-started
	cancel(cancelReason)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from cancellation, got nil")
		}
		if !strings.Contains(err.Error(), "superseded by new push") {
			t.Errorf("expected error to contain 'superseded by new push', got %q", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor timed out")
	}

	updated, _ := database.GetRun(run.ID)
	if updated.Status != types.RunCancelled {
		t.Errorf("expected run status %q, got %q", types.RunCancelled, updated.Status)
	}
	if updated.Error == nil || !strings.Contains(*updated.Error, "superseded by new push") {
		var got string
		if updated.Error != nil {
			got = *updated.Error
		}
		t.Errorf("expected run error to contain 'superseded by new push', got %q", got)
	}
}

func TestExecutor_SourceRefSupersessionIsCancelled(t *testing.T) {
	database, p, run, repo := setupTest(t)
	step := &adaptiveCallStep{
		name: types.StepPush,
		fn: func(*StepContext) (*StepOutcome, error) {
			return nil, fmt.Errorf("delivery ownership: %w", ErrSourceRefSuperseded)
		},
	}
	exec := NewExecutor(database, p, nil, nil, []Step{step}, nil)
	if err := exec.Execute(context.Background(), run, repo, t.TempDir()); !errors.Is(err, ErrSourceRefSuperseded) {
		t.Fatalf("Execute() error = %v, want source supersession", err)
	}
	updated, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != types.RunCancelled || updated.Error == nil || *updated.Error != types.RunCancelReasonSuperseded {
		t.Fatalf("superseded run = %#v", updated)
	}
}

func TestExecutor_IndependentFailureWinsCancellationRace(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := t.TempDir()

	started := make(chan struct{})
	release := make(chan struct{})
	stepErr := fmt.Errorf("independent test failure")
	step := &adaptiveCallStep{
		name: types.StepTest,
		fn: func(sctx *StepContext) (*StepOutcome, error) {
			close(started)
			<-release
			return nil, stepErr
		},
	}
	exec := NewExecutor(database, p, nil, nil, []Step{step}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- exec.Execute(ctx, run, repo, workDir)
	}()

	<-started
	cancel()
	close(release)

	err := <-done
	if err == nil || !strings.Contains(err.Error(), stepErr.Error()) {
		t.Fatalf("expected independent failure, got %v", err)
	}
	updated, getErr := database.GetRun(run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if updated.Status != types.RunFailed {
		t.Fatalf("expected run status %q, got %q", types.RunFailed, updated.Status)
	}
	if updated.Error == nil || *updated.Error != "step test failed: "+stepErr.Error() {
		t.Fatalf("expected independent run error, got %v", updated.Error)
	}
}

func TestExecutor_DeadlineRemainsFailure(t *testing.T) {
	database, p, run, repo := setupTest(t)
	workDir := t.TempDir()

	step := &adaptiveCallStep{
		name: types.StepTest,
		fn: func(sctx *StepContext) (*StepOutcome, error) {
			<-sctx.Ctx.Done()
			return nil, sctx.Ctx.Err()
		},
	}
	exec := NewExecutor(database, p, nil, nil, []Step{step}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := exec.Execute(ctx, run, repo, workDir); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	updated, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != types.RunFailed {
		t.Fatalf("expected run status %q, got %q", types.RunFailed, updated.Status)
	}
	if updated.Error == nil || *updated.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("expected deadline run error, got %v", updated.Error)
	}
}
