package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestListWorktreesAndAdministration(t *testing.T) {
	ctx := context.Background()
	src := initTestRepo(t)
	bare := filepath.Join(t.TempDir(), "gate.git")
	if err := InitBare(ctx, bare); err != nil {
		t.Fatal(err)
	}
	run(t, src, "git", "remote", "add", "gate-topology", bare)
	run(t, src, "git", "push", "gate-topology", "HEAD:refs/heads/main")
	sha := run(t, src, "git", "rev-parse", "HEAD")
	worktree := filepath.Join(t.TempDir(), "run-id")
	if err := WorktreeAdd(ctx, bare, worktree, sha); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = WorktreeRemove(ctx, bare, worktree) })

	listed, err := ListWorktrees(ctx, bare)
	if err != nil {
		t.Fatal(err)
	}
	resolvedWorktree, err := filepath.EvalSymlinks(worktree)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || !listed[0].Bare || listed[1].Path != resolvedWorktree || listed[1].HEAD != sha || !listed[1].Detached || listed[1].Prunable {
		t.Fatalf("worktree list = %#v", listed)
	}
	if got := run(t, worktree, "git", "rev-parse", "HEAD^{tree}"); got != run(t, bare, "git", "rev-parse", sha+"^{tree}") {
		t.Fatalf("detached worktree tree = %s", got)
	}
	if got := run(t, worktree, "git", "status", "--porcelain=v1"); got != "" {
		t.Fatalf("detached worktree is dirty: %q", got)
	}
	if got := run(t, bare, "git", "rev-parse", "refs/heads/main"); got != sha {
		t.Fatalf("detached add moved source ref to %s", got)
	}
	admin, err := ListWorktreeAdminEntries(bare)
	if err != nil {
		t.Fatal(err)
	}
	if len(admin) != 1 || admin[0].WorktreePath != resolvedWorktree {
		t.Fatalf("worktree admin = %#v", admin)
	}
	linkedAdmin, err := LinkedWorktreeAdminDir(worktree)
	if err != nil {
		t.Fatal(err)
	}
	resolvedAdmin, err := filepath.EvalSymlinks(admin[0].Dir)
	if err != nil {
		t.Fatal(err)
	}
	if linkedAdmin != resolvedAdmin {
		t.Fatalf("linked admin = %q, want %q", linkedAdmin, resolvedAdmin)
	}
}

func TestListWorktreesReportsPrunableRegistration(t *testing.T) {
	ctx := context.Background()
	src := initTestRepo(t)
	bare := filepath.Join(t.TempDir(), "gate.git")
	if err := InitBare(ctx, bare); err != nil {
		t.Fatal(err)
	}
	run(t, src, "git", "remote", "add", "gate-prunable", bare)
	run(t, src, "git", "push", "gate-prunable", "HEAD:refs/heads/main")
	sha := run(t, src, "git", "rev-parse", "HEAD")
	worktree := filepath.Join(t.TempDir(), "run-id")
	if err := WorktreeAdd(ctx, bare, worktree, sha); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(worktree); err != nil {
		t.Fatal(err)
	}
	listed, err := ListWorktrees(ctx, bare)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || !listed[1].Prunable {
		t.Fatalf("worktree list = %#v, want prunable linked registration", listed)
	}
}

func TestListWorktreeAdminEntriesRefusesSymlinkedRoot(t *testing.T) {
	bare := filepath.Join(t.TempDir(), "gate.git")
	if err := InitBare(context.Background(), bare); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(bare, "worktrees")); err != nil {
		t.Fatal(err)
	}
	if _, err := ListWorktreeAdminEntries(bare); err == nil {
		t.Fatal("symlinked worktree administration root was accepted")
	}
}

func TestListWorktreeAdminEntriesRefusesSymlink(t *testing.T) {
	bare := filepath.Join(t.TempDir(), "gate.git")
	if err := InitBare(context.Background(), bare); err != nil {
		t.Fatal(err)
	}
	adminRoot := filepath.Join(bare, "worktrees")
	if err := os.MkdirAll(adminRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(adminRoot, "run-id")); err != nil {
		t.Fatal(err)
	}
	if _, err := ListWorktreeAdminEntries(bare); err == nil {
		t.Fatal("symlinked worktree administration was accepted")
	}
}
