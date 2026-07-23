package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WorktreeInfo is one record from git worktree list --porcelain. Paths and
// status are returned exactly enough for recovery code to reject stale,
// prunable, moved, or duplicate registrations without invoking a global prune.
type WorktreeInfo struct {
	Path        string
	HEAD        string
	Branch      string
	Bare        bool
	Detached    bool
	Locked      bool
	LockReason  string
	Prunable    bool
	PruneReason string
}

// ListWorktrees returns every worktree registered in repoDir. The NUL-delimited
// porcelain form keeps paths with whitespace or newlines unambiguous.
func ListWorktrees(ctx context.Context, repoDir string) ([]WorktreeInfo, error) {
	out, err := Run(ctx, repoDir, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return nil, err
	}
	var worktrees []WorktreeInfo
	var current *WorktreeInfo
	flush := func() {
		if current != nil {
			worktrees = append(worktrees, *current)
			current = nil
		}
	}
	for _, field := range strings.Split(out, "\x00") {
		if field == "" {
			flush()
			continue
		}
		key, value, _ := strings.Cut(field, " ")
		switch key {
		case "worktree":
			flush()
			current = &WorktreeInfo{Path: value}
		case "HEAD":
			if current != nil {
				current.HEAD = value
			}
		case "branch":
			if current != nil {
				current.Branch = value
			}
		case "bare":
			if current != nil {
				current.Bare = true
			}
		case "detached":
			if current != nil {
				current.Detached = true
			}
		case "locked":
			if current != nil {
				current.Locked = true
				current.LockReason = value
			}
		case "prunable":
			if current != nil {
				current.Prunable = true
				current.PruneReason = value
			}
		}
	}
	flush()
	return worktrees, nil
}

// WorktreeAdminEntry is one linked-worktree administration directory under a
// common Git directory. GitDir is the linked checkout's .git file path and
// WorktreePath is its parent directory.
type WorktreeAdminEntry struct {
	Name         string
	Dir          string
	GitDir       string
	WorktreePath string
}

// ListWorktreeAdminEntries returns all linked-worktree admin entries. It fails
// closed on symlinks, non-directories, missing gitdir files, or relative gitdir
// values so callers never mistake ambiguous administration for an absent copy.
func ListWorktreeAdminEntries(commonDir string) ([]WorktreeAdminEntry, error) {
	adminRoot := filepath.Join(commonDir, "worktrees")
	rootInfo, err := os.Lstat(adminRoot)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect worktree administration root: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("worktree administration root is not a real directory")
	}
	entries, err := os.ReadDir(adminRoot)
	if err != nil {
		return nil, fmt.Errorf("read worktree administration: %w", err)
	}
	out := make([]WorktreeAdminEntry, 0, len(entries))
	for _, entry := range entries {
		dir := filepath.Join(adminRoot, entry.Name())
		info, err := os.Lstat(dir)
		if err != nil {
			return nil, fmt.Errorf("inspect worktree administration %q: %w", entry.Name(), err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("worktree administration %q is not a real directory", entry.Name())
		}
		gitDirPath := filepath.Join(dir, "gitdir")
		gitDirInfo, err := os.Lstat(gitDirPath)
		if err != nil {
			return nil, fmt.Errorf("inspect worktree administration %q gitdir: %w", entry.Name(), err)
		}
		if !gitDirInfo.Mode().IsRegular() || gitDirInfo.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("worktree administration %q gitdir is not a regular file", entry.Name())
		}
		data, err := os.ReadFile(gitDirPath)
		if err != nil {
			return nil, fmt.Errorf("read worktree administration %q gitdir: %w", entry.Name(), err)
		}
		gitDir := strings.TrimSpace(string(data))
		if !filepath.IsAbs(gitDir) {
			return nil, fmt.Errorf("worktree administration %q has a relative gitdir", entry.Name())
		}
		gitDir = filepath.Clean(gitDir)
		out = append(out, WorktreeAdminEntry{
			Name: entry.Name(), Dir: dir, GitDir: gitDir, WorktreePath: filepath.Dir(gitDir),
		})
	}
	return out, nil
}

// LinkedWorktreeAdminDir resolves the common-repository administration path
// named by a linked worktree's .git file.
func LinkedWorktreeAdminDir(worktreePath string) (string, error) {
	info, err := os.Lstat(filepath.Join(worktreePath, ".git"))
	if err != nil {
		return "", fmt.Errorf("inspect linked worktree git file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("linked worktree .git is not a regular file")
	}
	data, err := os.ReadFile(filepath.Join(worktreePath, ".git"))
	if err != nil {
		return "", fmt.Errorf("read linked worktree git file: %w", err)
	}
	line := strings.TrimSpace(string(data))
	const prefix = "gitdir: "
	if !strings.HasPrefix(line, prefix) {
		return "", fmt.Errorf("linked worktree .git has invalid contents")
	}
	adminDir := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	if !filepath.IsAbs(adminDir) {
		return "", fmt.Errorf("linked worktree .git names a relative gitdir")
	}
	return filepath.Clean(adminDir), nil
}
