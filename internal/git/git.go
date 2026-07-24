package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/safeurl"
	"github.com/kunchenguid/no-mistakes/internal/shellenv"
	"github.com/kunchenguid/no-mistakes/internal/winproc"
)

var trustedGitExecutable, trustedGitExecutableErr = resolveTrustedGitExecutable()

func resolveTrustedGitExecutable() (string, error) {
	path, err := exec.LookPath("git")
	if err != nil {
		return "", fmt.Errorf("trusted git executable: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve trusted git executable %q: %w", path, err)
	}
	return abs, nil
}

// EmptyTreeSHA is the well-known SHA of an empty tree in git.
// Used as a base when there is no prior commit to diff against.
const EmptyTreeSHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// IsZeroSHA returns true if the SHA is the null/zero ref that git uses for
// new or deleted branches (40 zeros).
func IsZeroSHA(sha string) bool {
	return sha == "0000000000000000000000000000000000000000"
}

// Run executes a git command in the given directory and returns trimmed stdout.
// Returns an error that includes the command and stderr on failure.
//
// When dir is itself a bare repository (a gate repo), the repo is named
// explicitly via --git-dir instead of relying on cwd-based discovery, which
// safe.bareRepository=explicit forbids. Agent harnesses (e.g. Claude Code)
// and hardened CI inject that setting, so gate operations must never depend
// on discovering a bare repo from the working directory (issue #362).
func Run(ctx context.Context, dir string, args ...string) (string, error) {
	if trustedGitExecutableErr != nil {
		return "", trustedGitExecutableErr
	}
	if isBareGitDir(dir) {
		args = append([]string{"--git-dir=" + dir}, args...)
	}
	cmd := exec.CommandContext(ctx, trustedGitExecutable, args...)
	cmd.Dir = dir
	cmd.Env = NonInteractiveEnv(dir)
	winproc.Harden(cmd)
	// exec.Cmd reports a context-driven SIGKILL as an ExitError. Record only a
	// successful CommandContext leader kill so callers can distinguish it from
	// an unrelated Git failure that merely raced with cancellation.
	var leaderKillApplied atomic.Bool
	cancelCommand := cmd.Cancel
	cmd.Cancel = func() error {
		err := cancelCommand()
		if err == nil {
			leaderKillApplied.Store(true)
		}
		return err
	}
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(ee.Stderr))
		}
		if shellenv.CommandLeaderCanceled(cmd, err, leaderKillApplied.Load()) {
			if cause := context.Cause(ctx); cause != nil {
				return "", fmt.Errorf("git %s: %w: %s", safeurl.RedactText(strings.Join(args, " ")), cause, safeurl.RedactText(stderr))
			}
		}
		return "", fmt.Errorf("git %s: %w: %s", safeurl.RedactText(strings.Join(args, " ")), err, safeurl.RedactText(stderr))
	}
	return strings.TrimSpace(string(out)), nil
}

func WithPreparedRefLock(ctx context.Context, dir, ref, expectedOID string, fn func() error) error {
	if trustedGitExecutableErr != nil {
		return trustedGitExecutableErr
	}
	ref = strings.TrimSpace(ref)
	expectedOID = strings.TrimSpace(expectedOID)
	if ref == "" || expectedOID == "" || fn == nil {
		return fmt.Errorf("prepared ref lock requires a ref, expected object, and operation")
	}
	args := []string{"update-ref", "--stdin"}
	if isBareGitDir(dir) {
		args = append([]string{"--git-dir=" + dir}, args...)
	}
	cmd := exec.CommandContext(ctx, trustedGitExecutable, args...)
	cmd.Dir = dir
	cmd.Env = NonInteractiveEnv(dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("prepare ref lock stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("prepare ref lock stdout: %w", err)
	}
	shellenv.ConfigureShellCommand(cmd)
	if err := shellenv.StartShellCommand(cmd); err != nil {
		return fmt.Errorf("start prepared ref lock: %w", err)
	}
	defer shellenv.TerminateShellCommandGroup(cmd)
	reader := bufio.NewReader(stdout)
	write := func(value string) error {
		if _, err := io.WriteString(stdin, value); err != nil {
			return err
		}
		return nil
	}
	expect := func(want string) error {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if strings.TrimSpace(line) != want {
			return fmt.Errorf("unexpected update-ref response %q", strings.TrimSpace(line))
		}
		return nil
	}
	finish := func(command, response string) error {
		if err := write(command + "\n"); err != nil {
			return err
		}
		if err := expect(response); err != nil {
			return err
		}
		if err := stdin.Close(); err != nil {
			return err
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return nil
	}
	abort := true
	defer func() {
		if abort {
			_ = stdin.Close()
			_ = cmd.Wait()
		}
	}()
	if err := write("start\n"); err != nil {
		return fmt.Errorf("start ref transaction: %w", err)
	}
	if err := expect("start: ok"); err != nil {
		return fmt.Errorf("start ref transaction: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if err := write(fmt.Sprintf("update %s %s %s\nprepare\n", ref, expectedOID, expectedOID)); err != nil {
		return fmt.Errorf("prepare ref transaction: %w", err)
	}
	if err := expect("prepare: ok"); err != nil {
		return fmt.Errorf("prepare ref transaction: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if err := fn(); err != nil {
		if abortErr := finish("abort", "abort: ok"); abortErr != nil {
			return fmt.Errorf("%w; release prepared ref lock: %v", err, abortErr)
		}
		abort = false
		return err
	}
	if err := finish("commit", "commit: ok"); err != nil {
		return fmt.Errorf("commit prepared ref lock: %w", err)
	}
	abort = false
	return nil
}

// isBareGitDir reports whether dir is itself a git directory (a bare repo),
// as opposed to a working tree or linked worktree, which carry a .git entry
// and keep using normal discovery. The check mirrors git's own git-dir
// heuristic: a HEAD file plus an objects directory.
func isBareGitDir(dir string) bool {
	if dir == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		return false
	}
	if fi, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil || fi.IsDir() {
		return false
	}
	fi, err := os.Stat(filepath.Join(dir, "objects"))
	return err == nil && fi.IsDir()
}

// InitBare creates a new bare git repository at the given path.
func InitBare(ctx context.Context, path string) error {
	cmd := exec.CommandContext(ctx, "git", "init", "--bare", path)
	winproc.Harden(cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git init --bare: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// AddRemote adds a named remote to the repo at dir.
func AddRemote(ctx context.Context, dir, name, url string) error {
	_, err := Run(ctx, dir, "remote", "add", name, url)
	return err
}

// EnsureRemote sets the named remote to url, adding it when absent and
// updating its URL when it already exists. Idempotent, so it is safe to call
// when repairing or re-running an init.
func EnsureRemote(ctx context.Context, dir, name, url string) error {
	if _, err := GetRemoteURL(ctx, dir, name); err == nil {
		_, err := Run(ctx, dir, "remote", "set-url", name, url)
		return err
	}
	return AddRemote(ctx, dir, name, url)
}

// RemoveRemote removes a named remote from the repo at dir.
func RemoveRemote(ctx context.Context, dir, name string) error {
	_, err := Run(ctx, dir, "remote", "remove", name)
	return err
}

// GetRemoteURL returns the URL of a named remote.
func GetRemoteURL(ctx context.Context, dir, name string) (string, error) {
	return Run(ctx, dir, "remote", "get-url", name)
}

// GetConfiguredRemoteURL returns the literal remote URL from git config,
// without applying url.*.insteadOf rewrites.
func GetConfiguredRemoteURL(ctx context.Context, dir, name string) (string, error) {
	return Run(ctx, dir, "config", "--get", "remote."+name+".url")
}

// HasRemote reports whether a remote named name is configured in the repo at
// dir, returning an error if the remote list cannot be read.
func HasRemote(ctx context.Context, dir, name string) (bool, error) {
	out, err := Run(ctx, dir, "remote")
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == name {
			return true, nil
		}
	}
	return false, nil
}

// FindGitRoot walks up from path to find the git repository root.
// Resolves symlinks for consistency on macOS (e.g. /tmp -> /private/tmp).
func FindGitRoot(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Dir = abs
	winproc.Harden(cmd)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("not a git repository: %s", abs)
	}
	root := strings.TrimSpace(string(out))
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return root, nil
	}
	return resolved, nil
}

// FindMainRepoRoot returns the root of the main working tree for a git
// repository. Three layouts are supported:
//
//  1. A regular repository or a linked worktree: the git common dir is
//     <root>/.git, so the main working tree is filepath.Dir(commonDir).
//  2. An absorbed submodule (including nested .../modules/a/modules/b):
//     the git common dir lives under the superproject's .git/modules/...
//     and is detached from its working tree. Git writes core.worktree
//     when it absorbs a submodule, pointing at the working tree whose
//     remote.origin.url is the submodule's own origin (which is what
//     callers like init and eject need).
//  3. Exotic GIT_DIR layouts without a core.worktree: fall back to
//     `git rev-parse --show-toplevel` from the original path, the same
//     answer FindGitRoot returns.
//
// In every branch the returned path is run through filepath.EvalSymlinks
// when possible so callers can compare it against other symlink-resolved
// paths (notably on macOS, where /tmp and /private/tmp refer to the same
// directory). Symlink resolution failures fall back to the unresolved
// path, matching the historical behavior.
func FindMainRepoRoot(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	// Resolve the git common dir.
	commonDirCmd := exec.Command("git", "rev-parse", "--git-common-dir")
	commonDirCmd.Dir = abs
	winproc.Harden(commonDirCmd)
	commonDirOut, err := commonDirCmd.Output()
	if err != nil {
		return "", fmt.Errorf("not a git repository: %s", abs)
	}
	commonDir := strings.TrimSpace(string(commonDirOut))
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(abs, commonDir)
	}

	// Branch 1: regular repo or linked worktree. Linked worktrees share
	// the main repo's <root>/.git, so the common dir's basename is still
	// ".git" and its parent is the main working tree.
	if filepath.Base(commonDir) == ".git" {
		return resolveMainRoot(filepath.Dir(commonDir))
	}

	// Branch 2: detached git dir (absorbed submodule). Ask the git dir
	// itself for its core.worktree, which git writes when it absorbs a
	// submodule's git dir. The value is typically relative (e.g.
	// "../../../sub"); resolve it against the common dir.
	worktreeCmd := exec.Command("git", "--git-dir", commonDir, "config", "--get", "core.worktree")
	winproc.Harden(worktreeCmd)
	if worktreeOut, err := worktreeCmd.Output(); err == nil {
		worktree := strings.TrimSpace(string(worktreeOut))
		if worktree != "" {
			if !filepath.IsAbs(worktree) {
				worktree = filepath.Join(commonDir, worktree)
			}
			return resolveMainRoot(worktree)
		}
	}

	// Branch 3: exotic GIT_DIR without a usable core.worktree. Defer to
	// `git rev-parse --show-toplevel` from the original path.
	topCmd := exec.Command("git", "rev-parse", "--show-toplevel")
	topCmd.Dir = abs
	winproc.Harden(topCmd)
	topOut, err := topCmd.Output()
	if err != nil {
		return "", fmt.Errorf("not a git repository: %s", abs)
	}
	return resolveMainRoot(strings.TrimSpace(string(topOut)))
}

// resolveMainRoot applies filepath.EvalSymlinks to path, falling back to
// the unresolved path when symlink resolution fails.
func resolveMainRoot(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return path, nil
	}
	return resolved, nil
}

// Diff returns the unified diff between two commits.
func Diff(ctx context.Context, dir, base, head string) (string, error) {
	return Run(ctx, dir, "diff", base+".."+head)
}

// DiffNameOnly returns the list of files changed between base and head.
// Output is split on newlines with empty entries removed.
func DiffNameOnly(ctx context.Context, dir, base, head string) ([]string, error) {
	out, err := Run(ctx, dir, "diff", "--name-only", base+".."+head)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, line := range strings.Split(out, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			files = append(files, trimmed)
		}
	}
	return files, nil
}

// DiffStat returns the bounded size of the diff between base and head: the
// number of changed files and the net changed lines (insertions + deletions)
// from `git diff --numstat`. Binary files (numstat "-") contribute a changed
// file but no line count. It carries no paths or content - just two counts.
func DiffStat(ctx context.Context, dir, base, head string) (files, lines int, err error) {
	out, err := Run(ctx, dir, "diff", "--numstat", base+".."+head)
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		files++
		added, aerr := strconv.Atoi(fields[0])
		deleted, derr := strconv.Atoi(fields[1])
		if aerr == nil {
			lines += added
		}
		if derr == nil {
			lines += deleted
		}
	}
	return files, lines, nil
}

// CommitTime returns the committer timestamp for a SHA in UTC.
func CommitTime(ctx context.Context, dir, sha string) (time.Time, error) {
	out, err := Run(ctx, dir, "show", "-s", "--format=%ct", sha)
	if err != nil {
		return time.Time{}, err
	}
	secs, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse commit time %q: %w", out, err)
	}
	return time.Unix(secs, 0).UTC(), nil
}

// CommitAuthorEmail returns the author email for a SHA.
func CommitAuthorEmail(ctx context.Context, dir, sha string) (string, error) {
	return Run(ctx, dir, "show", "-s", "--format=%ae", sha)
}

// DiffHead returns the unified diff between HEAD and the working tree
// (both staged and unstaged changes).
func DiffHead(ctx context.Context, dir string) (string, error) {
	return Run(ctx, dir, "diff", "HEAD")
}

// Log returns oneline log entries between two commits.
func Log(ctx context.Context, dir, base, head string) (string, error) {
	return Run(ctx, dir, "log", "--oneline", base+".."+head)
}

// HeadSHA returns the full SHA of HEAD.
func HeadSHA(ctx context.Context, dir string) (string, error) {
	return Run(ctx, dir, "rev-parse", "HEAD")
}

// CurrentBranch returns the current branch name.
func CurrentBranch(ctx context.Context, dir string) (string, error) {
	return Run(ctx, dir, "rev-parse", "--abbrev-ref", "HEAD")
}

// IsDetachedHEAD reports whether the working tree is in a detached-HEAD state
// (HEAD points at a commit rather than a branch ref). Uses `git symbolic-ref`
// which fails cleanly when HEAD is not a symbolic ref.
func IsDetachedHEAD(ctx context.Context, dir string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "symbolic-ref", "-q", "HEAD")
	cmd.Dir = dir
	winproc.Harden(cmd)
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			// Exit 1 means HEAD is not a symbolic ref because it is detached.
			if ee.ExitCode() == 1 {
				return true, nil
			}
		}
		return false, fmt.Errorf("git symbolic-ref: %w", err)
	}
	return false, nil
}

// DefaultBranch queries a remote to determine its default branch name.
// Uses git ls-remote --symref to read the remote's HEAD symref.
// Falls back to "main" if detection fails (e.g. empty remote, unreachable).
func DefaultBranch(ctx context.Context, dir, remote string) string {
	out, err := Run(ctx, dir, "ls-remote", "--symref", remote, "HEAD")
	if err != nil {
		return "main"
	}
	// Output format: "ref: refs/heads/main\tHEAD\n<sha>\tHEAD\n"
	// Fields splits: ["ref:", "refs/heads/main", "HEAD"]
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "ref: refs/heads/") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return strings.TrimPrefix(parts[1], "refs/heads/")
			}
		}
	}
	return "main"
}

func ValidatePortableBranchName(branch string) error {
	if branch == "" || strings.TrimSpace(branch) != branch || strings.ContainsAny(branch, " \t\r\n;&|<>^()%!$`'\"") {
		return fmt.Errorf("branch name contains command-unsafe characters")
	}
	return nil
}

func ValidateLocalBranchName(branch string) error {
	if branch == "" || strings.HasPrefix(branch, "-") || strings.HasPrefix(branch, "refs/") {
		return fmt.Errorf("invalid short branch name %q", branch)
	}
	ref := "refs/heads/" + branch
	if _, err := Run(context.Background(), "", "check-ref-format", ref); err != nil {
		return fmt.Errorf("invalid branch name %q: %w", branch, err)
	}
	return nil
}

func RebaseInProgress(ctx context.Context, workDir string) bool {
	for _, dir := range []string{"rebase-merge", "rebase-apply"} {
		p, err := Run(ctx, workDir, "rev-parse", "--git-path", dir)
		if err != nil {
			continue
		}
		if !filepath.IsAbs(p) {
			p = filepath.Join(workDir, p)
		}
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// FetchRemoteBranch fetches a single branch into a remote-tracking ref.
// Uses a force-update refspec (+) so non-fast-forward updates (e.g. after
// a force push on the remote) are accepted instead of silently rejected.
func FetchRemoteBranch(ctx context.Context, dir, remote, branch string) error {
	refspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/%s/%s", branch, remote, branch)
	_, err := Run(ctx, dir, "fetch", "--no-tags", remote, refspec)
	return err
}

func FetchRemoteBranchToRef(ctx context.Context, dir, remote, branch, localRef string) error {
	refspec := fmt.Sprintf("+refs/heads/%s:%s", branch, localRef)
	_, err := Run(ctx, dir, "fetch", "--no-tags", remote, refspec)
	return err
}

// FetchRemoteBranchToPrivateRef fetches one branch into a caller-owned private
// ref without touching FETCH_HEAD or ordinary remote-tracking refs.
func FetchRemoteBranchToPrivateRef(ctx context.Context, dir, remote, branch, localRef string) error {
	refspec := fmt.Sprintf("+refs/heads/%s:%s", branch, localRef)
	_, err := Run(ctx, dir, "fetch", "--no-tags", "--no-write-fetch-head", remote, refspec)
	return err
}

// Push pushes a ref to a remote. If forceWithLease is true, uses
// --force-with-lease with the expectedSHA for safe force-push.
func Push(ctx context.Context, dir, remote, ref, expectedSHA string, forceWithLease bool) error {
	return PushWithOptions(ctx, dir, remote, ref, expectedSHA, forceWithLease, nil)
}

// PushWithOptions pushes HEAD to a remote with per-push options.
func PushWithOptions(ctx context.Context, dir, remote, ref, expectedSHA string, forceWithLease bool, pushOptions []string) error {
	return PushSHAWithOptions(ctx, dir, remote, "HEAD", ref, expectedSHA, forceWithLease, pushOptions)
}

// PushSHA pushes one exact source object to a remote ref. Callers with
// validation provenance use this instead of mutable HEAD so the object proven
// immediately before publication is the object Git sends.
func PushSHA(ctx context.Context, dir, remote, sourceSHA, ref, expectedSHA string, forceWithLease bool) error {
	return PushSHAWithOptions(ctx, dir, remote, sourceSHA, ref, expectedSHA, forceWithLease, nil)
}

// PushSHAWithOptions is PushSHA with per-push options.
func PushSHAWithOptions(ctx context.Context, dir, remote, sourceSHA, ref, expectedSHA string, forceWithLease bool, pushOptions []string) error {
	args := []string{"push"}
	for _, option := range pushOptions {
		args = append(args, "-o", option)
	}
	args = append(args, remote)
	if forceWithLease {
		if expectedSHA != "" {
			args = append(args, fmt.Sprintf("--force-with-lease=%s:%s", ref, expectedSHA))
		} else {
			args = append(args, "--force-with-lease")
		}
	}
	args = append(args, sourceSHA+":"+ref)
	_, err := Run(ctx, dir, args...)
	return err
}

// LsRemote returns the SHA of a ref on a remote. Returns empty string if the ref doesn't exist.
func LsRemote(ctx context.Context, dir, remote, ref string) (string, error) {
	out, err := Run(ctx, dir, "ls-remote", remote, ref)
	if err != nil {
		return "", err
	}
	if out == "" {
		return "", nil
	}
	// Output format: "<sha>\t<ref>"
	parts := strings.Fields(out)
	if len(parts) < 1 {
		return "", nil
	}
	return parts[0], nil
}

// HasUncommittedChanges reports whether the working tree or index differs from HEAD.
// Returns true if any tracked file is modified, staged, or deleted, or if there are
// untracked files. Equivalent to a non-empty `git status --porcelain`.
func HasUncommittedChanges(ctx context.Context, dir string) (bool, error) {
	out, err := Run(ctx, dir, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return out != "", nil
}

// CreateBranch creates a new branch with the given name and switches to it.
// Fails if the branch already exists.
func CreateBranch(ctx context.Context, dir, name string) error {
	_, err := Run(ctx, dir, "checkout", "-b", name)
	return err
}

// CommitAll stages every change in the working tree and creates a single commit
// with the given message. Fails if there are no changes to commit.
func CommitAll(ctx context.Context, dir, message string) error {
	if _, err := Run(ctx, dir, "add", "-A"); err != nil {
		return err
	}
	dirty, err := HasUncommittedChanges(ctx, dir)
	if err != nil {
		return err
	}
	if !dirty {
		return fmt.Errorf("no changes to commit")
	}
	_, err = Run(ctx, dir, "commit", "-m", message)
	return err
}

// LocalUserIdentity is the complete local commit identity copied into a
// pipeline worktree. Empty values mean the registered source has no local
// override; callers must not invent values from global config or commit data.
type LocalUserIdentity struct {
	Name  string
	Email string
}

// ReadLocalUserIdentity snapshots user.name and user.email from local scope.
func ReadLocalUserIdentity(ctx context.Context, srcDir string) (LocalUserIdentity, error) {
	var identity LocalUserIdentity
	for _, item := range []struct {
		key string
		dst *string
	}{{"user.name", &identity.Name}, {"user.email", &identity.Email}} {
		value, err := Run(ctx, srcDir, "config", "--local", "--get", "--default", "", item.key)
		if err != nil {
			return LocalUserIdentity{}, err
		}
		*item.dst = value
	}
	return identity, nil
}

// ReadWorktreeUserIdentity reads only per-worktree identity scope. It is used
// by recovery to verify that no identity other than the registered source's
// local values was introduced.
func ReadWorktreeUserIdentity(ctx context.Context, dir string) (LocalUserIdentity, error) {
	var identity LocalUserIdentity
	for _, item := range []struct {
		key string
		dst *string
	}{{"user.name", &identity.Name}, {"user.email", &identity.Email}} {
		value, err := Run(ctx, dir, "config", "--worktree", "--get", "--default", "", item.key)
		if err != nil {
			return LocalUserIdentity{}, err
		}
		*item.dst = value
	}
	return identity, nil
}

// ApplyWorktreeUserIdentity writes only per-worktree identity. Recovery uses
// this strict form so it can never fall back to changing the shared gate
// configuration when the incident's isolated worktree configuration is absent.
func ApplyWorktreeUserIdentity(ctx context.Context, dstDir string, identity LocalUserIdentity) error {
	for _, item := range []struct {
		key   string
		value string
	}{{"user.name", identity.Name}, {"user.email", identity.Email}} {
		if item.value == "" {
			continue
		}
		if _, err := Run(ctx, dstDir, "config", "--worktree", item.key, item.value); err != nil {
			return err
		}
	}
	return nil
}

// ApplyLocalUserIdentity writes exactly the supplied non-empty values. The
// write uses per-worktree scope when available so linked runs do not contend on
// the shared gate config. Older Git retains the existing local-scope fallback.
func ApplyLocalUserIdentity(ctx context.Context, dstDir string, identity LocalUserIdentity) error {
	for _, item := range []struct {
		key   string
		value string
	}{{"user.name", identity.Name}, {"user.email", identity.Email}} {
		if item.value == "" {
			continue
		}
		if _, err := Run(ctx, dstDir, "config", "--worktree", item.key, item.value); err != nil {
			if !isWorktreeConfigWriteUnavailable(err) {
				return err
			}
			if _, err := Run(ctx, dstDir, "config", "--local", item.key, item.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// CopyLocalUserIdentity snapshots the source identity once and applies it to
// dstDir. Missing values in srcDir are ignored.
func CopyLocalUserIdentity(ctx context.Context, srcDir, dstDir string) error {
	identity, err := ReadLocalUserIdentity(ctx, srcDir)
	if err != nil {
		return err
	}
	return ApplyLocalUserIdentity(ctx, dstDir, identity)
}

// isWorktreeConfigWriteUnavailable reports whether a `git config --worktree`
// write failed because per-worktree config cannot be used on this repo: either
// the installed Git is too old for the flag (isWorktreeConfigUnsupported), or
// the repo has more than one worktree without extensions.worktreeConfig enabled
// ("--worktree cannot be used with multiple working trees unless the config
// extension worktreeConfig is enabled"). Both mean the caller should fall back
// to the shared --local config.
func isWorktreeConfigWriteUnavailable(err error) bool {
	if isWorktreeConfigUnsupported(err) {
		return true
	}
	return strings.Contains(err.Error(), "worktreeConfig")
}

// WorktreeAdd creates a detached worktree at wtPath checked out to the given SHA.
func WorktreeAdd(ctx context.Context, repoDir, wtPath, sha string) error {
	_, err := Run(ctx, repoDir, "worktree", "add", "--detach", wtPath, sha)
	return err
}

// WorktreeRemove removes a worktree at the given path.
func WorktreeRemove(ctx context.Context, repoDir, wtPath string) error {
	_, err := Run(ctx, repoDir, "worktree", "remove", "--force", wtPath)
	return err
}

// ResolveRef returns the commit SHA that ref resolves to via
// `git rev-parse --verify <ref>^{commit}`. Use it to pin an exact commit
// (e.g. the pipeline-base tip just fetched) before reading a file from it,
// so a shared-ref worktree cannot serve a stale remote-tracking ref. Returns
// an error if the ref does not resolve to a commit.
func ResolveRef(ctx context.Context, dir, ref string) (string, error) {
	out, err := Run(ctx, dir, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve ref %s: %w", ref, err)
	}
	return out, nil
}

// RefExists reports whether the given ref resolves to a commit. It uses
// `git rev-parse --verify --quiet` so a missing ref is a clean (nil, false)
// result rather than a loud error.
func RefExists(ctx context.Context, dir, ref string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	cmd.Env = NonInteractiveEnv(dir)
	winproc.Harden(cmd)
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == 1 {
			return false, nil
		}
		return false, fmt.Errorf("git rev-parse %s: %w", ref, err)
	}
	return true, nil
}

// ShowFile returns the content of path as stored at the given ref (e.g.
// "HEAD", "origin/main", or a SHA) via `git show <ref>:<path>`. A failure
// (e.g. the path is absent at the ref) is returned as the underlying git
// error from Run; callers that need to distinguish "absent" from a real
// failure should check RefExists first or inspect the error text.
func ShowFile(ctx context.Context, dir, ref, path string) (string, error) {
	out, err := Run(ctx, dir, "show", fmt.Sprintf("%s:%s", ref, path))
	if err != nil {
		return "", err
	}
	return out, nil
}

// ShowFileBytes returns a blob's exact bytes without Run's stdout trimming.
// Callers use it when whitespace and final newlines are security-relevant, such
// as digest-bound policy authorization.
func ShowFileBytes(ctx context.Context, dir, ref, path string) ([]byte, error) {
	args := []string{"show", fmt.Sprintf("%s:%s", ref, path)}
	if isBareGitDir(dir) {
		args = append([]string{"--git-dir=" + dir}, args...)
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = NonInteractiveEnv(dir)
	winproc.Harden(cmd)
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(ee.Stderr))
		}
		return nil, fmt.Errorf("git %s: %w: %s", safeurl.RedactText(strings.Join(args, " ")), err, safeurl.RedactText(stderr))
	}
	return out, nil
}
