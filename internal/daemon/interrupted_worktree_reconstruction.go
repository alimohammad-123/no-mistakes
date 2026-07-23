package daemon

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
	"github.com/kunchenguid/no-mistakes/internal/safeurl"
	"github.com/kunchenguid/no-mistakes/internal/shellenv"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	arenaMissingWorktreeRepoID             = "935b6bc75a9a"
	arenaMissingWorktreeRunID              = "01KY4FMNYM4AX8PA9MY92QMHN4"
	arenaMissingWorktreeTestStepID         = "01KY4FMT47C26MBEMSE3XMVZ4J"
	arenaMissingWorktreeRepositoryIdentity = "repoid://github.com/arenacrm/arena-crm"
	arenaMissingWorktreeBranch             = "fm/arena-no-mistakes-beta"
	arenaMissingWorktreeBaseBranch         = "staging"
	arenaMissingWorktreeDefaultBranch      = "main"
	arenaMissingWorktreeSubmittedHead      = "d1434c61a3eea0523785c6452d665d63c5997af8"
	arenaMissingWorktreePipelineHead       = "9d15bc97a6c93d0b8466f70704cf90b510ed2596"
	arenaMissingWorktreeIntentSHA256       = "6d960b00993b2302d142848f6ddbba8dde9290913c22840c74b0297c830e3ff2"
	arenaMissingWorktreeBootstrapRepo      = "repoid://github.com/arenacrm/arena-crm"
	arenaMissingWorktreeBootstrapCommand   = "bun run ci:local -- --full"
	arenaMissingWorktreeBootstrapDigest    = "aa36a99df97f3f91c5fa0133621492cdf3a459fee57c104a8e0578a4516b9860"
	arenaMissingWorktreeCreatedAt          = int64(1784709535)
	arenaMissingWorktreeUpdatedAt          = int64(1784723813)
	arenaMissingWorktreeParkedMS           = int64(11095356)
	interruptedJournalVersion              = 1
	interruptedJournalManifestName         = "manifest.json"
)

var (
	matchInterruptedWorktreeIncident = func(repo *db.Repo, run *db.Run, gate *db.StepResult) error {
		return matchArenaMissingWorktreeIncident(repo, run, gate, arenaMissingWorktreeIntentSHA256)
	}
	probeInterruptedExternalState            = defaultInterruptedExternalStateProbe
	probeInterruptedProcessOwners            = defaultInterruptedProcessOwnershipProbe
	addInterruptedWorktree                   = gitpkg.WorktreeAdd
	validateInterruptedRegisteredWorkingPath = validateRegisteredWorkingPath
	interruptedReconstructionPoint           = func(string) error { return nil }
)

// interruptedWorktreeJournal is the privacy-safe, persistent ownership record
// for the one allowlisted reconstruction. It contains no intent, findings,
// session identity, prompt, or output.
type interruptedWorktreeJournal struct {
	Version               int    `json:"version"`
	RepoID                string `json:"repo_id"`
	RunID                 string `json:"run_id"`
	TargetRel             string `json:"target_rel"`
	GatePath              string `json:"gate_path"`
	AdminRel              string `json:"admin_rel,omitempty"`
	WorktreeParentExisted bool   `json:"worktree_parent_existed"`
	SubmittedHead         string `json:"submitted_head"`
	PipelineHead          string `json:"pipeline_head"`
	TreeSHA               string `json:"tree_sha"`
	CanonicalRef          string `json:"canonical_ref"`
	EvidenceToken         string `json:"evidence_token"`
	Phase                 string `json:"phase"`
	Nonce                 string `json:"nonce"`
	Checksum              string `json:"checksum"`
}

type interruptedPR struct {
	Number              int    `json:"number"`
	HeadRefName         string `json:"headRefName"`
	BaseRefName         string `json:"baseRefName"`
	HeadRepositoryOwner *struct {
		Login string `json:"login"`
	} `json:"headRepositoryOwner"`
}

type interruptedWorktreeReconstruction struct {
	manager  *RunManager
	repo     *db.Repo
	run      *db.Run
	manifest interruptedWorktreeJournal
	dir      string
	target   string
	gate     string
}

func matchArenaMissingWorktreeIncident(repo *db.Repo, run *db.Run, gate *db.StepResult, intentDigest string) error {
	if repo == nil || run == nil || gate == nil {
		return fmt.Errorf("incident fingerprint is incomplete")
	}
	identity, err := repoidentity.Canonical(repo.UpstreamURL)
	if err != nil {
		return fmt.Errorf("canonicalize repository identity: %w", err)
	}
	if repo.ID != arenaMissingWorktreeRepoID || identity != arenaMissingWorktreeRepositoryIdentity ||
		strings.TrimSpace(repo.ForkURL) != "" || repo.DefaultBranch != arenaMissingWorktreeDefaultBranch || repo.EffectiveBaseBranch() != arenaMissingWorktreeBaseBranch {
		return fmt.Errorf("repository does not match the allowlisted incident")
	}
	if run.ID != arenaMissingWorktreeRunID || run.RepoID != arenaMissingWorktreeRepoID || run.Branch != arenaMissingWorktreeBranch ||
		run.BaseBranch != arenaMissingWorktreeBaseBranch || run.BaseSHA != strings.Repeat("0", 40) ||
		run.SubmittedHeadSHA == nil || *run.SubmittedHeadSHA != arenaMissingWorktreeSubmittedHead ||
		run.HeadSHA != arenaMissingWorktreePipelineHead || run.Status != types.RunFailed || run.Error == nil || *run.Error != db.LegacyDaemonShutdownError ||
		run.SourceRef != nil || run.CreatedAt != arenaMissingWorktreeCreatedAt || run.UpdatedAt != arenaMissingWorktreeUpdatedAt || run.ParkedMS != arenaMissingWorktreeParkedMS {
		return fmt.Errorf("run does not match the allowlisted incident")
	}
	if run.Intent == nil || sha256Hex(*run.Intent) != intentDigest ||
		run.BootstrapTestRepository == nil || *run.BootstrapTestRepository != arenaMissingWorktreeBootstrapRepo ||
		run.BootstrapTestBaseBranch == nil || *run.BootstrapTestBaseBranch != arenaMissingWorktreeBaseBranch ||
		run.BootstrapTestCommand == nil || *run.BootstrapTestCommand != arenaMissingWorktreeBootstrapCommand ||
		run.BootstrapTestPolicySHA256 == nil || *run.BootstrapTestPolicySHA256 != arenaMissingWorktreeBootstrapDigest {
		return fmt.Errorf("intent or trusted Test authorization does not match the allowlisted incident")
	}
	if gate.ID != arenaMissingWorktreeTestStepID || gate.RunID != run.ID || gate.StepName != types.StepTest {
		return fmt.Errorf("interrupted Test gate does not match the allowlisted incident")
	}
	return nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (m *RunManager) ensureInterruptedPipelineCopy(ctx context.Context, repo *db.Repo, run *db.Run, inspected *db.InterruptedGateRestore) (string, string, *interruptedWorktreeReconstruction, error) {
	workDir := m.paths.WorktreeDir(repo.ID, run.ID)
	gateDir := m.paths.RepoDir(repo.ID)
	info, lstatErr := os.Lstat(workDir)
	if lstatErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", "", nil, fmt.Errorf("worktree is missing or not a real directory")
		}
		validatedWorkDir, validatedGateDir, err := m.validateInterruptedPipelineCopy(ctx, repo, run)
		if err != nil {
			return "", "", nil, err
		}
		journal, err := m.loadInterruptedWorktreeJournal(repo, run, inspected, workDir, gateDir)
		if err != nil {
			if !os.IsNotExist(err) {
				return "", "", nil, fmt.Errorf("inspect reconstruction journal: %w", err)
			}
			return validatedWorkDir, validatedGateDir, nil, nil
		}
		if err := journal.verifyCompletedCopy(ctx); err != nil {
			return "", "", nil, err
		}
		if err := journal.finishPreparation(ctx); err != nil {
			return "", "", nil, err
		}
		if _, _, err := m.validateInterruptedPipelineCopy(ctx, repo, run); err != nil {
			return "", "", nil, fmt.Errorf("reconstructed pipeline copy is invalid: %w", err)
		}
		return validatedWorkDir, validatedGateDir, journal, nil
	}
	if !os.IsNotExist(lstatErr) {
		return "", "", nil, fmt.Errorf("worktree is missing or not a real directory")
	}
	if inspected == nil || inspected.Step == nil || inspected.EvidenceToken == "" {
		return "", "", nil, fmt.Errorf("missing-worktree reconstruction has no durable evidence token")
	}
	if err := matchInterruptedWorktreeIncident(repo, run, inspected.Step); err != nil {
		return "", "", nil, fmt.Errorf("worktree is missing or not a real directory")
	}
	if err := m.validateInterruptedReconstructionMutableState(ctx, repo, run, workDir, gateDir, true); err != nil {
		return "", "", nil, err
	}
	treeSHA, err := m.validateInterruptedReconstructionGit(ctx, run, workDir, gateDir, false)
	if err != nil {
		return "", "", nil, err
	}
	journal, err := m.loadInterruptedWorktreeJournal(repo, run, inspected, workDir, gateDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", "", nil, fmt.Errorf("inspect reconstruction journal: %w", err)
		}
		journal, err = m.createInterruptedWorktreeJournal(repo, run, inspected, workDir, gateDir, treeSHA)
		if err != nil {
			return "", "", nil, err
		}
	}
	fail := func(primary error) (string, string, *interruptedWorktreeReconstruction, error) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cleanupErr := journal.rollback(cleanupCtx); cleanupErr != nil {
			return "", "", nil, fmt.Errorf("%w; rollback failed: %v", primary, cleanupErr)
		}
		return "", "", nil, primary
	}
	if err := interruptedReconstructionPoint("journal-created"); err != nil {
		return fail(err)
	}
	if err := journal.ensureWorktreeParent(); err != nil {
		return fail(fmt.Errorf("prepare reconstructed-worktree parent: %w", err))
	}
	if err := addInterruptedWorktree(ctx, gateDir, workDir, run.HeadSHA); err != nil {
		return fail(fmt.Errorf("reconstruct pipeline worktree: %w", err))
	}
	if err := interruptedReconstructionPoint("worktree-added"); err != nil {
		return fail(err)
	}
	if err := journal.finishPreparation(ctx); err != nil {
		return fail(err)
	}
	if _, _, err := m.validateInterruptedPipelineCopy(ctx, repo, run); err != nil {
		return fail(fmt.Errorf("reconstructed pipeline copy is invalid: %w", err))
	}
	return workDir, gateDir, journal, nil
}

func (m *RunManager) validateInterruptedReconstructionMutableState(ctx context.Context, repo *db.Repo, run *db.Run, workDir, gateDir string, targetAbsent bool) error {
	m.mu.Lock()
	_, hasExecutor := m.executors[run.ID]
	_, hasCancel := m.cancels[run.ID]
	_, hasDone := m.dones[run.ID]
	m.mu.Unlock()
	if hasExecutor || hasCancel || hasDone {
		return fmt.Errorf("missing-worktree reconstruction refused: run still has a live executor owner")
	}
	if err := validateInterruptedReconstructionPath(m.paths.Root(), m.paths.WorktreesDir(), workDir, repo.ID, run.ID, targetAbsent); err != nil {
		return fmt.Errorf("missing-worktree reconstruction refused: %w", err)
	}
	if err := validateInterruptedGatePath(m.paths.Root(), m.paths.ReposDir(), gateDir, repo.ID); err != nil {
		return fmt.Errorf("missing-worktree reconstruction refused: %w", err)
	}
	if err := validateInterruptedRegisteredWorkingPath(ctx, repo); err != nil {
		return fmt.Errorf("missing-worktree reconstruction refused: %w", err)
	}
	if err := probeInterruptedProcessOwners(ctx, workDir, run.ID); err != nil {
		return fmt.Errorf("missing-worktree reconstruction refused: process ownership: %w", err)
	}
	if err := probeInterruptedExternalState(ctx, repo, gateDir, run.Branch, run.EffectiveBaseBranch(repo)); err != nil {
		return fmt.Errorf("missing-worktree reconstruction refused: external state: %w", err)
	}
	return nil
}

func validateRegisteredWorkingPath(ctx context.Context, repo *db.Repo) error {
	info, err := os.Lstat(repo.WorkingPath)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("registered repository worktree is missing or not a real directory")
	}
	configured, err := gitpkg.GetConfiguredRemoteURL(ctx, repo.WorkingPath, "origin")
	if err != nil {
		return fmt.Errorf("resolve registered repository origin: %w", err)
	}
	configuredIdentity, err := repoidentity.Canonical(configured)
	if err != nil {
		return fmt.Errorf("canonicalize registered repository origin: %w", err)
	}
	recordedIdentity, err := repoidentity.Canonical(repo.UpstreamURL)
	if err != nil || configuredIdentity != recordedIdentity {
		return fmt.Errorf("registered repository origin no longer matches its recorded upstream")
	}
	return nil
}

func (m *RunManager) validateInterruptedReconstructionGit(ctx context.Context, run *db.Run, workDir, gateDir string, wantTarget bool) (string, error) {
	if len(run.HeadSHA) != 40 || strings.ToLower(run.HeadSHA) != run.HeadSHA {
		return "", fmt.Errorf("recorded pipeline head is not a full lowercase SHA")
	}
	if _, err := hex.DecodeString(run.HeadSHA); err != nil {
		return "", fmt.Errorf("recorded pipeline head is malformed")
	}
	if run.SubmittedHeadSHA == nil || len(*run.SubmittedHeadSHA) != 40 || strings.ToLower(*run.SubmittedHeadSHA) != *run.SubmittedHeadSHA {
		return "", fmt.Errorf("recorded submitted head is incomplete")
	}
	if _, err := hex.DecodeString(*run.SubmittedHeadSHA); err != nil {
		return "", fmt.Errorf("recorded submitted head is malformed")
	}
	canonicalRef, err := sourceprovenance.CanonicalSourceRefFromBranch(run.Branch)
	if err != nil {
		return "", fmt.Errorf("durable branch identity is invalid: %w", err)
	}
	resolved, err := gitpkg.ResolveRef(ctx, gateDir, canonicalRef)
	if err != nil || resolved != run.HeadSHA {
		return "", fmt.Errorf("pipeline source ref does not match recorded pipeline head")
	}
	if _, err := gitpkg.Run(ctx, gateDir, "rev-parse", "--verify", run.HeadSHA+"^{commit}"); err != nil {
		return "", fmt.Errorf("recorded pipeline head is unreachable in the local gate")
	}
	if _, err := gitpkg.Run(ctx, gateDir, "rev-parse", "--verify", *run.SubmittedHeadSHA+"^{commit}"); err != nil {
		return "", fmt.Errorf("submitted head is unreachable in the local gate")
	}
	if _, err := gitpkg.Run(ctx, gateDir, "merge-base", "--is-ancestor", *run.SubmittedHeadSHA, run.HeadSHA); err != nil {
		return "", fmt.Errorf("submitted head is not an ancestor of the pipeline head")
	}
	treeSHA, err := gitpkg.Run(ctx, gateDir, "show", "-s", "--format=%T", run.HeadSHA)
	if err != nil || len(treeSHA) != 40 {
		return "", fmt.Errorf("recorded pipeline tree is unreadable")
	}
	if _, err := gitpkg.Run(ctx, gateDir, "cat-file", "-e", treeSHA+"^{tree}"); err != nil {
		return "", fmt.Errorf("recorded pipeline tree is unreachable")
	}
	if err := verifyInterruptedWorktreeTopology(ctx, gateDir, workDir, run.ID, run.HeadSHA, wantTarget); err != nil {
		return "", err
	}
	if wantTarget {
		if err := validateInterruptedReconstructionPath(m.paths.Root(), m.paths.WorktreesDir(), workDir, run.RepoID, run.ID, false); err != nil {
			return "", err
		}
		observedTree, err := gitpkg.Run(ctx, workDir, "show", "-s", "--format=%T", "HEAD")
		if err != nil || observedTree != treeSHA {
			return "", fmt.Errorf("reconstructed worktree tree does not match the recorded pipeline tree")
		}
	}
	return treeSHA, nil
}

func verifyInterruptedWorktreeTopology(ctx context.Context, gateDir, target, runID, pipelineHead string, wantTarget bool) error {
	worktrees, err := gitpkg.ListWorktrees(ctx, gateDir)
	if err != nil {
		return fmt.Errorf("inspect gate worktree registrations: %w", err)
	}
	adminEntries, err := gitpkg.ListWorktreeAdminEntries(gateDir)
	if err != nil {
		return fmt.Errorf("inspect gate worktree administration: %w", err)
	}
	adminByDir := make(map[string]gitpkg.WorktreeAdminEntry, len(adminEntries))
	for _, entry := range adminEntries {
		resolved, err := filepath.EvalSymlinks(entry.Dir)
		if err != nil {
			return fmt.Errorf("resolve gate worktree administration %q: %w", entry.Name, err)
		}
		adminByDir[filepath.Clean(resolved)] = entry
	}
	seenAdmin := make(map[string]struct{}, len(adminEntries))
	targetCount := 0
	bareCount := 0
	for _, worktree := range worktrees {
		if worktree.Bare {
			bareCount++
			if !samePath(worktree.Path, gateDir) {
				return fmt.Errorf("gate worktree topology names an unexpected bare repository")
			}
			continue
		}
		if worktree.Prunable {
			return fmt.Errorf("gate worktree topology contains a prunable registration")
		}
		isTarget := samePath(worktree.Path, target)
		if isTarget {
			targetCount++
			if worktree.HEAD != pipelineHead || !worktree.Detached {
				return fmt.Errorf("registered reconstructed worktree does not match the detached pipeline head")
			}
		} else if filepath.Base(filepath.Clean(worktree.Path)) == runID || worktree.HEAD == pipelineHead {
			return fmt.Errorf("a replacement or moved pipeline worktree already exists")
		}
		adminDir, err := gitpkg.LinkedWorktreeAdminDir(worktree.Path)
		if err != nil {
			return fmt.Errorf("inspect linked worktree administration: %w", err)
		}
		resolvedAdmin, err := filepath.EvalSymlinks(adminDir)
		if err != nil {
			return fmt.Errorf("resolve linked worktree administration: %w", err)
		}
		resolvedAdmin = filepath.Clean(resolvedAdmin)
		entry, ok := adminByDir[resolvedAdmin]
		if !ok {
			return fmt.Errorf("linked worktree has no matching gate administration")
		}
		if !samePath(entry.WorktreePath, worktree.Path) {
			return fmt.Errorf("linked worktree administration names a different checkout")
		}
		if (entry.Name == runID || filepath.Base(entry.WorktreePath) == runID) && !isTarget {
			return fmt.Errorf("the interrupted run's worktree administration was moved")
		}
		seenAdmin[resolvedAdmin] = struct{}{}
	}
	if bareCount != 1 || len(seenAdmin) != len(adminByDir) {
		return fmt.Errorf("gate worktree administration contains an unowned entry")
	}
	if wantTarget && targetCount != 1 {
		return fmt.Errorf("reconstructed worktree registration is missing or duplicated")
	}
	if !wantTarget && targetCount != 0 {
		return fmt.Errorf("registered pipeline worktree already exists")
	}
	return nil
}

func validateInterruptedGatePath(rootPath, reposRoot, gateDir, repoID string) error {
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("daemon root is missing, symlinked, or not a directory")
	}
	reposInfo, err := os.Lstat(reposRoot)
	if err != nil || !reposInfo.IsDir() || reposInfo.Mode()&os.ModeSymlink != 0 || !sameFilesystemInfo(rootInfo, reposInfo) {
		return fmt.Errorf("gate root is missing, symlinked, mounted elsewhere, or not a directory")
	}
	if repoID != arenaMissingWorktreeRepoID || filepath.Clean(gateDir) != filepath.Join(reposRoot, repoID+".git") {
		return fmt.Errorf("gate repository path is not canonical")
	}
	gateInfo, err := os.Lstat(gateDir)
	if err != nil || !gateInfo.IsDir() || gateInfo.Mode()&os.ModeSymlink != 0 || !sameFilesystemInfo(reposInfo, gateInfo) {
		return fmt.Errorf("gate repository is missing, symlinked, mounted elsewhere, or not a directory")
	}
	return nil
}

func validateInterruptedReconstructionPath(rootPath, worktreesRoot, target, repoID, runID string, wantAbsent bool) error {
	if repoID != arenaMissingWorktreeRepoID || runID != arenaMissingWorktreeRunID || filepath.Base(repoID) != repoID || filepath.Base(runID) != runID {
		return fmt.Errorf("reconstruction path identity is not canonical")
	}
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("daemon root is missing, symlinked, or not a directory")
	}
	worktreesInfo, err := os.Lstat(worktreesRoot)
	if err != nil || !worktreesInfo.IsDir() || worktreesInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree root is missing, symlinked, or not a directory")
	}
	expected := filepath.Join(worktreesRoot, repoID, runID)
	if filepath.Clean(target) != expected {
		return fmt.Errorf("registered worktree path escaped its canonical location")
	}
	rel, err := filepath.Rel(worktreesRoot, target)
	if err != nil || rel != filepath.Join(repoID, runID) || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("registered worktree path is outside its owned root")
	}
	if !sameFilesystemInfo(rootInfo, worktreesInfo) {
		return fmt.Errorf("worktree root crosses a filesystem boundary")
	}
	parent := filepath.Dir(target)
	if parentInfo, err := os.Lstat(parent); err == nil {
		if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("worktree repository parent is symlinked or not a directory")
		}
		if !sameFilesystemInfo(worktreesInfo, parentInfo) {
			return fmt.Errorf("worktree repository parent crosses a filesystem boundary")
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect worktree repository parent: %w", err)
	}
	info, err := os.Lstat(target)
	if wantAbsent {
		if err == nil {
			return fmt.Errorf("registered worktree path already exists")
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("inspect registered worktree path: %w", err)
		}
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("reconstructed worktree is missing or not a real directory")
	}
	if !sameFilesystemInfo(worktreesInfo, info) {
		return fmt.Errorf("reconstructed worktree crosses a filesystem boundary")
	}
	return nil
}

func defaultInterruptedProcessOwnershipProbe(ctx context.Context, target, runID string) error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("process ownership proof is supported only for the allowlisted macOS incident")
	}
	const lsof = "/usr/sbin/lsof"
	if info, err := os.Stat(lsof); err != nil || info.IsDir() {
		return fmt.Errorf("%s is unavailable", lsof)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, lsof, "-n", "-P", "-Fpcfn")
	shellenv.ConfigureShellCommand(cmd)
	out, err := shellenv.CombinedOutputShellCommand(cmd)
	if err != nil {
		return fmt.Errorf("lsof ownership probe failed: %w", err)
	}
	text := string(out)
	if strings.Contains(text, target) || strings.Contains(text, runID) {
		return fmt.Errorf("a live process still owns the interrupted worktree")
	}
	return nil
}

func defaultInterruptedExternalStateProbe(ctx context.Context, repo *db.Repo, gateDir, branch, base string) error {
	if base != arenaMissingWorktreeBaseBranch {
		return fmt.Errorf("pipeline base changed before reconstruction")
	}
	configuredURL, err := gitpkg.GetConfiguredRemoteURL(ctx, gateDir, "origin")
	if err != nil {
		return fmt.Errorf("resolve gate origin: %w", err)
	}
	configuredIdentity, err := repoidentity.Canonical(configuredURL)
	if err != nil || configuredIdentity != arenaMissingWorktreeRepositoryIdentity {
		return fmt.Errorf("gate origin no longer identifies the allowlisted parent")
	}
	canonicalRef, err := sourceprovenance.CanonicalSourceRefFromBranch(branch)
	if err != nil {
		return err
	}
	remote, err := gitpkg.Run(ctx, gateDir, "ls-remote", "--refs", "origin", canonicalRef)
	if err != nil {
		return fmt.Errorf("read candidate remote branch: %w", err)
	}
	if strings.TrimSpace(remote) != "" {
		return fmt.Errorf("candidate branch already exists on the parent remote")
	}
	gh, err := exec.LookPath("gh")
	if err != nil {
		return fmt.Errorf("gh CLI is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, gh, "pr", "list", "--repo", "ArenaCRM/arena-crm", "--head", branch, "--base", base, "--state", "all", "--json", "number,headRefName,headRepositoryOwner,baseRefName")
	cmd.Dir = repo.WorkingPath
	shellenv.ConfigureShellCommand(cmd)
	out, err := shellenv.CombinedOutputShellCommand(cmd)
	if err != nil {
		return fmt.Errorf("list all-state pull requests: %s: %w", safeurl.RedactText(strings.TrimSpace(string(out))), err)
	}
	var prs []interruptedPR
	if err := json.Unmarshal(out, &prs); err != nil {
		return fmt.Errorf("parse all-state pull requests: %w", err)
	}
	number, exists, err := arenaCandidatePR(prs, branch, base)
	if err != nil {
		return fmt.Errorf("validate all-state pull requests: %w", err)
	}
	if exists {
		return fmt.Errorf("pull request %d already exists for the candidate branch", number)
	}
	return nil
}

func arenaCandidatePR(prs []interruptedPR, branch, base string) (int, bool, error) {
	for _, pr := range prs {
		if pr.Number <= 0 || pr.HeadRefName != branch || pr.BaseRefName != base || pr.HeadRepositoryOwner == nil || strings.TrimSpace(pr.HeadRepositoryOwner.Login) == "" {
			return 0, false, fmt.Errorf("pull request query returned incomplete or mismatched identity")
		}
		if strings.EqualFold(pr.HeadRepositoryOwner.Login, "ArenaCRM") {
			return pr.Number, true, nil
		}
	}
	return 0, false, nil
}

func (m *RunManager) createInterruptedWorktreeJournal(repo *db.Repo, run *db.Run, inspected *db.InterruptedGateRestore, workDir, gateDir, treeSHA string) (*interruptedWorktreeReconstruction, error) {
	resolvedGate, err := filepath.EvalSymlinks(gateDir)
	if err != nil {
		return nil, fmt.Errorf("resolve gate for reconstruction journal: %w", err)
	}
	targetRel, err := filepath.Rel(m.paths.WorktreesDir(), workDir)
	if err != nil || filepath.IsAbs(targetRel) || targetRel != filepath.Join(repo.ID, run.ID) || strings.HasPrefix(targetRel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("reconstruction target is outside the worktree root")
	}
	parentExisted := false
	if info, statErr := os.Lstat(filepath.Dir(workDir)); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("reconstruction worktree parent is not a real directory")
		}
		parentExisted = true
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect reconstruction worktree parent: %w", statErr)
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("create reconstruction nonce: %w", err)
	}
	journal := &interruptedWorktreeReconstruction{
		manager: m, repo: repo, run: run, dir: m.paths.InterruptedWorktreeRecoveryDir(repo.ID, run.ID), target: workDir, gate: gateDir,
		manifest: interruptedWorktreeJournal{
			Version: interruptedJournalVersion, RepoID: repo.ID, RunID: run.ID, TargetRel: filepath.ToSlash(targetRel),
			GatePath: resolvedGate, WorktreeParentExisted: parentExisted, SubmittedHead: *run.SubmittedHeadSHA, PipelineHead: run.HeadSHA, TreeSHA: treeSHA,
			CanonicalRef: "refs/heads/" + run.Branch, EvidenceToken: inspected.EvidenceToken, Phase: "prepared", Nonce: hex.EncodeToString(nonceBytes),
		},
	}
	if err := journal.create(); err != nil {
		return nil, err
	}
	return journal, nil
}

func (m *RunManager) loadInterruptedWorktreeJournal(repo *db.Repo, run *db.Run, inspected *db.InterruptedGateRestore, workDir, gateDir string) (*interruptedWorktreeReconstruction, error) {
	journal := &interruptedWorktreeReconstruction{manager: m, repo: repo, run: run, dir: m.paths.InterruptedWorktreeRecoveryDir(repo.ID, run.ID), target: workDir, gate: gateDir}
	if err := journal.load(); err != nil {
		return nil, err
	}
	if err := journal.matches(repo, run, inspected, workDir, gateDir); err != nil {
		return nil, err
	}
	return journal, nil
}

func (m *RunManager) loadClaimedInterruptedWorktreeJournal(repo *db.Repo, run *db.Run, workDir, gateDir string) (*interruptedWorktreeReconstruction, error) {
	if repo.ID != arenaMissingWorktreeRepoID || run.ID != arenaMissingWorktreeRunID {
		return nil, nil
	}
	journal := &interruptedWorktreeReconstruction{manager: m, repo: repo, run: run, dir: m.paths.InterruptedWorktreeRecoveryDir(repo.ID, run.ID), target: workDir, gate: gateDir}
	if err := journal.load(); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if journal.manifest.Version != interruptedJournalVersion || journal.manifest.RepoID != repo.ID || journal.manifest.RunID != run.ID ||
		run.SubmittedHeadSHA == nil || journal.manifest.SubmittedHead != *run.SubmittedHeadSHA || journal.manifest.PipelineHead != run.HeadSHA ||
		journal.manifest.CanonicalRef != "refs/heads/"+run.Branch || journal.manifest.Phase != "identity" || journal.manifest.AdminRel == "" {
		return nil, fmt.Errorf("claimed reconstruction journal does not match the running incident")
	}
	resolvedGate, err := filepath.EvalSymlinks(gateDir)
	if err != nil || journal.manifest.GatePath != resolvedGate {
		return nil, fmt.Errorf("claimed reconstruction journal gate changed")
	}
	if tree, err := m.validateInterruptedReconstructionGit(context.Background(), run, workDir, gateDir, true); err != nil || tree != journal.manifest.TreeSHA {
		return nil, fmt.Errorf("claimed reconstruction journal worktree changed")
	}
	if err := journal.finishPreparation(context.Background()); err != nil {
		return nil, fmt.Errorf("claimed reconstruction journal ownership changed: %w", err)
	}
	return journal, nil
}

func (j *interruptedWorktreeReconstruction) rootAndRel() (*os.Root, string, error) {
	rootPath := j.manager.paths.Root()
	info, err := os.Lstat(rootPath)
	if err != nil {
		return nil, "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, "", fmt.Errorf("daemon root is not a real directory")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, "", err
	}
	rel, err := filepath.Rel(j.manager.paths.Root(), j.dir)
	if err != nil || filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		root.Close()
		return nil, "", fmt.Errorf("reconstruction journal escaped the daemon root")
	}
	return root, rel, nil
}

func (j *interruptedWorktreeReconstruction) create() error {
	root, rel, err := j.rootAndRel()
	if err != nil {
		return fmt.Errorf("open reconstruction journal root: %w", err)
	}
	defer root.Close()
	parent := filepath.Dir(rel)
	if err := mkdirRootPath(root, parent, 0o700); err != nil {
		return fmt.Errorf("create reconstruction journal parent: %w", err)
	}
	if err := root.Mkdir(rel, 0o700); err != nil {
		return fmt.Errorf("create exclusive reconstruction journal: %w", err)
	}
	if err := syncRootDir(root, parent); err != nil {
		_ = root.RemoveAll(rel)
		return fmt.Errorf("sync reconstruction journal parent: %w", err)
	}
	if err := j.persistWithRoot(root, rel, true); err != nil {
		_ = root.RemoveAll(rel)
		return err
	}
	return nil
}

func syncRootDir(root *os.Root, rel string) error {
	file, err := root.Open(rel)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func readRootDir(root *os.Root, rel string) ([]os.DirEntry, error) {
	file, err := root.Open(rel)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.ReadDir(-1)
}

func mkdirRootPath(root *os.Root, rel string, mode os.FileMode) error {
	current := ""
	for _, component := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if os.IsNotExist(err) {
			if err := root.Mkdir(current, mode); err != nil {
				return err
			}
			if err := syncRootDir(root, filepath.Dir(current)); err != nil {
				return err
			}
			info, err = root.Lstat(current)
		}
		if err != nil {
			return fmt.Errorf("inspect reconstruction journal path: %w", err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("reconstruction journal path contains a symlink or non-directory")
		}
	}
	return nil
}

func (j *interruptedWorktreeReconstruction) load() error {
	root, rel, err := j.rootAndRel()
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(rel)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("reconstruction journal is not a private real directory")
	}
	manifestRel := filepath.Join(rel, interruptedJournalManifestName)
	manifestInfo, err := root.Lstat(manifestRel)
	if err != nil {
		return err
	}
	if !manifestInfo.Mode().IsRegular() || manifestInfo.Mode()&os.ModeSymlink != 0 || manifestInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("reconstruction journal manifest is not a private regular file")
	}
	data, err := root.ReadFile(manifestRel)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &j.manifest); err != nil {
		return fmt.Errorf("parse reconstruction journal: %w", err)
	}
	if !j.manifest.validChecksum() {
		return fmt.Errorf("reconstruction journal checksum mismatch")
	}
	return nil
}

func (j *interruptedWorktreeReconstruction) matches(repo *db.Repo, run *db.Run, inspected *db.InterruptedGateRestore, workDir, gateDir string) error {
	if run.SubmittedHeadSHA == nil || inspected == nil {
		return fmt.Errorf("reconstruction journal has incomplete durable evidence")
	}
	if j.manifest.Version != interruptedJournalVersion || j.manifest.RepoID != repo.ID || j.manifest.RunID != run.ID ||
		j.manifest.SubmittedHead != *run.SubmittedHeadSHA || j.manifest.PipelineHead != run.HeadSHA ||
		j.manifest.EvidenceToken != inspected.EvidenceToken || j.manifest.Nonce == "" {
		return fmt.Errorf("reconstruction journal does not match durable incident evidence")
	}
	expectedTarget := filepath.Join(j.manager.paths.WorktreesDir(), filepath.FromSlash(j.manifest.TargetRel))
	resolvedGate, err := filepath.EvalSymlinks(gateDir)
	if err != nil {
		return err
	}
	if filepath.Clean(expectedTarget) != filepath.Clean(workDir) || filepath.FromSlash(j.manifest.TargetRel) != filepath.Join(repo.ID, run.ID) ||
		j.manifest.GatePath != resolvedGate || j.manifest.CanonicalRef != "refs/heads/"+run.Branch {
		return fmt.Errorf("reconstruction journal path or source ref does not match the incident")
	}
	return nil
}

func (j *interruptedWorktreeReconstruction) ensureWorktreeParent() error {
	if j.manifest.WorktreeParentExisted {
		info, err := os.Lstat(filepath.Dir(j.target))
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("pre-existing worktree parent changed")
		}
		return nil
	}
	rootPath := j.manager.paths.WorktreesDir()
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree root is not a real directory")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()
	rel := j.repo.ID
	if info, err := root.Lstat(rel); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("journal-owned worktree parent is not a real directory")
		}
		entries, err := readRootDir(root, rel)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return fmt.Errorf("journal-owned worktree parent is no longer empty")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := root.Mkdir(rel, 0o755); err != nil {
		return err
	}
	return syncRootDir(root, ".")
}

func interruptedAdminRel(gateDir, adminDir string) (string, error) {
	resolvedGate, err := filepath.EvalSymlinks(gateDir)
	if err != nil {
		return "", err
	}
	resolvedAdmin, err := filepath.EvalSymlinks(adminDir)
	if err != nil {
		return "", err
	}
	adminRel, err := filepath.Rel(resolvedGate, resolvedAdmin)
	if err != nil || filepath.IsAbs(adminRel) || filepath.Dir(adminRel) != "worktrees" || filepath.Base(adminRel) == "." || filepath.Base(adminRel) == ".." {
		return "", fmt.Errorf("reconstructed-worktree administration escaped the gate")
	}
	return filepath.ToSlash(adminRel), nil
}

func (j *interruptedWorktreeReconstruction) finishPreparation(ctx context.Context) error {
	if err := j.verifyCompletedCopy(ctx); err != nil {
		return err
	}
	adminDir, err := gitpkg.LinkedWorktreeAdminDir(j.target)
	if err != nil {
		return fmt.Errorf("discover reconstructed-worktree administration: %w", err)
	}
	adminRel, err := interruptedAdminRel(j.gate, adminDir)
	if err != nil {
		return err
	}
	if j.manifest.AdminRel != "" && j.manifest.AdminRel != adminRel {
		return fmt.Errorf("reconstructed-worktree administration changed")
	}
	if j.manifest.Phase == "prepared" {
		j.manifest.AdminRel = adminRel
		j.manifest.Phase = "added"
		if err := j.persist(); err != nil {
			return fmt.Errorf("persist reconstructed-worktree phase: %w", err)
		}
		if err := interruptedReconstructionPoint("registration-recorded"); err != nil {
			return err
		}
	}
	identity, err := gitpkg.ReadLocalUserIdentity(ctx, j.repo.WorkingPath)
	if err != nil {
		return fmt.Errorf("read registered worktree identity: %w", err)
	}
	if j.manifest.Phase == "added" {
		if err := gitpkg.ApplyWorktreeUserIdentity(ctx, j.target, identity); err != nil {
			return fmt.Errorf("restore worktree git identity: %w", err)
		}
		j.manifest.Phase = "identity"
		if err := j.persist(); err != nil {
			return fmt.Errorf("persist reconstructed identity phase: %w", err)
		}
		if err := interruptedReconstructionPoint("identity-restored"); err != nil {
			return err
		}
	}
	actualIdentity, err := gitpkg.ReadWorktreeUserIdentity(ctx, j.target)
	if err != nil {
		return fmt.Errorf("verify worktree git identity: %w", err)
	}
	if actualIdentity != identity {
		return fmt.Errorf("worktree git identity does not match the registered source")
	}
	return nil
}

func (j *interruptedWorktreeReconstruction) verifyCompletedCopy(ctx context.Context) error {
	if j.manifest.Phase != "added" && j.manifest.Phase != "identity" && j.manifest.Phase != "prepared" {
		return fmt.Errorf("reconstruction journal has an invalid phase %q", j.manifest.Phase)
	}
	if j.manifest.Phase != "prepared" && j.manifest.AdminRel == "" {
		return fmt.Errorf("reconstruction journal is missing worktree administration ownership")
	}
	tree, err := j.manager.validateInterruptedReconstructionGit(ctx, j.run, j.target, j.gate, true)
	if err != nil {
		return fmt.Errorf("journal-owned reconstructed worktree is invalid: %w", err)
	}
	if tree != j.manifest.TreeSHA {
		return fmt.Errorf("journal-owned reconstructed tree changed")
	}
	if j.manifest.AdminRel != "" {
		adminDir, err := gitpkg.LinkedWorktreeAdminDir(j.target)
		if err != nil {
			return err
		}
		rel, err := interruptedAdminRel(j.gate, adminDir)
		if err != nil || rel != j.manifest.AdminRel {
			return fmt.Errorf("journal-owned worktree administration changed")
		}
	}
	return nil
}

func (j *interruptedWorktreeReconstruction) persist() error {
	root, rel, err := j.rootAndRel()
	if err != nil {
		return err
	}
	defer root.Close()
	return j.persistWithRoot(root, rel, false)
}

func (j *interruptedWorktreeReconstruction) persistWithRoot(root *os.Root, rel string, exclusive bool) error {
	j.manifest.Checksum = j.manifest.checksum()
	data, err := json.Marshal(j.manifest)
	if err != nil {
		return err
	}
	manifestRel := filepath.Join(rel, interruptedJournalManifestName)
	if exclusive {
		file, err := root.OpenFile(manifestRel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if _, err := file.Write(data); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		return syncRootDir(root, rel)
	}
	tempRel := filepath.Join(rel, "manifest.tmp")
	_ = root.Remove(tempRel)
	file, err := root.OpenFile(tempRel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := root.Rename(tempRel, manifestRel); err != nil {
		return err
	}
	return syncRootDir(root, rel)
}

func (m interruptedWorktreeJournal) checksum() string {
	m.Checksum = ""
	data, _ := json.Marshal(m)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (m interruptedWorktreeJournal) validChecksum() bool {
	return m.Checksum != "" && m.Checksum == m.checksum()
}

func (j *interruptedWorktreeReconstruction) removeJournal() error {
	root, rel, err := j.rootAndRel()
	if err != nil {
		return err
	}
	defer root.Close()
	current := &interruptedWorktreeReconstruction{manager: j.manager, dir: j.dir}
	if err := current.load(); err != nil {
		return err
	}
	if current.manifest.Nonce != j.manifest.Nonce || current.manifest.Checksum != j.manifest.Checksum {
		return fmt.Errorf("reconstruction journal ownership changed")
	}
	entries, err := readRootDir(root, rel)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != interruptedJournalManifestName && entry.Name() != "manifest.tmp" {
			return fmt.Errorf("reconstruction journal contains an unowned entry")
		}
		info, err := root.Lstat(filepath.Join(rel, entry.Name()))
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("reconstruction journal contains an ambiguous entry")
		}
	}
	if err := root.RemoveAll(rel); err != nil {
		return err
	}
	for parent := filepath.Dir(rel); parent != "." && parent != ""; parent = filepath.Dir(parent) {
		entries, err := readRootDir(root, parent)
		if err != nil || len(entries) != 0 {
			break
		}
		if err := root.Remove(parent); err != nil {
			break
		}
	}
	return syncRootDir(root, ".")
}

func (j *interruptedWorktreeReconstruction) verifyRemovalOwnership(ctx context.Context) (bool, error) {
	worktrees, err := gitpkg.ListWorktrees(ctx, j.gate)
	if err != nil {
		return false, err
	}
	adminEntries, err := gitpkg.ListWorktreeAdminEntries(j.gate)
	if err != nil {
		return false, err
	}
	registered := false
	for _, worktree := range worktrees {
		if worktree.Bare {
			continue
		}
		if samePath(worktree.Path, j.target) {
			if registered || worktree.HEAD != j.run.HeadSHA || !worktree.Detached {
				return false, fmt.Errorf("journal target registration no longer identifies the reconstructed head")
			}
			registered = true
			continue
		}
		if filepath.Base(filepath.Clean(worktree.Path)) == j.run.ID || worktree.HEAD == j.run.HeadSHA {
			return false, fmt.Errorf("replacement pipeline worktree appeared during rollback")
		}
	}
	adminMatches := 0
	for _, entry := range adminEntries {
		if samePath(entry.WorktreePath, j.target) {
			adminMatches++
			if j.manifest.AdminRel != "" {
				rel, relErr := interruptedAdminRel(j.gate, entry.Dir)
				if relErr != nil || rel != j.manifest.AdminRel {
					return false, fmt.Errorf("journal target administration changed during rollback")
				}
			}
		} else if entry.Name == j.run.ID || filepath.Base(entry.WorktreePath) == j.run.ID {
			return false, fmt.Errorf("moved pipeline administration appeared during rollback")
		}
	}
	if registered && adminMatches != 1 {
		return false, fmt.Errorf("journal target registration has ambiguous administration")
	}
	if registered {
		dirty, err := gitpkg.HasUncommittedChanges(ctx, j.target)
		if err != nil || dirty {
			return false, fmt.Errorf("journal target changed before rollback")
		}
		for _, marker := range []string{"rebase-merge", "rebase-apply", "MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "sequencer"} {
			path, err := gitpkg.Run(ctx, j.target, "rev-parse", "--git-path", marker)
			if err != nil {
				return false, fmt.Errorf("inspect journal target operation state: %w", err)
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(j.target, path)
			}
			if _, err := os.Lstat(path); err == nil || !os.IsNotExist(err) {
				return false, fmt.Errorf("journal target has ambiguous operation state")
			}
		}
	}
	if !registered && adminMatches != 0 {
		return false, fmt.Errorf("journal target administration has no matching registration")
	}
	return registered, nil
}

func (j *interruptedWorktreeReconstruction) rollback(ctx context.Context) error {
	current := &interruptedWorktreeReconstruction{manager: j.manager, dir: j.dir}
	if err := current.load(); err != nil {
		return fmt.Errorf("verify reconstruction journal ownership: %w", err)
	}
	if current.manifest.Nonce != j.manifest.Nonce || current.manifest.RunID != j.run.ID || current.manifest.RepoID != j.repo.ID || current.manifest.Checksum != j.manifest.Checksum {
		return fmt.Errorf("reconstruction journal ownership changed")
	}
	registered, err := j.verifyRemovalOwnership(ctx)
	if err != nil {
		return err
	}
	if registered {
		if err := gitpkg.WorktreeRemove(ctx, j.gate, j.target); err != nil {
			return fmt.Errorf("remove journal-owned linked worktree: %w", err)
		}
	} else if _, err := os.Lstat(j.target); err == nil {
		if err := validateInterruptedReconstructionPath(j.manager.paths.Root(), j.manager.paths.WorktreesDir(), j.target, j.repo.ID, j.run.ID, false); err != nil {
			return fmt.Errorf("refuse ambiguous partial-worktree cleanup: %w", err)
		}
		root, err := os.OpenRoot(j.manager.paths.Root())
		if err != nil {
			return err
		}
		targetRel, err := filepath.Rel(j.manager.paths.Root(), j.target)
		if err == nil {
			err = root.RemoveAll(targetRel)
		}
		root.Close()
		if err != nil {
			return fmt.Errorf("remove journal-owned partial worktree: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	worktrees, err := gitpkg.ListWorktrees(ctx, j.gate)
	if err != nil {
		return err
	}
	for _, worktree := range worktrees {
		if !worktree.Bare && samePath(worktree.Path, j.target) {
			return fmt.Errorf("journal-owned worktree registration survived cleanup")
		}
	}
	if _, err := os.Lstat(j.target); !os.IsNotExist(err) {
		return fmt.Errorf("journal-owned worktree path survived cleanup")
	}
	if !j.manifest.WorktreeParentExisted {
		root, openErr := os.OpenRoot(j.manager.paths.WorktreesDir())
		if openErr != nil {
			return openErr
		}
		entries, readErr := readRootDir(root, j.repo.ID)
		if readErr == nil && len(entries) == 0 {
			readErr = root.Remove(j.repo.ID)
		}
		root.Close()
		if readErr != nil && !os.IsNotExist(readErr) {
			return fmt.Errorf("remove journal-owned empty worktree parent: %w", readErr)
		}
	}
	return j.removeJournal()
}

// reconcileInterruptedWorktreeJournal makes crash points deterministic before
// ordinary stale-run/worktree cleanup. It never reconstructs or claims a run;
// only the ordinary exact AXI path may do that.
func (m *RunManager) reconcileInterruptedWorktreeJournal(ctx context.Context) {
	dir := m.paths.InterruptedWorktreeRecoveryDir(arenaMissingWorktreeRepoID, arenaMissingWorktreeRunID)
	if _, err := os.Lstat(dir); os.IsNotExist(err) {
		return
	}
	run, err := m.db.GetRun(arenaMissingWorktreeRunID)
	if err != nil || run == nil {
		slog.Warn("cannot reconcile interrupted-worktree journal", "error", err)
		return
	}
	repo, err := m.db.GetRepo(arenaMissingWorktreeRepoID)
	if err != nil || repo == nil {
		slog.Warn("cannot reconcile interrupted-worktree journal repository", "error", err)
		return
	}
	canonicalRef, err := sourceprovenance.CanonicalSourceRefFromBranch(run.Branch)
	if err != nil || run.SubmittedHeadSHA == nil || run.Intent == nil {
		slog.Warn("cannot reconcile interrupted-worktree journal identity", "error", err)
		return
	}
	if run.Status == types.RunFailed && run.Error != nil && *run.Error == db.LegacyDaemonShutdownError {
		inspected, inspectErr := m.db.InspectLegacyInterruptedGate(run.ID, repo.ID, run.Branch, run.HeadSHA, *run.SubmittedHeadSHA, *run.Intent, canonicalRef, types.AllSteps())
		if inspectErr != nil || matchInterruptedWorktreeIncident(repo, run, inspected.Step) != nil {
			slog.Warn("interrupted-worktree journal no longer matches the failed run", "error", inspectErr)
			return
		}
		journal, loadErr := m.loadInterruptedWorktreeJournal(repo, run, inspected, m.paths.WorktreeDir(repo.ID, run.ID), m.paths.RepoDir(repo.ID))
		if loadErr != nil {
			if errors.Is(loadErr, os.ErrNotExist) {
				if removeErr := m.removeEmptyInterruptedJournalMarker(ctx, repo, run, dir); removeErr == nil {
					return
				} else {
					slog.Warn("cannot clear empty interrupted-worktree journal marker", "error", removeErr)
					return
				}
			}
			slog.Warn("cannot load interrupted-worktree journal", "error", loadErr)
			return
		}
		if _, statErr := os.Lstat(journal.target); os.IsNotExist(statErr) {
			if topologyErr := verifyInterruptedWorktreeTopology(ctx, journal.gate, journal.target, run.ID, run.HeadSHA, false); topologyErr == nil {
				if removeErr := journal.removeJournal(); removeErr != nil {
					slog.Warn("failed to clear empty interrupted-worktree journal", "error", removeErr)
				}
				return
			}
		}
		if verifyErr := journal.verifyCompletedCopy(ctx); verifyErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if rollbackErr := journal.rollback(cleanupCtx); rollbackErr != nil {
				slog.Warn("failed to roll back partial interrupted-worktree reconstruction", "error", rollbackErr)
			}
		}
		return
	}
	if run.Status == types.RunRunning && run.AwaitingAgentSince != nil {
		return // normal parked-run recovery owns the complete copy and journal
	}
	journal := &interruptedWorktreeReconstruction{manager: m, repo: repo, run: run, dir: dir, target: m.paths.WorktreeDir(repo.ID, run.ID), gate: m.paths.RepoDir(repo.ID)}
	if loadErr := journal.load(); loadErr != nil {
		slog.Warn("cannot clean nonmatching interrupted-worktree journal", "error", loadErr)
		return
	}
	if identityErr := journal.matchesAllowlistedCleanupIdentity(repo, run); identityErr != nil {
		slog.Warn("refusing to clean unowned interrupted-worktree artifact", "error", identityErr)
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if rollbackErr := journal.rollback(cleanupCtx); rollbackErr != nil {
		slog.Warn("failed to clean nonmatching interrupted-worktree artifact", "error", rollbackErr)
	}
}

func (j *interruptedWorktreeReconstruction) matchesAllowlistedCleanupIdentity(repo *db.Repo, run *db.Run) error {
	identity, err := repoidentity.Canonical(repo.UpstreamURL)
	if err != nil {
		return err
	}
	if repo.ID != arenaMissingWorktreeRepoID || identity != arenaMissingWorktreeRepositoryIdentity || strings.TrimSpace(repo.ForkURL) != "" ||
		repo.DefaultBranch != arenaMissingWorktreeDefaultBranch || repo.EffectiveBaseBranch() != arenaMissingWorktreeBaseBranch {
		return fmt.Errorf("repository identity no longer matches the allowlisted incident")
	}
	if run.ID != arenaMissingWorktreeRunID || run.RepoID != arenaMissingWorktreeRepoID || run.Branch != arenaMissingWorktreeBranch ||
		run.BaseBranch != arenaMissingWorktreeBaseBranch || run.BaseSHA != strings.Repeat("0", 40) || run.SubmittedHeadSHA == nil ||
		*run.SubmittedHeadSHA != arenaMissingWorktreeSubmittedHead || run.HeadSHA != arenaMissingWorktreePipelineHead || run.Intent == nil ||
		sha256Hex(*run.Intent) != arenaMissingWorktreeIntentSHA256 {
		return fmt.Errorf("run identity no longer matches the allowlisted incident")
	}
	resolvedGate, err := filepath.EvalSymlinks(j.gate)
	if err != nil {
		return err
	}
	if j.manifest.Version != interruptedJournalVersion || j.manifest.RepoID != arenaMissingWorktreeRepoID || j.manifest.RunID != arenaMissingWorktreeRunID ||
		filepath.FromSlash(j.manifest.TargetRel) != filepath.Join(arenaMissingWorktreeRepoID, arenaMissingWorktreeRunID) ||
		j.manifest.GatePath != resolvedGate || j.manifest.SubmittedHead != arenaMissingWorktreeSubmittedHead ||
		j.manifest.PipelineHead != arenaMissingWorktreePipelineHead || j.manifest.CanonicalRef != "refs/heads/"+arenaMissingWorktreeBranch ||
		j.manifest.TreeSHA == "" || j.manifest.EvidenceToken == "" || j.manifest.Nonce == "" {
		return fmt.Errorf("journal identity no longer matches the allowlisted incident")
	}
	return nil
}

func (m *RunManager) removeEmptyInterruptedJournalMarker(ctx context.Context, repo *db.Repo, run *db.Run, dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("journal marker is not a private real directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		return fmt.Errorf("journal marker is not provably empty")
	}
	target := m.paths.WorktreeDir(repo.ID, run.ID)
	if err := validateInterruptedReconstructionPath(m.paths.Root(), m.paths.WorktreesDir(), target, repo.ID, run.ID, true); err != nil {
		return err
	}
	gate := m.paths.RepoDir(repo.ID)
	if err := validateInterruptedGatePath(m.paths.Root(), m.paths.ReposDir(), gate, repo.ID); err != nil {
		return err
	}
	if err := verifyInterruptedWorktreeTopology(ctx, gate, target, run.ID, run.HeadSHA, false); err != nil {
		return err
	}
	root, err := os.OpenRoot(m.paths.Root())
	if err != nil {
		return err
	}
	defer root.Close()
	rel, err := filepath.Rel(m.paths.Root(), dir)
	if err != nil || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("journal marker escaped the daemon root")
	}
	if err := root.Remove(rel); err != nil {
		return err
	}
	return syncRootDir(root, filepath.Dir(rel))
}

func (j *interruptedWorktreeReconstruction) markAdmitted() {
	if err := j.removeJournal(); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("failed to remove admitted interrupted-worktree journal", "run_id", j.run.ID, "error", err)
	}
}
