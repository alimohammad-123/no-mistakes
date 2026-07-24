package sourceprovenance

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
)

// EnvironmentVariable is the runtime-owned source-ref provenance supplied to
// pipeline commands and agents.
const EnvironmentVariable = "NO_MISTAKES_SOURCE_REF"

const headsPrefix = "refs/heads/"

const recoveryAnchorPrefix = "refs/no-mistakes/recovery/"

func ExactRecoveryAnchorRef(runID string) (string, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" || strings.ContainsAny(runID, "/\\ \t\r\n") {
		return "", fmt.Errorf("exact recovery run identity is malformed")
	}
	ref := recoveryAnchorPrefix + runID + "/exact-final-head"
	if _, err := gitpkg.Run(context.Background(), ".", "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("exact recovery anchor ref is malformed: %w", err)
	}
	return ref, nil
}

func EnsureExactRecoveryAnchor(ctx context.Context, workDir, anchorRef, headSHA string) error {
	if !strings.HasPrefix(anchorRef, recoveryAnchorPrefix) || strings.TrimSpace(headSHA) == "" {
		return fmt.Errorf("exact recovery anchor identity is invalid")
	}
	if _, err := gitpkg.Run(ctx, workDir, "rev-parse", "--verify", headSHA+"^{commit}"); err != nil {
		return fmt.Errorf("verify exact recovery anchor commit: %w", err)
	}
	resolved, err := gitpkg.ResolveRef(ctx, workDir, anchorRef)
	if err == nil {
		if resolved != headSHA {
			return fmt.Errorf("exact recovery anchor %s resolves to %s, want %s", anchorRef, resolved, headSHA)
		}
		return nil
	}
	if _, err := gitpkg.Run(ctx, workDir, "update-ref", anchorRef, headSHA, "0000000000000000000000000000000000000000"); err != nil {
		return fmt.Errorf("create exact recovery anchor: %w", err)
	}
	resolved, err = gitpkg.ResolveRef(ctx, workDir, anchorRef)
	if err != nil || resolved != headSHA {
		return fmt.Errorf("verify exact recovery anchor")
	}
	return nil
}

func VerifyExactRecoveryAnchor(ctx context.Context, workDir, anchorRef, headSHA string) error {
	if !strings.HasPrefix(anchorRef, recoveryAnchorPrefix) {
		return fmt.Errorf("exact recovery anchor ref is outside the private namespace")
	}
	resolved, err := gitpkg.ResolveRef(ctx, workDir, anchorRef)
	if err != nil {
		return fmt.Errorf("resolve exact recovery anchor: %w", err)
	}
	if resolved != headSHA {
		return fmt.Errorf("exact recovery anchor %s resolves to %s, want %s", anchorRef, resolved, headSHA)
	}
	return nil
}

func RetireExactRecoveryAnchor(ctx context.Context, workDir, anchorRef, headSHA, equivalentRef string) (bool, error) {
	if err := VerifyExactRecoveryAnchor(ctx, workDir, anchorRef, headSHA); err != nil {
		return false, err
	}
	equivalent, err := gitpkg.ResolveRef(ctx, workDir, equivalentRef)
	if err != nil || equivalent != headSHA {
		return false, nil
	}
	if err := gitpkg.WithPreparedRefLock(ctx, workDir, equivalentRef, headSHA, func() error {
		if _, err := gitpkg.Run(ctx, workDir, "update-ref", "-d", anchorRef, headSHA); err != nil {
			return fmt.Errorf("retire exact recovery anchor: %w", err)
		}
		if resolved, err := gitpkg.ResolveRef(ctx, workDir, anchorRef); err == nil || resolved != "" {
			return fmt.Errorf("retire exact recovery anchor: private ref remains")
		}
		return nil
	}); err != nil {
		return false, err
	}
	return true, nil
}

func WithExactRecoveryOwnership(ctx context.Context, workDir, sourceRef, headSHA string, fn func() error) error {
	if err := VerifyCandidateBinding(ctx, workDir, sourceRef, headSHA); err != nil {
		return err
	}
	return gitpkg.WithPreparedRefLock(ctx, workDir, sourceRef, headSHA, func() error {
		return fn()
	})
}

// CanonicalSourceRefFromBranch derives the only accepted source-ref identity
// from the branch name frozen at authoritative run intake.
func CanonicalSourceRefFromBranch(branch string) (string, error) {
	if strings.HasPrefix(branch, headsPrefix) {
		short := strings.TrimPrefix(branch, headsPrefix)
		if err := validateBranch(short); err != nil {
			return "", err
		}
		return branch, nil
	}
	if err := validateBranch(branch); err != nil {
		return "", err
	}
	return headsPrefix + branch, nil
}

// ValidateFrozenSourceRef requires a full local branch ref that exactly matches
// the canonical identity derived from the run's frozen branch record.
func ValidateFrozenSourceRef(ref, branch string) error {
	canonical, err := CanonicalSourceRefFromBranch(branch)
	if err != nil {
		return err
	}
	if ref != canonical {
		return fmt.Errorf("source ref %q does not match frozen branch identity %q", ref, canonical)
	}
	return nil
}

func validateBranch(branch string) error {
	if branch == "HEAD" || branch == "(detached)" || branch == "@" || strings.HasPrefix(branch, "refs/") || strings.HasPrefix(branch, "-") {
		return fmt.Errorf("source branch %q is not a branch name", branch)
	}
	if err := gitpkg.ValidateLocalBranchName(branch); err != nil {
		return fmt.Errorf("source branch %q is malformed: %w", branch, err)
	}
	return nil
}

// BindCandidate makes the frozen source ref resolve to the exact current
// pipeline candidate. It only updates a local refs/heads ref in workDir's Git
// repository and never contacts or mutates a remote.
func BindCandidate(ctx context.Context, workDir, ref, candidateSHA string) error {
	if !strings.HasPrefix(ref, headsPrefix) {
		return fmt.Errorf("source ref %q is not a local branch ref", ref)
	}
	branch := strings.TrimPrefix(ref, headsPrefix)
	if err := ValidateFrozenSourceRef(ref, branch); err != nil {
		return err
	}
	candidateSHA = strings.TrimSpace(candidateSHA)
	if candidateSHA == "" {
		return fmt.Errorf("pipeline candidate SHA is empty")
	}
	head, err := gitpkg.HeadSHA(ctx, workDir)
	if err != nil {
		return fmt.Errorf("resolve pipeline candidate HEAD: %w", err)
	}
	if head != candidateSHA {
		return fmt.Errorf("pipeline candidate mismatch: worktree HEAD %s does not match recorded run head %s", head, candidateSHA)
	}
	if _, err := gitpkg.Run(ctx, workDir, "rev-parse", "--verify", candidateSHA+"^{commit}"); err != nil {
		return fmt.Errorf("verify pipeline candidate commit: %w", err)
	}
	if _, err := gitpkg.Run(ctx, workDir, "update-ref", ref, candidateSHA); err != nil {
		return fmt.Errorf("bind pipeline source ref: %w", err)
	}
	return VerifyCandidateBinding(ctx, workDir, ref, candidateSHA)
}

// BindCandidateIfUnchanged verifies the detached candidate and atomically
// refreshes its already-matching source ref. The expected-old argument makes a
// concurrent receive-side ref move a refusal instead of silently rewinding it.
func BindCandidateIfUnchanged(ctx context.Context, workDir, ref, candidateSHA, expectedOld string) error {
	if err := VerifyCandidateBinding(ctx, workDir, ref, expectedOld); err != nil {
		return err
	}
	head, err := gitpkg.HeadSHA(ctx, workDir)
	if err != nil {
		return fmt.Errorf("resolve pipeline candidate HEAD: %w", err)
	}
	if head != candidateSHA {
		return fmt.Errorf("pipeline candidate mismatch: worktree HEAD %s does not match recorded run head %s", head, candidateSHA)
	}
	if _, err := gitpkg.Run(ctx, workDir, "update-ref", ref, candidateSHA, expectedOld); err != nil {
		return fmt.Errorf("bind unchanged pipeline source ref: %w", err)
	}
	return VerifyCandidateBinding(ctx, workDir, ref, candidateSHA)
}

// AdvanceCandidate moves a stable pipeline source ref from the run's previous
// candidate to its new detached HEAD. A candidate already bound before a
// durable-state retry is accepted idempotently; every other ref value is a
// superseding receive-side move and is refused without being overwritten.
func AdvanceCandidate(ctx context.Context, workDir, ref, candidateSHA, previousSHA string) error {
	resolved, err := gitpkg.ResolveRef(ctx, workDir, ref)
	if err != nil {
		return fmt.Errorf("resolve pipeline source ref before advance: %w", err)
	}
	switch resolved {
	case candidateSHA:
		return BindCandidateIfUnchanged(ctx, workDir, ref, candidateSHA, candidateSHA)
	case previousSHA:
		return BindCandidateIfUnchanged(ctx, workDir, ref, candidateSHA, previousSHA)
	default:
		return fmt.Errorf("pipeline source ref binding mismatch: %s resolves to %s, want previous %s or candidate %s", ref, resolved, previousSHA, candidateSHA)
	}
}

func VerifyCandidateBinding(ctx context.Context, workDir, ref, candidateSHA string) error {
	if !strings.HasPrefix(ref, headsPrefix) {
		return fmt.Errorf("source ref %q is not a local branch ref", ref)
	}
	branch := strings.TrimPrefix(ref, headsPrefix)
	if err := ValidateFrozenSourceRef(ref, branch); err != nil {
		return err
	}
	candidateSHA = strings.TrimSpace(candidateSHA)
	if candidateSHA == "" {
		return fmt.Errorf("pipeline candidate SHA is empty")
	}
	if _, err := gitpkg.Run(ctx, workDir, "rev-parse", "--verify", candidateSHA+"^{commit}"); err != nil {
		return fmt.Errorf("verify pipeline candidate commit: %w", err)
	}
	resolved, err := gitpkg.ResolveRef(ctx, workDir, ref)
	if err != nil {
		return fmt.Errorf("verify pipeline source ref binding: %w", err)
	}
	if resolved != candidateSHA {
		return fmt.Errorf("pipeline source ref binding mismatch: %s resolves to %s, want %s", ref, resolved, candidateSHA)
	}
	return nil
}

func WithoutEnvironmentVariable(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		matches := key == EnvironmentVariable
		if runtime.GOOS == "windows" {
			matches = strings.EqualFold(key, EnvironmentVariable)
		}
		if !matches {
			out = append(out, entry)
		}
	}
	return out
}

// AuthoritativeEnv removes inherited or caller-supplied source-ref entries and
// appends the runtime-frozen value last.
func AuthoritativeEnv(env []string, ref string) []string {
	out := WithoutEnvironmentVariable(env)
	return append(out, EnvironmentVariable+"="+ref)
}
