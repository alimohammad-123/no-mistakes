package pipeline

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/sourceprovenance"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

var ErrSourceRefSuperseded = errors.New(types.RunCancelReasonSuperseded)

// StepContext provides shared resources to pipeline steps during execution.
type StepContext struct {
	Ctx              context.Context
	Run              *db.Run
	Repo             *db.Repo
	WorkDir          string
	Agent            agent.Agent
	Config           *config.Config
	DB               *db.DB
	Log              func(string) // discrete log line (newline-terminated, user-visible + file)
	LogChunk         func(string) // raw streaming chunk (user-visible + file)
	LogFile          func(string) // file-only log callback (not shown to user)
	Fixing           bool         // true when re-executing after a "fix" action
	PreviousFindings string       // JSON findings from the previous execution (set during fix loop)
	// StepResultID is the DB row ID of the current step's step_results record.
	// Steps use it to query their own round history for multi-round prompts.
	StepResultID string
	Env          []string // extra environment variables for subprocesses (used in tests)
	// UserIntent is a short, possibly-empty summary of what the change author
	// was trying to accomplish. It's surfaced in step prompts so agents have
	// context beyond the diff. Its authority depends on IntentSource: an
	// explicit `--intent` is the author's own goal statement, while an
	// inferred summary comes from a local agent transcript.
	UserIntent string
	// IntentSource records the provenance of UserIntent so steps can weigh
	// its authority. db.RunIntentSourceAgent ("agent") means the driving
	// agent supplied it explicitly via `axi run --intent` (authoritative
	// acceptance criteria); an agent name ("claude", "codex", ...) means it
	// was inferred from a transcript (a hint). Empty when no intent exists.
	IntentSource string
	// Sessions manages the run's durable review-loop agent sessions
	// (reviewer and fixer roles). nil runs every invocation cold.
	Sessions *RunSessions
	// Shared carries in-memory run-scoped results one step hands to a later
	// step in the same run (e.g. the combined document+lint pass).
	Shared *RunShared
}

type rebaseConflictAgentContextKey struct{}

func RebaseConflictAgentContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, rebaseConflictAgentContextKey{}, true)
}

func isRebaseConflictAgentContext(ctx context.Context) bool {
	marked, _ := ctx.Value(rebaseConflictAgentContextKey{}).(bool)
	return marked
}

// BaseBranch returns the immutable pipeline integration and trusted-config
// branch for this run. Historical runs without a snapshot retain their old
// behavior by falling back only to the repository's recorded remote default.
func (sctx *StepContext) BaseBranch() string {
	if sctx == nil || sctx.Run == nil {
		return ""
	}
	return sctx.Run.EffectiveBaseBranch(sctx.Repo)
}

// BindSourceRef verifies that the stable detached candidate and canonical
// source ref both still match the durable run head. The no-op compare-and-swap
// refuses a superseding receive-side ref move instead of rewinding it.
func (sctx *StepContext) BindSourceRef() (string, error) {
	if sctx == nil || sctx.Run == nil {
		return "", fmt.Errorf("pipeline source-ref context is missing")
	}
	ref, err := sctx.Run.FrozenSourceRef()
	if err != nil {
		return "", err
	}
	var recovery *db.RunRecoveryEvent
	if sctx.DB != nil {
		recovery, err = sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
		if err != nil {
			return "", err
		}
	}
	if recovery != nil && (recovery.SourceRef != ref || recovery.HeadSHA != sctx.Run.HeadSHA) {
		return "", fmt.Errorf("exact recovery source ownership identity changed")
	}
	if err := sourceprovenance.BindCandidateIfUnchanged(sctx.Ctx, sctx.WorkDir, ref, sctx.Run.HeadSHA, sctx.Run.HeadSHA); err != nil {
		if recovery != nil {
			resolved, resolveErr := git.ResolveRef(sctx.Ctx, sctx.WorkDir, ref)
			if resolveErr == nil && resolved != recovery.HeadSHA {
				return "", fmt.Errorf("%w: source ref %s resolves to %s, want %s", ErrSourceRefSuperseded, ref, resolved, recovery.HeadSHA)
			}
		}
		return "", err
	}
	return ref, nil
}

// AdvanceHeadSHA compare-and-swaps the source ref from the run's prior head to
// candidateSHA before persisting the new durable candidate. A ref already at
// candidateSHA is an idempotent database-write retry; any third value is a
// superseding push and is left untouched.
func (sctx *StepContext) AdvanceHeadSHA(candidateSHA string) error {
	return sctx.advanceHeadSHA(candidateSHA, db.HeadAdvancePipeline)
}

func (sctx *StepContext) AdvanceHeadSHAWithPushCustody(candidateSHA string) error {
	return sctx.advanceHeadSHA(candidateSHA, db.HeadAdvancePush)
}

func (sctx *StepContext) PreflightHeadMutation() error {
	if sctx == nil || sctx.Run == nil || sctx.DB == nil {
		return fmt.Errorf("pipeline head-mutation context is missing")
	}
	if sctx.Config == nil || sctx.Config.Commands.Test == "" {
		return nil
	}
	return sctx.DB.CheckHeadValidationMutationCapacity(sctx.Run.ID, maxHeadValidationReplays)
}

// CheckBoundaryHeadAssessment authorizes only the exact proved replay
// boundary. It never grants mutation capacity; callers must isolate all edits
// and accept only a verified no-op.
func (sctx *StepContext) CheckBoundaryHeadAssessment() error {
	if sctx == nil || sctx.Run == nil || sctx.DB == nil {
		return fmt.Errorf("pipeline boundary-assessment context is missing")
	}
	if sctx.Config == nil || sctx.Config.Commands.Test == "" {
		return fmt.Errorf("pipeline boundary assessment requires configured Test proof")
	}
	return sctx.DB.CheckHeadValidationBoundaryAssessmentEligibility(sctx.Run.ID, sctx.Run.HeadSHA, maxHeadValidationReplays)
}

func (sctx *StepContext) advanceHeadSHA(candidateSHA string, phase db.HeadAdvancePhase) error {
	if sctx == nil || sctx.Run == nil || sctx.DB == nil {
		return fmt.Errorf("pipeline source-ref context is missing")
	}
	previousSHA := sctx.Run.HeadSHA
	if candidateSHA == previousSHA {
		if err := sctx.DB.ValidateRunHeadAdvance(sctx.Run.ID, previousSHA, phase); err != nil {
			return err
		}
		ref, err := sctx.Run.FrozenSourceRef()
		if err != nil {
			return err
		}
		return sourceprovenance.BindCandidateIfUnchanged(sctx.Ctx, sctx.WorkDir, ref, previousSHA, previousSHA)
	}
	ref, err := sctx.Run.FrozenSourceRef()
	if err != nil {
		return err
	}
	requireValidation := sctx.Config != nil && sctx.Config.Commands.Test != "" &&
		((sctx.Run.TestHeadSHA != nil && *sctx.Run.TestHeadSHA != candidateSHA) ||
			(sctx.Run.TestHeadSHA == nil && sctx.Run.ValidationTargetSHA != nil &&
				*sctx.Run.ValidationTargetSHA == previousSHA))
	transition, err := sctx.DB.BeginRunHeadAdvance(
		sctx.Run.ID, ref, previousSHA, candidateSHA, requireValidation, maxHeadValidationReplays, phase,
	)
	if err != nil {
		return err
	}
	if err := sourceprovenance.AdvanceCandidate(sctx.Ctx, sctx.WorkDir, ref, candidateSHA, previousSHA); err != nil {
		return err
	}
	replayCount, err := sctx.DB.FinalizeRunHeadAdvance(transition, false, maxHeadValidationReplays)
	if err != nil {
		return err
	}
	sctx.Run.HeadSHA = candidateSHA
	if requireValidation {
		target := candidateSHA
		sctx.Run.ValidationTargetSHA = &target
		sctx.Run.ValidationReplayCount = replayCount
		sctx.Run.CIReadyAt = nil
		if replayCount > maxHeadValidationReplays {
			return fmt.Errorf("final-head validation did not converge after %d replay attempts", maxHeadValidationReplays)
		}
	}
	return nil
}

func RecoverRunHeadTransition(ctx context.Context, database *db.DB, run *db.Run, workDir string, steps []Step) (bool, error) {
	if database == nil || run == nil {
		return false, fmt.Errorf("recover run head transition: context is missing")
	}
	transition, err := database.GetRunHeadTransition(run.ID)
	if err != nil || transition == nil {
		return false, err
	}
	authoritativeRun, err := database.ValidateRecoverableRunHeadTransition(transition, maxHeadValidationReplays)
	if err != nil {
		return false, err
	}
	if authoritativeRun.ID != run.ID {
		return false, fmt.Errorf("recover run head transition: authoritative run identity changed")
	}
	results, err := database.GetStepsByRun(run.ID)
	if err != nil {
		return false, fmt.Errorf("recover run head transition: read topology: %w", err)
	}
	if len(results) != len(steps) {
		return false, fmt.Errorf("recover run head transition: topology has %d records for %d steps", len(results), len(steps))
	}
	activeStep := types.StepName("")
	activeIndex := -1
	testCompleted := false
	for index, result := range results {
		if result.StepName != steps[index].Name() || result.StepOrder != result.StepName.Order() {
			return false, fmt.Errorf("recover run head transition: topology changed at step %d", index)
		}
		if result.StepName == types.StepTest && result.Status == types.StepStatusCompleted {
			testCompleted = true
		}
		if result.Status == types.StepStatusRunning || result.Status == types.StepStatusFixing {
			if activeStep != "" {
				return false, fmt.Errorf("recover run head transition: topology has multiple active steps")
			}
			activeStep = result.StepName
			activeIndex = index
		}
	}
	replayRetarget := authoritativeRun.TestHeadSHA == nil
	if !testCompleted && !replayRetarget {
		return false, fmt.Errorf("recover run head transition: topology lacks completed Test evidence")
	}
	validPhase := transition.Phase == db.HeadAdvancePipeline &&
		(activeStep == types.StepDocument || activeStep == types.StepLint)
	validReplayRetarget := replayRetarget &&
		transition.Phase == db.HeadAdvancePipeline &&
		activeStep == types.StepTest
	validPushPhase := transition.Phase == db.HeadAdvancePush &&
		(activeStep == types.StepPush || activeStep == types.StepCI)
	if !validPhase && !validReplayRetarget && !validPushPhase {
		return false, fmt.Errorf("recover run head transition: phase does not match active topology")
	}
	for index, result := range results {
		if index < activeIndex && result.Status != types.StepStatusCompleted && result.Status != types.StepStatusSkipped {
			return false, fmt.Errorf("recover run head transition: predecessor %s is %s", result.StepName, result.Status)
		}
		if index > activeIndex && result.Status != types.StepStatusPending {
			return false, fmt.Errorf("recover run head transition: successor %s is %s", result.StepName, result.Status)
		}
	}
	for _, sha := range []string{transition.PreviousSHA, transition.CandidateSHA} {
		if (len(sha) != 40 && len(sha) != 64) || strings.ToLower(sha) != sha {
			return false, fmt.Errorf("recover run head transition: candidate provenance is not an exact commit")
		}
		if _, err := hex.DecodeString(sha); err != nil {
			return false, fmt.Errorf("recover run head transition: candidate provenance is not an exact commit")
		}
		resolved, err := git.Run(ctx, workDir, "rev-parse", "--verify", sha+"^{commit}")
		if err != nil || strings.TrimSpace(resolved) != sha {
			return false, fmt.Errorf("recover run head transition: candidate provenance is not an exact commit")
		}
	}
	headSHA, err := git.HeadSHA(ctx, workDir)
	if err != nil {
		return false, fmt.Errorf("recover run head transition: read worktree head: %w", err)
	}
	if headSHA != transition.CandidateSHA {
		return false, fmt.Errorf("recover run head transition: worktree head does not match candidate")
	}
	if err := sourceprovenance.AdvanceCandidate(
		ctx, workDir, transition.SourceRef, transition.CandidateSHA, transition.PreviousSHA,
	); err != nil {
		return false, fmt.Errorf("recover run head transition: %w", err)
	}
	replayCount, err := database.FinalizeRecoveredRunHeadAdvance(transition, maxHeadValidationReplays)
	if err != nil {
		return false, err
	}
	*run = *authoritativeRun
	run.HeadSHA = transition.CandidateSHA
	run.ValidationTargetSHA = transition.NextTargetSHA
	run.ValidationReplayCount = replayCount
	run.HeadAdvanceGeneration = transition.OwnershipGeneration
	run.CIReadyAt = nil
	if transition.ExpectedPushActive {
		run.PushActive = false
	}
	if replayCount > maxHeadValidationReplays {
		err := fmt.Errorf("final-head validation did not converge after %d replay attempts", maxHeadValidationReplays)
		run.Status = types.RunFailed
		errMsg := err.Error()
		run.Error = &errMsg
		return false, err
	}
	return true, nil
}

func (sctx *StepContext) AcquirePushCustody() (func(), error) {
	if sctx == nil || sctx.Run == nil || sctx.DB == nil {
		return nil, fmt.Errorf("pipeline push custody context is missing")
	}
	if sctx.Run.PushActive {
		return func() {}, nil
	}
	if err := sctx.DB.SetRunPushActive(sctx.Run.ID, true); err != nil {
		return nil, err
	}
	sctx.Run.PushActive = true
	return func() {
		_ = sctx.DB.SetRunPushActive(sctx.Run.ID, false)
		sctx.Run.PushActive = false
	}, nil
}

// ValidateDeliveryCandidate refuses remote delivery unless the canonical
// source ref still names this run's exact detached HEAD and, when configured,
// Test proof names that same candidate.
func (sctx *StepContext) ValidateDeliveryCandidate() error {
	if _, err := sctx.BindSourceRef(); err != nil {
		return fmt.Errorf("verify delivery source ref: %w", err)
	}
	return sctx.checkDeliveryProof()
}

func (sctx *StepContext) checkDeliveryProof() error {
	if sctx != nil && sctx.DB != nil && sctx.Run != nil {
		if err := sctx.DB.CheckExactRecoveryRemoteRefAmbiguity(sctx.Run.ID); err != nil {
			return err
		}
	}
	if sctx.Config != nil && sctx.Config.Commands.Test != "" {
		if err := sctx.DB.CheckHeadValidationDeliveryEligibility(
			sctx.Run.ID, sctx.Run.HeadSHA, maxHeadValidationReplays,
		); err != nil {
			return err
		}
	}
	return nil
}

func (sctx *StepContext) WithDeliverySourceOwnership(fn func() error) error {
	if sctx == nil || sctx.Run == nil || sctx.DB == nil || fn == nil {
		return fmt.Errorf("delivery source ownership context is incomplete")
	}
	event, err := sctx.DB.GetRunRecoveryEvent(sctx.Run.ID, db.RunRecoveryExactFinalHeadCapacity)
	if err != nil {
		return err
	}
	if event == nil {
		if err := sctx.ValidateDeliveryCandidate(); err != nil {
			return err
		}
		return fn()
	}
	ref, err := sctx.Run.FrozenSourceRef()
	if err != nil {
		return err
	}
	if event.DeliveryProtocol != db.ExactRecoveryDeliveryProtocol ||
		event.SourceRef != ref || event.HeadSHA != sctx.Run.HeadSHA {
		return fmt.Errorf("exact recovery delivery ownership identity changed")
	}
	if err := sourceprovenance.VerifyExactRecoveryAnchor(sctx.Ctx, sctx.WorkDir, event.AnchorRef, event.HeadSHA); err != nil {
		return err
	}
	err = sourceprovenance.WithExactRecoveryOwnership(sctx.Ctx, sctx.WorkDir, ref, event.HeadSHA, func() error {
		if err := sourceprovenance.VerifyExactRecoveryAnchor(sctx.Ctx, sctx.WorkDir, event.AnchorRef, event.HeadSHA); err != nil {
			return err
		}
		if err := sctx.checkDeliveryProof(); err != nil {
			return err
		}
		return fn()
	})
	if err != nil {
		if cause := context.Cause(sctx.Ctx); cause != nil &&
			(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, cause)) {
			return cause
		}
		resolved, resolveErr := git.ResolveRef(sctx.Ctx, sctx.WorkDir, ref)
		if resolveErr == nil && resolved != event.HeadSHA {
			return fmt.Errorf("%w: source ref %s resolves to %s, want %s", ErrSourceRefSuperseded, ref, resolved, event.HeadSHA)
		}
	}
	return err
}

// AuthoritativeEnv removes spoofed source-ref values and appends the frozen
// runtime value last.
func (sctx *StepContext) AuthoritativeEnv(env []string) ([]string, error) {
	if sctx == nil || sctx.Run == nil {
		return nil, fmt.Errorf("pipeline source-ref context is missing")
	}
	ref, err := sctx.Run.FrozenSourceRef()
	if err != nil {
		return nil, err
	}
	return sourceprovenance.AuthoritativeEnv(env, ref), nil
}

// RunAgentSession executes one turn of a durable review-loop role session,
// running cold when sessions are unavailable. Only the review step's
// reviewer/fixer turns use this; every other agent invocation goes through
// sctx.Agent.Run directly and stays session-isolated.
func (sctx *StepContext) RunAgentSession(role SessionRole, opts agent.RunOpts) (*agent.Result, error) {
	if sctx.Sessions == nil {
		return sctx.Agent.Run(sctx.Ctx, opts)
	}
	return sctx.Sessions.Run(sctx.Ctx, sctx.Agent, role, opts, sctx.Log)
}

// StepOutcome is the result of executing a pipeline step.
type StepOutcome struct {
	NeedsApproval bool // whether the step pauses for user action
	AutoFixable   bool
	Findings      string // JSON findings for TUI display (optional)
	ExitCode      int    // process exit code (0 = success)
	PRURL         string // PR/MR URL if this step created or found one
	Skipped       bool   // mark the step as skipped without failing the run
	SkipRemaining bool   // skip all subsequent steps (e.g. empty diff after rebase)
	// FixSummary, when non-empty, is the agent's one-line commit summary for
	// the fix attempt performed during this round. Steps populate it in fix
	// mode so the executor can persist it on the round record and later
	// rounds can reference what was previously attempted.
	FixSummary string

	// TestedHeadSHA is positive configured-Test evidence produced by the Test
	// step for this exact candidate. The executor persists it before any gate
	// can park; empty means no successful configured command ran this round.
	TestedHeadSHA string
	// ReplayValidation asks the executor to restart the bounded Test/Document/
	// Lint convergence phase. A mutating delivery step uses this only after it
	// has committed locally and durably advanced the run head, before network
	// publication.
	ReplayValidation bool

	// DurationOverrideMS, when positive, replaces the wall-clock duration
	// reported for this step. Used by demo mode to show realistic durations
	// without actually waiting.
	DurationOverrideMS int64
}

// Step is the interface that each pipeline step implements.
type Step interface {
	// Name returns the step's identity in the fixed pipeline sequence.
	Name() types.StepName

	// Execute runs the step logic and returns an outcome.
	// A step that returns NeedsApproval=true will pause the pipeline
	// until the user responds with an approval action.
	Execute(sctx *StepContext) (*StepOutcome, error)
}

// ApprovalGateReconciler is implemented by a step whose parked approval gate
// can become obsolete when an external source of truth changes. The executor
// invokes it with a bounded context while also waiting for an approval. A true
// result completes the step through the normal success path; false or an error
// leaves the gate parked. Implementations must be read-only and fail closed.
type ApprovalGateReconciler interface {
	ReconcileApprovalGate(sctx *StepContext) (resolved bool, err error)
}
