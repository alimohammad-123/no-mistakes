---
title: Daemon & Worktrees
description: Background process management, worktrees, state, and recovery.
---

The daemon is a long-running background process that manages pipeline runs. The
installer prefers setting it up as a managed background service, and
`no-mistakes`, `init`, `attach`, `rerun`, and `update` keep that service
installed and running for you when that path is available.

## Why a daemon exists

The daemon exists so `git push no-mistakes` stays fast and the gate can keep
working after your shell command returns.

- Git hands the push to the local gate repo.
- The hook notifies the daemon and exits immediately.
- The daemon owns the long-running work: worktrees, pipeline execution, TUI
  events, state, cleanup, and crash recovery.

```mermaid
flowchart LR
  push["git push no-mistakes"] --> gate["Gate repo hook"] --> daemon["Daemon"]
  daemon --> run["Run in detached worktree"]
  daemon --> state["Persist state + logs"]
  run --> tui["TUI can attach or detach"]
  run --> cleanup["Cleanup when run finishes"]
```

On macOS this is a per-user `launchd` agent, on Linux a per-user `systemd` service, and on Windows a Task Scheduler task. The installed artifact names are scoped by `NM_HOME` with a short stable suffix, so the paths and service identifiers look like `~/Library/LaunchAgents/com.kunchenguid.no-mistakes.daemon.<suffix>.plist`, `~/.config/systemd/user/no-mistakes-daemon-<suffix>.service`, and the Windows task `no-mistakes-daemon-<suffix>`. That keeps multiple `no-mistakes` installs from colliding when they use different `NM_HOME` roots. Those service managers keep the daemon available across CLI invocations and restart it after `no-mistakes update` replaces the binary. A managed service starts with a minimal environment, so at daemon startup it resolves `PATH` and proxy variables from your login shell and the baked-in service definition; [Environment the daemon sees](/no-mistakes/reference/environment/#environment-the-daemon-sees) owns that resolution story. Restart the daemon after changing those values. If managed service install or startup is unavailable or fails, `no-mistakes` falls back to starting a detached daemon process instead.

## Starting and stopping

Most people do not need to manage the daemon directly. The usual commands
already make sure it exists when needed.

```sh
# Explicit management
no-mistakes daemon start
no-mistakes daemon stop
no-mistakes daemon restart
no-mistakes daemon status

# Ensures the daemon is running, using the managed service when possible
no-mistakes
no-mistakes init
no-mistakes attach
no-mistakes rerun
no-mistakes axi run
no-mistakes axi respond

# Resets the daemon after replacing the binary
no-mistakes update
```

`no-mistakes update` stops and starts the daemon when it is running, or when stale daemon artifacts exist, so the new executable is used.
It prefers the managed service path and falls back to a detached daemon if service startup is unavailable or fails.
If pending or running pipeline runs exist, `update` refuses to restart the daemon by default and prints each active run's ID, status, branch, and short head SHA. Pass `--force` to restart the daemon anyway and accept that those runs may fail; `-y`/`--yes` does not bypass this guard.
If the daemon is already running from a different executable path, update still prompts before replacing it; `-y`/`--yes` answers that prompt non-interactively.
If the daemon executable path cannot be determined, the update aborts before replacing anything.

`no-mistakes daemon stop` and `no-mistakes daemon restart` apply the same guard: if pending or running pipeline runs exist, each refuses by default and lists the active runs, and each takes its own `--force` to proceed anyway.
Every invocation of `daemon stop`, `daemon restart`, or `update` - forced or not - logs the caller's PID, parent PID, and parent command line to `~/.no-mistakes/logs/cli.log` so a later incident can identify which agent or process triggered it.

The daemon writes an identity record to `~/.no-mistakes/daemon.pid` and listens on a Unix socket at `~/.no-mistakes/socket`. On Windows, it uses a localhost TCP listener and a protected endpoint file at the same path. CLI clients bound how long they wait for that socket to accept a connection with `daemon_connect_timeout` (default `3s`, override with `NM_DAEMON_CONNECT_TIMEOUT`), so a daemon process that is alive but stuck fails the connection instead of hanging the caller; see [Troubleshooting](/no-mistakes/guides/troubleshooting/#check-for-stale-artifacts).
Commands that ensure the daemon is running (`no-mistakes`, `init`, `attach`, `rerun`, `axi run`, `axi respond`) also fail fast rather than silently starting a replacement daemon when the socket file exists but nothing answers at all, such as a dead socket left behind by an unclean exit; `no-mistakes daemon start` self-heals past that case.

Only one live daemon can own an `NM_HOME` at a time.
At startup - before crash recovery runs and before the socket is bound - the daemon takes an exclusive OS file lock on `~/.no-mistakes/daemon.lock` and holds it for the life of the process.
A second daemon started against the same root fails with "a no-mistakes daemon is already running for this NM_HOME" (with the holder's PID and start time when available) instead of stealing the first daemon's socket and running crash recovery against its live runs.
The OS releases the lock automatically when the owning process exits or crashes, even on SIGKILL, so unlike the PID file the lock can never go stale.
As an independent safety layer, the daemon also refuses to bind the Unix socket while something is still answering on it; only a provably stale socket file (nothing listening) is removed and rebound.

## What it does

When a push arrives via the post-receive hook:

1. Creates a detached worktree at `~/.no-mistakes/worktrees/<repoID>/<runID>/`
2. Starts the pipeline executor in that worktree
3. Streams events to any connected TUI clients and serves request/response state to AXI clients
4. Cleans up the worktree when the run finishes (success or failure)

Pipeline agents are prompted to keep intentional writes inside that detached worktree and avoid changing system state outside it, such as Homebrew packages, apps under `/Applications`, or global tool configuration.
That reduces surprising machine-level side effects and macOS App Management prompts, but it is prompt steering rather than a true sandbox.
While executing steps, the daemon also owns child-process cleanup.
Configured commands and one-shot agent subprocesses are terminated as a process tree on completion, failure, or cancellation so leaked test workers, build watchers, or dev servers cannot accumulate across runs.

## Concurrent push handling

If you push to the same branch while a run is already active, the daemon:

1. Cancels the in-progress run (reason: "cancelled: superseded by new push")
2. Waits for it to finish
3. Starts a new run with the latest push

Pushes to different branches run concurrently.

This is another reason the daemon exists: branch-level coordination is easier to
reason about in one long-lived process than inside independent hook invocations.

## Crash recovery

On startup, the daemon checks for runs that were left in `pending` or `running` status (which means the daemon crashed while they were active):

- Resumes only fully recorded parked approval gates whose worktree and step history can be validated; incomplete or ambiguous active runs fail closed
- Resumes narrowly eligible active final-head validation runs whose clean worktree, fixed step topology, source ref, trusted configured Test command, and delivery provenance still agree. An interrupted persisted replay continues from Test without resetting its three-head budget. A legacy running-CI run with coherent push provenance and the same stored PR identity but no exact Test proof keeps its run ID and replays Test, Document, and Lint before the ordinary Push, PR, and CI steps can reuse that delivery state. Missing or ambiguous evidence fails closed instead of being inferred or written into terminal history.
- Reloads trusted repository policy from the run's frozen pipeline base, not the repo's current registration, so re-init cannot switch trust roots or integration targets underneath a parked run or its reused reviewer/fixer sessions
- Restores an authorized first-policy Test command only from the complete run snapshot, never from later global bootstrap edits; durable repository/base retirement makes recovery refuse after trusted base policy has ever been observed
- Before resuming a parked CI gate, re-checks its persisted PR URL through the configured provider; a currently merged or closed PR completes the stale gate, while an open, unknown, or unreachable PR remains parked
- Marks every other stale active run as `failed` with the message "daemon crashed during execution"
- Reaps orphaned managed agent servers left behind by a crashed daemon or setup wizard
- Removes orphaned worktree directories via `git worktree remove --force` - but never one whose run is still `pending` or `running`; only leftovers from terminal runs or directories with no matching run record are removed
- Refreshes legacy no-mistakes-managed `post-receive` hooks, installs missing managed hooks, and leaves custom hooks untouched
- Reapplies per-worktree gate hook-path isolation to existing bare repos when Git supports `config --worktree`, so shared `core.hookspath` writes cannot disable `post-receive`
- Enables Git push-option support on existing gate repos so per-push options like `no-mistakes.skip=...` keep working after upgrades
- Clears any parked-awaiting-agent marker so a recovered failed run is not shown as still waiting for `axi respond`

### Exact final-head capacity recovery

A historical run can fail in Document at the three-target replay boundary even after the exact candidate passed configured Test. When AXI reports that exact capacity error, an operator may request the guarded same-run transition explicitly:

```sh
no-mistakes axi recover-final-head --run <id>
```

This path accepts only the precise terminal footprint. The run must still have matching head, Test proof, active validation target, replay count, source ref, gate ref, earlier push binding, open PR identity, Test and Document step history, and pending delivery suffix. It refuses a pending head transition, active push, returned custody, changed remote branch or PR, missing proof, unrelated failure, delivered head, or prior recovery event.

The daemon may reconstruct only the missing canonical detached worktree at the exact gate commit. It reloads trusted policy, verifies the clean worktree and source binding, checks the remote branch and PR twice around an evidence-token-bound database claim, and never moves the operator worktree. Before reviving the run, the transaction appends a durable recovery event that preserves the prior failed status, errors, exact proof, source ref, PR, published head, and Document identity. The event makes the exceptional transition auditable and prevents another terminal revival.

The resumed executor starts at Document without rerunning or weakening the exact Test proof. Document uses an isolated disposable clone at the boundary and can continue only when that assessment is a no-op. Any proposed change fails before a commit or source-ref advance. A daemon shutdown during the resumed suffix keeps the recovery event, proof, run ID, and worktree for ordinary active-run recovery. Delivery recovery rechecks the authoritative remote ref, recognizes an exact durable Push binding without publishing again, and journals the PR's prior and intended content before mutation so restart can distinguish a safe retry from an already-applied update or ambiguous external drift.

### Interrupted approval compatibility

Older daemons could turn a run that was waiting for approval into a failed run
with the exact error `daemon shutting down`. The upgraded runtime has one
bounded compatibility path for that legacy state. From the unchanged submitted
branch, run the ordinary command again with the same authoritative intent:

```sh
no-mistakes axi run --intent "<the same goal supplied to the interrupted run>"
```

AXI restores the same run ID and returns its preserved approval gate. Continue
with `no-mistakes axi respond` as usual. It does not create a replacement run,
rerun Review or Test, return custody, move the operator branch, push, or
otherwise mutate a remote. It may fetch the frozen pipeline base to reload
trusted repository policy. The pipeline-local `refs/heads/<branch>` ref is
derived from the durable branch identity and bound to the recorded pipeline
head before execution can continue.

Recovery is allowed only for the exact legacy shutdown error with one preserved
failed gate, a nonempty matching findings round, completed earlier steps, and
pristine pending later steps. The run must be the newest run for the repository
and branch, have the same submitted head and authoritative intent, and have no
push, pull request, CI, or custody-return provenance. Ordinarily it must also
retain a clean registered pipeline worktree whose recorded head is available in
the local gate. Dirty worktrees, head or branch mismatches, malformed or empty
findings, ambiguous step history, genuine command failures, and any
external-delivery provenance are refused without changing the failed run.

There is one compile-time allowlisted exception for a known Arena Test-gate
incident whose registered pipeline worktree is genuinely absent. This is not a
general repair command or fallback. The ordinary `axi run` path reconstructs
that exact run's detached worktree at its canonical path and exact stored
pipeline commit only after its fixed repository, run, branch, base, submitted
head, pipeline head, intent digest, Test step, shutdown error, Git topology,
paths, process ownership, parent remote branch, all-state pull requests,
delivery state, and complete durable database evidence all match. Any mismatch,
unreachable commit, existing replacement, ambiguous Git administration, live
owner, or changed evidence is refused without mutation.

The exception records a private ownership journal under
`$NM_HOME/recovery/interrupted-worktrees/` before creating the linked worktree,
copies commit identity from the registered source worktree into per-worktree
configuration, repeats mutable checks before an evidence-token-bound database
claim, and removes the journal only after executor admission. Before that claim,
failure rolls back only journal-owned artifacts. After the claim, failure marks
the same run failed and rolls back owned artifacts. Daemon startup reconciles a
partial journal before ordinary stale-run and worktree cleanup, while a fully
claimed parked run resumes through the existing crash-recovery path.

Trusted repository policy is reloaded from the run's frozen pipeline base before
the restore transaction. Existing reviewer and fixer session metadata must
still match the configured provider. The transaction clears only the legacy
shutdown errors, restores the interrupted step to `awaiting_approval` or
`fix_review`, freezes a missing canonical source ref, and starts a new parked
time interval. Completed steps, findings, rounds, execution timing, sessions,
and pipeline commits remain in place. A later daemon restart uses the ordinary
parked-run crash recovery path, so reattaching again returns the same gate
without duplicate rounds, agent turns, or fix commits.

## Logging

Daemon logs go to `~/.no-mistakes/logs/daemon.log`. The setup wizard captures managed agent-server output in `~/.no-mistakes/logs/wizard-agent.log`. Each pipeline step also writes to its own log at `~/.no-mistakes/logs/<runID>/<step>.log`, and fatal step errors are appended there so the step log includes the failure reason even when the detail comes from command stderr. `daemon stop`, `daemon restart`, and `update` invocations are logged separately to `~/.no-mistakes/logs/cli.log` with the caller's PID, parent PID, and parent command line.

Set the log level in global config:

```yaml
log_level: debug # debug | info | warn | error
```

## Shutdown

`no-mistakes daemon stop` stops the current daemon process without removing the managed service. The next `no-mistakes daemon start`, `no-mistakes`, `init`, `attach`, `rerun`, or `update` will start it again through the same service manager when available, or as a detached daemon otherwise.
It refuses by default while pending or running pipeline runs exist for this `NM_HOME`; pass `--force` to stop anyway and accept that those runs may fail.

1. Cancels all active runs
2. Waits up to 30 seconds for goroutines to finish
3. Removes the PID file and socket
