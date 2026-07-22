package agent

import (
	"runtime"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/git"
)

// GateRoleEnvVar is exported into every spawned gate agent's environment as an
// unspoofable-from-outside marker that the process is a no-mistakes gate agent
// (a review/fix/document/test/lint/rebase/pr/ci invocation), NOT a fleet
// operator. Its purpose is containment: when the target repository is itself an
// agent-orchestration harness (for example firstmate), the target's project
// agent-instruction file can otherwise convince the gate agent it is the fleet
// captain and drive it to spawn a crew and reset the shared branch it is
// validating (see the ambient-authority incident). A cooperating harness reads
// this marker and its fleet-lifecycle entrypoints fail closed. It is deliberately
// coarse (`=1`): presence is the whole signal.
const GateRoleEnvVar = "NO_MISTAKES_GATE"

// gitSafeEnv returns the environment for a spawned agent subprocess with git
// forced into non-interactive mode. Agents shell out to git directly (for
// example `git rebase --continue` during conflict resolution), which would
// otherwise open $EDITOR and hang in the headless subprocess until the agent
// times out.
//
// It also stamps GateRoleEnvVar so a cooperating orchestration harness in the
// target repo can recognize the gate agent and refuse to let it act as a fleet
// operator. Appended last so it wins over any ambient value.
//
// dir must be the value assigned to cmd.Dir so PWD stays coupled to the working
// directory; see git.NonInteractiveEnv for why this matters.
func gitSafeEnv(dir string, extra ...[]string) []string {
	runtimeEnv := []string(nil)
	unsetEnv := []string(nil)
	if len(extra) > 0 {
		runtimeEnv = extra[0]
	}
	if len(extra) > 1 {
		unsetEnv = extra[1]
	}
	return mergeAgentEnv(git.NonInteractiveEnv(dir), append(runtimeEnv, GateRoleEnvVar+"=1"), unsetEnv)
}

// mergeAgentEnv removes inherited entries overridden by runtime-owned values
// and appends each runtime value once, in order. This keeps the final process
// environment unambiguous instead of relying on duplicate-key resolution.
func mergeAgentEnv(base, runtimeValues, unsetNames []string) []string {
	key := func(entry string) string {
		name, _, _ := strings.Cut(entry, "=")
		if runtime.GOOS == "windows" {
			return strings.ToUpper(name)
		}
		return name
	}
	overridden := make(map[string]struct{}, len(runtimeValues))
	unset := make(map[string]struct{}, len(unsetNames))
	for _, entry := range runtimeValues {
		overridden[key(entry)] = struct{}{}
	}
	for _, name := range unsetNames {
		name = key(name)
		overridden[name] = struct{}{}
		unset[name] = struct{}{}
	}
	out := make([]string, 0, len(base)+len(runtimeValues))
	for _, entry := range base {
		if _, ok := overridden[key(entry)]; !ok {
			out = append(out, entry)
		}
	}
	seen := make(map[string]struct{}, len(runtimeValues))
	for i := len(runtimeValues) - 1; i >= 0; i-- {
		name := key(runtimeValues[i])
		if _, ok := unset[name]; ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, runtimeValues[i])
	}
	for left, right := len(out)-len(seen), len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
