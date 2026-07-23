//go:build unix

package shellenv

import (
	"errors"
	"os/exec"
	"syscall"
)

func CommandLeaderCanceled(cmd *exec.Cmd, err error, leaderKillApplied bool) bool {
	if !leaderKillApplied || cmd == nil || cmd.ProcessState == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}
