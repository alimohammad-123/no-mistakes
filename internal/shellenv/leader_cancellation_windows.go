//go:build windows

package shellenv

import (
	"errors"
	"os/exec"
)

func CommandLeaderCanceled(cmd *exec.Cmd, err error, leaderKillApplied bool) bool {
	if !leaderKillApplied || cmd == nil || cmd.ProcessState == nil {
		return false
	}
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 1
}
