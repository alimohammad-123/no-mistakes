//go:build !unix && !windows

package shellenv

import "os/exec"

func CommandLeaderCanceled(_ *exec.Cmd, _ error, _ bool) bool {
	return false
}
