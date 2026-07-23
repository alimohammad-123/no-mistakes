//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package daemon

import (
	"os"
	"syscall"
)

func sameFilesystemInfo(a, b os.FileInfo) bool {
	aStat, aOK := a.Sys().(*syscall.Stat_t)
	bStat, bOK := b.Sys().(*syscall.Stat_t)
	return aOK && bOK && uint64(aStat.Dev) == uint64(bStat.Dev)
}
