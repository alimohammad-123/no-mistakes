//go:build windows

package daemon

import "os"

// The exact allowlisted reconstruction is macOS-only. Keep Windows builds
// compiling while making filesystem-boundary proof fail closed there.
func sameFilesystemInfo(os.FileInfo, os.FileInfo) bool { return false }
