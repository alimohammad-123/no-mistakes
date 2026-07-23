package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "pipeline-git-config-")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(configDir, "gitconfig")); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_NOSYSTEM", "1"); err != nil {
		panic(err)
	}
	// Agent harnesses inject git config via GIT_CONFIG_COUNT/KEY_n/VALUE_n.
	// Tests that need injected config must set it explicitly with t.Setenv.
	if err := os.Unsetenv("GIT_CONFIG_COUNT"); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}
