package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadGlobalBootstrapTestBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := `bootstrap:
  test:
    - repository: github.com/owner/repo
      base_branch: staging
      command: go test ./...
      policy_sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadGlobal(path)
	if err != nil {
		t.Fatalf("LoadGlobal: %v", err)
	}
	if len(cfg.Bootstrap.Test) != 1 {
		t.Fatalf("bootstrap.test count = %d, want 1", len(cfg.Bootstrap.Test))
	}
	got := cfg.Bootstrap.Test[0]
	if got.Repository != "github.com/owner/repo" || got.BaseBranch != "staging" || got.Command != "go test ./..." || got.PolicySHA256 != "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" {
		t.Fatalf("bootstrap binding = %+v", got)
	}
}

func TestLoadGlobalBootstrapTestBindingRefusesAmbiguousDuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	binding := `    - repository: github.com/owner/repo
      base_branch: staging
      command: go test ./...
      policy_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`
	if err := os.WriteFile(path, []byte("bootstrap:\n  test:\n"+binding+binding), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGlobal(path); err == nil {
		t.Fatal("LoadGlobal accepted ambiguous duplicate bootstrap bindings")
	}
}

func TestLoadGlobalBootstrapTestBindingRefusesMissingAndMalformedFields(t *testing.T) {
	validDigest := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "missing repository", body: "base_branch: staging\n      command: go test ./...\n      policy_sha256: " + validDigest},
		{name: "missing base", body: "repository: github.com/owner/repo\n      command: go test ./...\n      policy_sha256: " + validDigest},
		{name: "missing command", body: "repository: github.com/owner/repo\n      base_branch: staging\n      policy_sha256: " + validDigest},
		{name: "missing digest", body: "repository: github.com/owner/repo\n      base_branch: staging\n      command: go test ./..."},
		{name: "noncanonical repository", body: "repository: https://github.com/owner/repo.git\n      base_branch: staging\n      command: go test ./...\n      policy_sha256: " + validDigest},
		{name: "full ref base", body: "repository: github.com/owner/repo\n      base_branch: refs/heads/staging\n      command: go test ./...\n      policy_sha256: " + validDigest},
		{name: "whitespace command", body: "repository: github.com/owner/repo\n      base_branch: staging\n      command: ' go test ./...'\n      policy_sha256: " + validDigest},
		{name: "malformed digest", body: "repository: github.com/owner/repo\n      base_branch: staging\n      command: go test ./...\n      policy_sha256: xyz"},
		{name: "uppercase digest", body: "repository: github.com/owner/repo\n      base_branch: staging\n      command: go test ./...\n      policy_sha256: " + strings.Repeat("A", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := "bootstrap:\n  test:\n    - " + tc.body + "\n"
			if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGlobal(path); err == nil {
				t.Fatal("LoadGlobal accepted incomplete or malformed bootstrap binding")
			}
		})
	}
}
