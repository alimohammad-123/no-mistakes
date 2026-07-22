package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
)

func TestRunBootstrapTestAuthorizationIsFrozenAndRoundTrips(t *testing.T) {
	d := openTestDB(t)
	repo, err := d.InsertRepoWithIDAndForkAndBase("bootstrap-repo", "/tmp/bootstrap-repo", "https://github.com/owner/repo.git", "", "main", "staging")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRunWithBaseBranch(repo.ID, "feature/policy", "head", "base", "staging")
	if err != nil {
		t.Fatal(err)
	}
	auth := BootstrapTestAuthorization{
		Repository:   "repoid://github.com/owner/repo",
		BaseBranch:   "staging",
		Command:      "go test ./...",
		PolicySHA256: strings.Repeat("a", 64),
	}
	if err := d.SetRunBootstrapTestAuthorization(run.ID, auth); err != nil {
		t.Fatalf("SetRunBootstrapTestAuthorization: %v", err)
	}

	persisted, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := persisted.FrozenBootstrapTestAuthorization()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != auth {
		t.Fatalf("authorization = %+v, want %+v", got, auth)
	}

	auth.Command = "go test ./internal/..."
	if err := d.SetRunBootstrapTestAuthorization(run.ID, auth); err == nil {
		t.Fatal("second authorization update succeeded; snapshot is mutable")
	}
	persisted, err = d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err = persisted.FrozenBootstrapTestAuthorization()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Command != "go test ./..." {
		t.Fatalf("frozen command = %+v, want original", got)
	}
}

func TestRunBootstrapTestAuthorizationRejectsPartialSnapshot(t *testing.T) {
	run := &Run{}
	value := "repoid://github.com/owner/repo"
	run.BootstrapTestRepository = &value
	if _, err := run.FrozenBootstrapTestAuthorization(); err == nil {
		t.Fatal("partial bootstrap snapshot was accepted")
	}
}

func TestBootstrapTestRetirementPersistsAcrossReopenAndUsesExactKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retirement.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if retired, err := d.IsBootstrapTestRetired("repoid://github.com/owner/repo", "staging"); err != nil || retired {
		t.Fatalf("initial retirement = %v, err=%v", retired, err)
	}
	if err := d.RetireBootstrapTest("repoid://github.com/owner/repo", "staging"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		repository string
		base       string
		want       bool
	}{
		{repository: "repoid://github.com/owner/repo", base: "staging", want: true},
		{repository: "repoid://github.com/owner/repo", base: "main", want: false},
		{repository: "repoid://github.com/other/repo", base: "staging", want: false},
	} {
		retired, err := d.IsBootstrapTestRetired(tc.repository, tc.base)
		if err != nil || retired != tc.want {
			t.Fatalf("retirement %s/%s = %v, err=%v, want %v", tc.repository, tc.base, retired, err, tc.want)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if retired, err := d.IsBootstrapTestRetired("repoid://github.com/owner/repo", "staging"); err != nil || !retired {
		t.Fatalf("reopened retirement = %v, err=%v", retired, err)
	}
}

func TestBootstrapTestRetirementUsesCanonicalRepositoryIdentity(t *testing.T) {
	d := openTestDB(t)
	for i, tc := range []struct {
		name    string
		remotes []string
		want    string
	}{
		{
			name: "URL canonical SCP and defaults",
			remotes: []string{
				"repoid://github.com/owner/repo",
				"https://github.com/Owner/Repo.git",
				"https://github.com/Owner/Repo.GIT",
				"https://github.com:443/owner/repo.git",
				"https://github.com./owner/repo.git",
				"git@github.com:Owner/Repo.git",
				"ssh://git@github.com:22/owner/repo.git",
			},
			want: "repoid://github.com/owner/repo",
		},
		{
			name:    "non-default scheme port",
			remotes: []string{"https://github.com:22/owner/repo.git", "repoid://github.com:22/owner/repo"},
			want:    "repoid://github.com:22/owner/repo",
		},
		{
			name:    "IPv4",
			remotes: []string{"https://192.0.2.1:8443/owner/repo.git", "repoid://192.0.2.1:8443/owner/repo"},
			want:    "repoid://192.0.2.1:8443/owner/repo",
		},
		{
			name:    "bracketed IPv6",
			remotes: []string{"https://[2001:0db8::1]:8443/owner/repo.git", "repoid://[2001:db8::1]:8443/owner/repo"},
			want:    "repoid://[2001:db8::1]:8443/owner/repo",
		},
		{
			name:    "bracketed IPv6 SCP",
			remotes: []string{"git@[2001:0db8::1]:owner/repo.git", "repoid://[2001:db8::1]/owner/repo"},
			want:    "repoid://[2001:db8::1]/owner/repo",
		},
		{
			name:    "repository ending git",
			remotes: []string{"https://git.example.test/Group/Repo.git.git", "repoid://git.example.test/Group/Repo.git"},
			want:    "repoid://git.example.test/Group/Repo.git",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fmt.Sprintf("base-%d", i)
			if err := d.RetireBootstrapTest(tc.want, base); err != nil {
				t.Fatal(err)
			}
			for _, remote := range tc.remotes {
				identity, err := repoidentity.Canonical(remote)
				if err != nil {
					t.Fatalf("Canonical(%q): %v", remote, err)
				}
				if identity != tc.want {
					t.Fatalf("Canonical(%q) = %q, want %q", remote, identity, tc.want)
				}
				retired, err := d.IsBootstrapTestRetired(identity, base)
				if err != nil || !retired {
					t.Fatalf("retirement via %q = %v, err=%v", remote, retired, err)
				}
			}
		})
	}

	if retired, err := d.IsBootstrapTestRetired("repoid://github.com:8443/owner/repo", "base-0"); err != nil || retired {
		t.Fatalf("distinct non-default-port retirement = %v, err=%v", retired, err)
	}
}

func TestSetRunBootstrapTestAuthorizationRefusesRetiredKey(t *testing.T) {
	d := openTestDB(t)
	repo, err := d.InsertRepoWithIDAndForkAndBase("bootstrap-retired", "/tmp/bootstrap-retired", "https://github.com/owner/repo.git", "", "main", "staging")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRunWithBaseBranch(repo.ID, "feature/policy", "head", "base", "staging")
	if err != nil {
		t.Fatal(err)
	}
	auth := BootstrapTestAuthorization{
		Repository: "repoid://github.com/owner/repo", BaseBranch: "staging", Command: "go test ./...", PolicySHA256: strings.Repeat("a", 64),
	}
	if err := d.RetireBootstrapTest(auth.Repository, auth.BaseBranch); err != nil {
		t.Fatal(err)
	}
	if err := d.SetRunBootstrapTestAuthorization(run.ID, auth); !errors.Is(err, ErrBootstrapTestRetired) {
		t.Fatalf("authorization error = %v, want ErrBootstrapTestRetired", err)
	}
	persisted, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if frozen, err := persisted.FrozenBootstrapTestAuthorization(); err != nil || frozen != nil {
		t.Fatalf("retired run authorization = %+v, err=%v", frozen, err)
	}
}

func TestBootstrapTestRetirementStorageErrorsFailClosed(t *testing.T) {
	d := openTestDB(t)
	if _, err := d.sql.Exec(`DROP TABLE bootstrap_test_retirements`); err != nil {
		t.Fatal(err)
	}
	if retired, err := d.IsBootstrapTestRetired("repoid://github.com/owner/repo", "staging"); err == nil || retired {
		t.Fatalf("retirement lookup did not fail closed: retired=%v err=%v", retired, err)
	}
	if err := d.RetireBootstrapTest("repoid://github.com/owner/repo", "staging"); err == nil {
		t.Fatal("retirement persistence error was ignored")
	}
}
