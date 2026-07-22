package db

import (
	"errors"
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
		Repository:   "github.com/owner/repo",
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
	value := "github.com/owner/repo"
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
	if retired, err := d.IsBootstrapTestRetired("github.com/owner/repo", "staging"); err != nil || retired {
		t.Fatalf("initial retirement = %v, err=%v", retired, err)
	}
	if err := d.RetireBootstrapTest("github.com/owner/repo", "staging"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		repository string
		base       string
		want       bool
	}{
		{repository: "github.com/owner/repo", base: "staging", want: true},
		{repository: "github.com/owner/repo", base: "main", want: false},
		{repository: "github.com/other/repo", base: "staging", want: false},
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
	if retired, err := d.IsBootstrapTestRetired("github.com/owner/repo", "staging"); err != nil || !retired {
		t.Fatalf("reopened retirement = %v, err=%v", retired, err)
	}
}

func TestBootstrapTestRetirementUsesCanonicalRepositoryIdentity(t *testing.T) {
	d := openTestDB(t)
	canonical, err := repoidentity.Canonical("https://github.com/owner/repo.git")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RetireBootstrapTest(canonical, "main"); err != nil {
		t.Fatal(err)
	}
	for _, remote := range []string{
		"github.com/owner/repo",
		"https://github.com:443/owner/repo.git",
		"https://github.com./owner/repo.git",
		"git@github.com:owner/repo.git",
		"ssh://git@github.com:22/owner/repo.git",
	} {
		identity, err := repoidentity.Canonical(remote)
		if err != nil {
			t.Fatalf("Canonical(%q): %v", remote, err)
		}
		retired, err := d.IsBootstrapTestRetired(identity, "main")
		if err != nil || !retired {
			t.Fatalf("retirement via %q = %v, err=%v", remote, retired, err)
		}
	}

	distinct, err := repoidentity.Canonical("https://github.com:8443/owner/repo.git")
	if err != nil {
		t.Fatal(err)
	}
	if retired, err := d.IsBootstrapTestRetired(distinct, "main"); err != nil || retired {
		t.Fatalf("non-default-port retirement = %v, err=%v", retired, err)
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
		Repository: "github.com/owner/repo", BaseBranch: "staging", Command: "go test ./...", PolicySHA256: strings.Repeat("a", 64),
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
	if retired, err := d.IsBootstrapTestRetired("github.com/owner/repo", "staging"); err == nil || retired {
		t.Fatalf("retirement lookup did not fail closed: retired=%v err=%v", retired, err)
	}
	if err := d.RetireBootstrapTest("github.com/owner/repo", "staging"); err == nil {
		t.Fatal("retirement persistence error was ignored")
	}
}
