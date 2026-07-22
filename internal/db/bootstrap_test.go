package db

import (
	"strings"
	"testing"
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
