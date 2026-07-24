package scm_test

import (
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/scm"
	"github.com/kunchenguid/no-mistakes/internal/scm/azuredevops"
	"github.com/kunchenguid/no-mistakes/internal/scm/github"
	"github.com/kunchenguid/no-mistakes/internal/scm/gitlab"
)

func TestAdmittedRecoveryProvidersExposeAuthoritativeSnapshots(t *testing.T) {
	tests := []struct {
		name       string
		host       scm.Host
		repository string
	}{
		{name: "github", host: github.New(nil, nil, "github.com", "owner/repo"), repository: "owner/repo"},
		{name: "gitlab", host: gitlab.New(nil, nil, "gitlab.example.com", "group/repo"), repository: "group/repo"},
		{name: "azure devops", host: azuredevops.New(nil, nil, "https://dev.azure.com/org", "project", "repo"), repository: "project/repo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.host.Capabilities().RecoverySnapshot {
				t.Fatal("authoritative recovery snapshot capability is disabled")
			}
			reader, ok := tc.host.(scm.PRSnapshotReader)
			if !ok {
				t.Fatal("authoritative recovery snapshot reader is missing")
			}
			if got := reader.ExpectedRepository(); got != tc.repository {
				t.Fatalf("expected repository = %q, want %q", got, tc.repository)
			}
		})
	}
}
