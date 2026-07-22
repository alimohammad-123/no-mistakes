package repoidentity

import "testing"

func TestCanonical(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "https", raw: "https://token@GitHub.com/owner/repo.git", want: "github.com/owner/repo"},
		{name: "scp ssh", raw: "git@github.com:owner/repo.git", want: "github.com/owner/repo"},
		{name: "ssh URL", raw: "ssh://git@git.example.test/group/subgroup/repo.git", want: "git.example.test/group/subgroup/repo"},
		{name: "canonical", raw: "github.com/owner/repo", want: "github.com/owner/repo"},
		{name: "self-hosted port", raw: "https://Git.Example.test:8443/group/repo.git", want: "git.example.test:8443/group/repo"},
		{name: "canonical port", raw: "git.example.test:8443/group/repo", want: "git.example.test:8443/group/repo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Canonical(tc.raw)
			if err != nil {
				t.Fatalf("Canonical(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("Canonical(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestCanonicalRejectsAmbiguousOrLocalIdentities(t *testing.T) {
	for _, raw := range []string{
		"",
		"../owner/repo",
		"/tmp/owner/repo",
		"file:///tmp/owner/repo",
		"github.com/owner//repo",
		"github.com/owner/../repo",
		"https://github.com/owner/repo?ref=other",
		"https://github.com/owner/%72epo",
		"github.com/owner/repo/",
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := Canonical(raw); err == nil {
				t.Fatalf("Canonical(%q) = %q, want error", raw, got)
			}
		})
	}
}
