package repoidentity

import "testing"

func TestCanonicalEquivalentRepositorySpellings(t *testing.T) {
	const want = "github.com/owner/repo"
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "canonical", raw: "github.com/owner/repo"},
		{name: "canonical default HTTPS port", raw: "github.com:443/owner/repo"},
		{name: "canonical default HTTP port", raw: "github.com:80/owner/repo"},
		{name: "canonical default SSH port", raw: "github.com:22/owner/repo"},
		{name: "HTTPS", raw: "https://GitHub.com/owner/repo.git"},
		{name: "HTTPS default port", raw: "https://github.com:443/owner/repo.git"},
		{name: "HTTP default port", raw: "http://github.com:80/owner/repo.git"},
		{name: "DNS trailing dot", raw: "https://github.com./owner/repo.git"},
		{name: "scp SSH", raw: "git@github.com:owner/repo.git"},
		{name: "SSH URL", raw: "ssh://git@github.com/owner/repo.git"},
		{name: "SSH default port", raw: "ssh://git@github.com:22/owner/repo.git"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Canonical(tc.raw)
			if err != nil {
				t.Fatalf("Canonical(%q): %v", tc.raw, err)
			}
			if got != want {
				t.Fatalf("Canonical(%q) = %q, want %q", tc.raw, got, want)
			}
		})
	}
}

func TestCanonicalPreservesNormalizedNonDefaultPorts(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "https://Git.Example.test:8443/group/repo.git", want: "git.example.test:8443/group/repo"},
		{raw: "ssh://git@Git.Example.test:2222/group/repo.git", want: "git.example.test:2222/group/repo"},
		{raw: "git.example.test:8443/group/repo", want: "git.example.test:8443/group/repo"},
	} {
		got, err := Canonical(tc.raw)
		if err != nil {
			t.Fatalf("Canonical(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("Canonical(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestCanonicalRejectsAmbiguousCredentialBearingOrNoncanonicalIdentities(t *testing.T) {
	for _, raw := range []string{
		"",
		"../owner/repo",
		"/tmp/owner/repo",
		"file:///tmp/owner/repo",
		"git://github.com/owner/repo.git",
		"https://token@github.com/owner/repo.git",
		"https://user:token@github.com/owner/repo.git",
		"ssh://git:secret@github.com/owner/repo.git",
		"git@@github.com:owner/repo.git",
		"github.com@evil.test/owner/repo",
		"github.com/owner//repo",
		"github.com/owner/../repo",
		"https://github.com/owner/repo?ref=other",
		"https://github.com/owner/%72epo",
		"github.com/owner/repo/",
		"https://github.com:/owner/repo",
		"https://github.com:0443/owner/repo",
		"https://github.com:65536/owner/repo",
		"https://github.com../owner/repo",
		"https://-github.com/owner/repo",
		"https://github..com/owner/repo",
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := Canonical(raw); err == nil {
				t.Fatalf("Canonical(%q) = %q, want error", raw, got)
			}
		})
	}
}
