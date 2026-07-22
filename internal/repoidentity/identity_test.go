package repoidentity

import "testing"

func TestCanonicalEquivalentRepositorySpellings(t *testing.T) {
	const want = "github.com/owner/repo"
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "canonical", raw: "github.com/owner/repo"},
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

func TestCanonicalIsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "URL", raw: "https://Git.Example.test/group/repo.git", want: "git.example.test/group/repo"},
		{name: "scheme-less", raw: "Git.Example.test/group/repo.git", want: "git.example.test/group/repo"},
		{name: "SCP", raw: "git@Git.Example.test:group/repo.git", want: "git.example.test/group/repo"},
		{name: "HTTPS default port", raw: "https://Git.Example.test:443/group/repo.git", want: "git.example.test/group/repo"},
		{name: "HTTP default port", raw: "http://Git.Example.test:80/group/repo.git", want: "git.example.test/group/repo"},
		{name: "SSH default port", raw: "ssh://git@Git.Example.test:22/group/repo.git", want: "git.example.test/group/repo"},
		{name: "HTTPS non-default 22", raw: "https://Git.Example.test:22/group/repo.git", want: "git.example.test:22/group/repo"},
		{name: "HTTP non-default 443", raw: "http://Git.Example.test:443/group/repo.git", want: "git.example.test:443/group/repo"},
		{name: "SSH non-default 443", raw: "ssh://git@Git.Example.test:443/group/repo.git", want: "git.example.test:443/group/repo"},
		{name: "scheme-less port 22", raw: "Git.Example.test:22/group/repo", want: "git.example.test:22/group/repo"},
		{name: "scheme-less port 80", raw: "Git.Example.test:80/group/repo", want: "git.example.test:80/group/repo"},
		{name: "scheme-less port 443", raw: "Git.Example.test:443/group/repo", want: "git.example.test:443/group/repo"},
		{name: "IPv4", raw: "https://192.0.2.1:8443/group/repo.git", want: "192.0.2.1:8443/group/repo"},
		{name: "IPv6", raw: "https://[2001:0db8::1]/group/repo.git", want: "[2001:db8::1]/group/repo"},
		{name: "IPv6 default port", raw: "ssh://git@[2001:0db8::1]:22/group/repo.git", want: "[2001:db8::1]/group/repo"},
		{name: "IPv6 non-default port", raw: "https://[2001:0db8::1]:22/group/repo.git", want: "[2001:db8::1]:22/group/repo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Canonical(tc.raw)
			if err != nil {
				t.Fatalf("Canonical(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("Canonical(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			roundTrip, err := Canonical(got)
			if err != nil {
				t.Fatalf("Canonical(Canonical(%q)): %v", tc.raw, err)
			}
			if roundTrip != got {
				t.Fatalf("Canonical(Canonical(%q)) = %q, want %q", tc.raw, roundTrip, got)
			}
		})
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
		"https://192.168.001.1/owner/repo",
		"[2001:db8::1/owner/repo",
		"[2001:db8::1]extra/owner/repo",
		"[2001:db8::1]:/owner/repo",
		"[2001:db8::1]:022/owner/repo",
		"[fe80::1%en0]/owner/repo",
		"[github.com]/owner/repo",
		"[192.0.2.1]/owner/repo",
		"2001:db8::1/owner/repo",
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := Canonical(raw); err == nil {
				t.Fatalf("Canonical(%q) = %q, want error", raw, got)
			}
		})
	}
}
