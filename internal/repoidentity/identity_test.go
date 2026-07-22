package repoidentity

import "testing"

func TestCanonicalEquivalentRepositorySpellings(t *testing.T) {
	const want = "repoid://github.com/owner/repo"
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "canonical", raw: want},
		{name: "HTTPS", raw: "https://GitHub.com/Owner/Repo.git"},
		{name: "HTTPS uppercase suffix", raw: "https://github.com/Owner/Repo.GIT"},
		{name: "HTTPS default port", raw: "https://github.com:443/owner/repo.git"},
		{name: "HTTP default port", raw: "http://github.com:80/owner/repo.git"},
		{name: "DNS trailing dot", raw: "https://github.com./owner/repo.git"},
		{name: "SCP SSH", raw: "git@github.com:Owner/Repo.git"},
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
		{name: "URL", raw: "https://Git.Example.test/Group/Repo.git", want: "repoid://git.example.test/Group/Repo"},
		{name: "SCP", raw: "git@Git.Example.test:Group/Repo.git", want: "repoid://git.example.test/Group/Repo"},
		{name: "SCP numeric namespace", raw: "git@Git.Example.test:123/Repo.git", want: "repoid://git.example.test/123/Repo"},
		{name: "SCP IPv6", raw: "git@[2001:0db8::1]:Group/Repo.git", want: "repoid://[2001:db8::1]/Group/Repo"},
		{name: "HTTPS default port", raw: "https://Git.Example.test:443/Group/Repo.git", want: "repoid://git.example.test/Group/Repo"},
		{name: "HTTP default port", raw: "http://Git.Example.test:80/Group/Repo.git", want: "repoid://git.example.test/Group/Repo"},
		{name: "SSH default port", raw: "ssh://git@Git.Example.test:22/Group/Repo.git", want: "repoid://git.example.test/Group/Repo"},
		{name: "HTTPS non-default 22", raw: "https://Git.Example.test:22/Group/Repo.git", want: "repoid://git.example.test:22/Group/Repo"},
		{name: "HTTP non-default 443", raw: "http://Git.Example.test:443/Group/Repo.git", want: "repoid://git.example.test:443/Group/Repo"},
		{name: "SSH non-default 443", raw: "ssh://git@Git.Example.test:443/Group/Repo.git", want: "repoid://git.example.test:443/Group/Repo"},
		{name: "repository ending git", raw: "https://Git.Example.test/Group/Repo.git.git", want: "repoid://git.example.test/Group/Repo.git"},
		{name: "IPv4", raw: "https://192.0.2.1:8443/Group/Repo.git", want: "repoid://192.0.2.1:8443/Group/Repo"},
		{name: "IPv6", raw: "https://[2001:0db8::1]/Group/Repo.git", want: "repoid://[2001:db8::1]/Group/Repo"},
		{name: "IPv6 default port", raw: "ssh://git@[2001:0db8::1]:22/Group/Repo.git", want: "repoid://[2001:db8::1]/Group/Repo"},
		{name: "IPv6 non-default port", raw: "https://[2001:0db8::1]:22/Group/Repo.git", want: "repoid://[2001:db8::1]:22/Group/Repo"},
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
		"github.com/owner/repo",
		"github.com:123/group/repo.git",
		"file:///tmp/owner/repo",
		"git://github.com/owner/repo.git",
		"https://token@github.com/owner/repo.git",
		"https://user:token@github.com/owner/repo.git",
		"ssh://git:secret@github.com/owner/repo.git",
		"git@@github.com:owner/repo.git",
		"github.com:owner/repo.git",
		"github.com@evil.test/owner/repo",
		"https://github.com/owner//repo",
		"https://github.com/owner/../repo",
		"https://github.com/owner/repo?ref=other",
		"https://github.com/owner/%72epo",
		"https://github.com/owner/repo/",
		"https://github.com:/owner/repo",
		"https://github.com:0443/owner/repo",
		"https://github.com:65536/owner/repo",
		"https://github.com../owner/repo",
		"https://-github.com/owner/repo",
		"https://github..com/owner/repo",
		"https://192.168.001.1/owner/repo",
		"repoid://GitHub.com/owner/repo",
		"repoid://github.com/Owner/Repo",
		"repoid://github.com./owner/repo",
		"repoid://github.com:0443/owner/repo",
		"repoid://user@github.com/owner/repo",
		"repoid://github.com/owner/repo?ref=other",
		"repoid://github.com/owner/%72epo",
		"repoid://github.com/owner/../repo",
		"repoid://[2001:db8::1/owner/repo",
		"repoid://[2001:db8::1]extra/owner/repo",
		"repoid://[2001:db8::1]:/owner/repo",
		"repoid://[2001:db8::1]:022/owner/repo",
		"repoid://[fe80::1%25en0]/owner/repo",
		"repoid://[github.com]/owner/repo",
		"repoid://[192.0.2.1]/owner/repo",
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := Canonical(raw); err == nil {
				t.Fatalf("Canonical(%q) = %q, want error", raw, got)
			}
		})
	}
}
