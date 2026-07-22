package safeurl

import (
	"strings"
	"testing"
)

func TestRedactHidesHTTPSCredentials(t *testing.T) {
	got := Redact("https://user:token@example.com/owner/repo.git")
	if got != "https://redacted@example.com/owner/repo.git" {
		t.Fatalf("Redact() = %q, want credentials hidden", got)
	}
}

func TestRedactLeavesCredentialFreeValues(t *testing.T) {
	for _, input := range []string{
		"https://github.com/owner/repo.git",
		"ssh://git@git.example.test/owner/repo.git",
		"git@github.com:owner/repo.git",
		"/tmp/repo.git",
	} {
		if got := Redact(input); got != input {
			t.Fatalf("Redact(%q) = %q, want unchanged", input, got)
		}
	}
}

func TestRedactHidesSSHPasswords(t *testing.T) {
	got := Redact("ssh://user:token@git.example.test/owner/repo.git")
	if got != "ssh://redacted@git.example.test/owner/repo.git" {
		t.Fatalf("Redact() = %q, want password hidden", got)
	}
}

func TestRedactTextHidesCredentialURLsInsideMessages(t *testing.T) {
	input := "fatal: unable to access 'https://user:token@example.com/owner/repo.git': rejected"
	got := RedactText(input)
	if strings.Contains(got, "token") {
		t.Fatalf("RedactText() leaked credential: %q", got)
	}
	if !strings.Contains(got, "https://redacted@example.com/owner/repo.git") {
		t.Fatalf("RedactText() = %q, want redacted URL", got)
	}
}
