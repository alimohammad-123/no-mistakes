// Package repoidentity derives a credential-free, canonical repository identity
// from Git remote URLs. The identity is suitable for exact local authorization
// checks, not for network access.
package repoidentity

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// Canonical returns a host/path repository identity. It accepts ordinary
// HTTPS, SSH URL, scp-style SSH, and already-canonical inputs. Ambiguous local
// paths, escaped paths, query strings, fragments, and path traversal are
// rejected rather than normalized.
func Canonical(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("repository identity is empty")
	}
	if strings.HasPrefix(value, "/") || strings.HasPrefix(value, "./") || strings.HasPrefix(value, "../") {
		return "", fmt.Errorf("repository identity must include a remote host")
	}

	var host, repoPath string
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil {
			return "", fmt.Errorf("parse repository URL: %w", err)
		}
		if u.Scheme == "file" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
			return "", fmt.Errorf("repository URL must name one unambiguous remote")
		}
		if strings.Contains(u.EscapedPath(), "%") {
			return "", fmt.Errorf("repository path must not use percent escapes")
		}
		host = strings.ToLower(u.Host)
		repoPath = strings.TrimPrefix(u.Path, "/")
	} else if slash, colon := strings.IndexByte(value, '/'), strings.IndexByte(value, ':'); colon >= 0 && (slash < 0 || colon < slash) && !isCanonicalPort(value, colon, slash) {
		left, right := value[:colon], value[colon+1:]
		if at := strings.LastIndexByte(left, '@'); at >= 0 {
			left = left[at+1:]
		}
		host = strings.ToLower(left)
		repoPath = right
	} else {
		slash := strings.IndexByte(value, '/')
		if slash <= 0 {
			return "", fmt.Errorf("repository identity must be host/path")
		}
		host = strings.ToLower(value[:slash])
		repoPath = value[slash+1:]
	}

	if err := validateHost(host); err != nil {
		return "", err
	}
	if repoPath == "" || strings.HasSuffix(repoPath, "/") || strings.Contains(repoPath, "//") || strings.ContainsAny(repoPath, `\%?#`) {
		return "", fmt.Errorf("repository path is not canonical")
	}
	repoPath = strings.TrimSuffix(repoPath, ".git")
	parts := strings.Split(repoPath, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("repository path must include namespace and name")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.IndexFunc(part, unicode.IsSpace) >= 0 {
			return "", fmt.Errorf("repository path is not canonical")
		}
	}
	return host + "/" + repoPath, nil
}

func isCanonicalPort(value string, colon, slash int) bool {
	if slash <= colon+1 || strings.Contains(value[:colon], "@") {
		return false
	}
	for _, r := range value[colon+1 : slash] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func validateHost(host string) error {
	if host == "" || host == "." || host == ".." || strings.ContainsAny(host, `/\@`) || strings.IndexFunc(host, unicode.IsSpace) >= 0 {
		return fmt.Errorf("repository host is not canonical")
	}
	return nil
}
