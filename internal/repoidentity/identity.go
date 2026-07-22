// Package repoidentity derives a credential-free, canonical repository identity
// from Git remote URLs. The identity is suitable for exact local authorization
// checks, not for network access.
package repoidentity

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"
)

const envelopePrefix = "repoid://"

// Canonical returns a versioned repository identity for a raw HTTP(S), SSH URL,
// user-qualified scp-style SSH remote, or an already-canonical identity.
func Canonical(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("repository identity is empty")
	}
	if strings.HasPrefix(value, envelopePrefix) {
		return canonicalEnvelope(value)
	}
	if strings.Contains(value, "://") {
		return canonicalURL(value)
	}
	return canonicalSCP(value)
}

func canonicalEnvelope(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse repository identity: %w", err)
	}
	if u.Scheme != "repoid" || u.Opaque != "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("repository identity envelope is invalid")
	}
	if u.RawPath != "" || strings.Contains(u.EscapedPath(), "%") {
		return "", fmt.Errorf("repository identity path must not use percent escapes")
	}
	authority, err := canonicalAuthority(u.Host, "")
	if err != nil {
		return "", err
	}
	repoPath, err := canonicalPath(strings.TrimPrefix(u.Path, "/"), false, githubAuthority(authority))
	if err != nil {
		return "", err
	}
	identity := envelopePrefix + authority + "/" + repoPath
	if identity != value {
		return "", fmt.Errorf("repository identity envelope is not canonical")
	}
	return identity, nil
}

func canonicalURL(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse repository URL: %w", err)
	}
	if u.Opaque != "" || u.Host == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("repository URL must name one unambiguous remote")
	}
	if u.RawPath != "" || strings.Contains(u.EscapedPath(), "%") {
		return "", fmt.Errorf("repository path must not use percent escapes")
	}
	defaultPort := ""
	switch strings.ToLower(u.Scheme) {
	case "https":
		defaultPort = "443"
		if u.User != nil {
			return "", fmt.Errorf("repository URL must not contain credentials")
		}
	case "http":
		defaultPort = "80"
		if u.User != nil {
			return "", fmt.Errorf("repository URL must not contain credentials")
		}
	case "ssh":
		defaultPort = "22"
		if u.User != nil {
			if _, hasPassword := u.User.Password(); hasPassword || u.User.Username() == "" {
				return "", fmt.Errorf("repository SSH URL has invalid user information")
			}
		}
	default:
		return "", fmt.Errorf("repository URL scheme %q is not supported", u.Scheme)
	}
	authority, err := canonicalAuthority(u.Host, defaultPort)
	if err != nil {
		return "", err
	}
	repoPath, err := canonicalPath(strings.TrimPrefix(u.Path, "/"), true, githubAuthority(authority))
	if err != nil {
		return "", err
	}
	return envelopePrefix + authority + "/" + repoPath, nil
}

func canonicalSCP(value string) (string, error) {
	colon := strings.IndexByte(value, ':')
	if colon <= 0 || strings.Count(value[:colon], "@") != 1 {
		return "", fmt.Errorf("repository remote must be an explicit URL or user-qualified SCP remote")
	}
	left, repoPath := value[:colon], value[colon+1:]
	at := strings.IndexByte(left, '@')
	if at <= 0 || at == len(left)-1 {
		return "", fmt.Errorf("repository SSH authority is invalid")
	}
	authority, err := canonicalAuthority(left[at+1:], "")
	if err != nil {
		return "", err
	}
	repoPath, err = canonicalPath(repoPath, true, githubAuthority(authority))
	if err != nil {
		return "", err
	}
	return envelopePrefix + authority + "/" + repoPath, nil
}

func canonicalPath(repoPath string, stripTransportSuffix, lowercase bool) (string, error) {
	if repoPath == "" || strings.HasPrefix(repoPath, "/") || strings.HasSuffix(repoPath, "/") || strings.Contains(repoPath, "//") || strings.ContainsAny(repoPath, `\%?#@`) {
		return "", fmt.Errorf("repository path is not canonical")
	}
	if stripTransportSuffix {
		repoPath = strings.TrimSuffix(repoPath, ".git")
	}
	parts := strings.Split(repoPath, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("repository path must include namespace and name")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.IndexFunc(part, unicode.IsSpace) >= 0 {
			return "", fmt.Errorf("repository path is not canonical")
		}
	}
	if lowercase {
		repoPath = strings.ToLower(repoPath)
	}
	return repoPath, nil
}

func githubAuthority(authority string) bool {
	host := authority
	if strings.HasPrefix(authority, "[") {
		close := strings.IndexByte(authority, ']')
		if close >= 0 {
			host = authority[1:close]
		}
	} else if strings.Contains(authority, ":") {
		if parsed, _, err := net.SplitHostPort(authority); err == nil {
			host = parsed
		}
	}
	return host == "github.com"
}

func canonicalAuthority(authority, defaultPort string) (string, error) {
	if authority == "" || strings.ContainsAny(authority, `/\@?#`) || strings.IndexFunc(authority, unicode.IsSpace) >= 0 {
		return "", fmt.Errorf("repository host is not canonical")
	}

	hostname := authority
	port := ""
	if strings.HasPrefix(authority, "[") {
		var err error
		hostname, port, err = splitBracketedAuthority(authority)
		if err != nil {
			return "", err
		}
		if ip := net.ParseIP(hostname); ip == nil || !strings.Contains(hostname, ":") {
			return "", fmt.Errorf("repository bracketed host must be IPv6")
		}
	} else if strings.Contains(authority, ":") {
		var err error
		hostname, port, err = net.SplitHostPort(authority)
		if err != nil || port == "" {
			return "", fmt.Errorf("repository host/port is not canonical")
		}
	}

	hostname, err := canonicalHostname(hostname)
	if err != nil {
		return "", err
	}
	if port != "" {
		if !isDecimal(port) || len(port) > 1 && port[0] == '0' {
			return "", fmt.Errorf("repository port is not canonical")
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", fmt.Errorf("repository port is invalid")
		}
		if defaultPort != "" && port == defaultPort {
			port = ""
		}
	}

	if strings.Contains(hostname, ":") {
		if port == "" {
			return "[" + hostname + "]", nil
		}
		return net.JoinHostPort(hostname, port), nil
	}
	if port != "" {
		return net.JoinHostPort(hostname, port), nil
	}
	return hostname, nil
}

func splitBracketedAuthority(authority string) (string, string, error) {
	close := strings.IndexByte(authority, ']')
	if close < 0 {
		return "", "", fmt.Errorf("repository host is not canonical")
	}
	host := authority[1:close]
	rest := authority[close+1:]
	if rest == "" {
		return host, "", nil
	}
	if !strings.HasPrefix(rest, ":") || len(rest) == 1 {
		return "", "", fmt.Errorf("repository host/port is not canonical")
	}
	return host, rest[1:], nil
}

func canonicalHostname(host string) (string, error) {
	if host == "" || host == "." || host == ".." {
		return "", fmt.Errorf("repository host is not canonical")
	}
	if ip := net.ParseIP(host); ip != nil {
		return strings.ToLower(ip.String()), nil
	}
	if strings.IndexFunc(host, func(r rune) bool { return r != '.' && (r < '0' || r > '9') }) < 0 {
		return "", fmt.Errorf("repository host is not canonical")
	}
	if strings.HasSuffix(host, "..") {
		return "", fmt.Errorf("repository host is not canonical")
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if len(host) > 253 {
		return "", fmt.Errorf("repository host is not canonical")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("repository host is not canonical")
		}
		for _, r := range label {
			if r > unicode.MaxASCII || r != '-' && (r < '0' || r > '9') && (r < 'a' || r > 'z') {
				return "", fmt.Errorf("repository host is not canonical")
			}
		}
	}
	return host, nil
}

func isDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
