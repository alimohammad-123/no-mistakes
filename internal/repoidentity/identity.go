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

// Canonical returns a host/path repository identity. It accepts ordinary
// HTTP(S), SSH URL, scp-style SSH, and already-canonical inputs. Ambiguous local
// paths, credentials, escaped paths, query strings, fragments, and path
// traversal are rejected rather than normalized.
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
		if u.Opaque != "" || u.Host == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return "", fmt.Errorf("repository URL must name one unambiguous remote")
		}
		if u.RawPath != "" || strings.Contains(u.EscapedPath(), "%") {
			return "", fmt.Errorf("repository path must not use percent escapes")
		}
		scheme := strings.ToLower(u.Scheme)
		defaultPort := ""
		switch scheme {
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
		host, err = canonicalAuthority(u.Host, defaultPort)
		if err != nil {
			return "", err
		}
		repoPath = strings.TrimPrefix(u.Path, "/")
	} else if strings.HasPrefix(value, "[") {
		slash := strings.IndexByte(value, '/')
		if slash <= 0 {
			return "", fmt.Errorf("repository identity must be host/path")
		}
		var err error
		host, err = canonicalAuthority(value[:slash], "")
		if err != nil {
			return "", err
		}
		repoPath = value[slash+1:]
	} else if scpHost, scpPath, ok, err := splitSCP(value); err != nil {
		return "", err
	} else if ok {
		var authorityErr error
		host, authorityErr = canonicalAuthority(scpHost, "")
		if authorityErr != nil {
			return "", authorityErr
		}
		repoPath = scpPath
	} else {
		slash := strings.IndexByte(value, '/')
		if slash <= 0 {
			return "", fmt.Errorf("repository identity must be host/path")
		}
		var err error
		host, err = canonicalAuthority(value[:slash], "")
		if err != nil {
			return "", err
		}
		repoPath = value[slash+1:]
	}

	if repoPath == "" || strings.HasSuffix(repoPath, "/") || strings.Contains(repoPath, "//") || strings.ContainsAny(repoPath, `\%?#@`) {
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

func splitSCP(value string) (host, repoPath string, ok bool, err error) {
	slash := strings.IndexByte(value, '/')
	colon := strings.IndexByte(value, ':')
	if colon < 0 || (slash >= 0 && colon > slash) {
		return "", "", false, nil
	}
	authorityEnd := len(value)
	if slash >= 0 {
		authorityEnd = slash
	}
	if strings.Count(value[:authorityEnd], ":") != 1 {
		return "", "", false, fmt.Errorf("repository host is ambiguous")
	}
	left, right := value[:colon], value[colon+1:]
	if slash > colon && isDecimal(right[:slash-colon-1]) {
		return "", "", false, nil
	}
	if strings.Count(left, "@") > 1 {
		return "", "", false, fmt.Errorf("repository SSH authority is ambiguous")
	}
	if at := strings.IndexByte(left, '@'); at >= 0 {
		if at == 0 {
			return "", "", false, fmt.Errorf("repository SSH user is empty")
		}
		left = left[at+1:]
	}
	return left, right, true, nil
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
		if err != nil {
			return "", fmt.Errorf("repository host/port is not canonical")
		}
		if port == "" {
			return "", fmt.Errorf("repository port is not canonical")
		}
	}

	hostname, err := canonicalHostname(hostname)
	if err != nil {
		return "", err
	}
	if port != "" {
		if !isDecimal(port) || (len(port) > 1 && port[0] == '0') {
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
