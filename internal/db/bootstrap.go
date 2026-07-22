package db

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/repoidentity"
)

// ErrBootstrapTestRetired means this user-owned state has permanently closed
// first-policy Test bootstrap for one repository and pipeline base.
var ErrBootstrapTestRetired = errors.New("bootstrap Test authorization is permanently retired")

func validateBootstrapTestRetirementKey(repository, baseBranch string) error {
	identity, err := repoidentity.Canonical(repository)
	if err != nil || identity != repository {
		return fmt.Errorf("invalid bootstrap Test retirement repository %q", repository)
	}
	if baseBranch == "" || strings.TrimSpace(baseBranch) != baseBranch {
		return fmt.Errorf("invalid bootstrap Test retirement base branch %q", baseBranch)
	}
	return nil
}

// RetireBootstrapTest durably and idempotently closes bootstrap for the exact
// repository/base trust root. The single insert commits atomically in SQLite;
// callers must stop if it returns an error.
func (d *DB) RetireBootstrapTest(repository, baseBranch string) error {
	if err := validateBootstrapTestRetirementKey(repository, baseBranch); err != nil {
		return err
	}
	_, err := d.sql.Exec(
		`INSERT INTO bootstrap_test_retirements (repository, base_branch, retired_at) VALUES (?, ?, ?)
		 ON CONFLICT(repository, base_branch) DO NOTHING`,
		repository, baseBranch, now(),
	)
	if err != nil {
		return fmt.Errorf("retire bootstrap Test authorization: %w", err)
	}
	return nil
}

// IsBootstrapTestRetired reports the durable retirement state for one exact
// repository/base key. Storage errors are returned so authorization can fail
// closed instead of treating an unreadable tombstone as absent.
func (d *DB) IsBootstrapTestRetired(repository, baseBranch string) (bool, error) {
	if err := validateBootstrapTestRetirementKey(repository, baseBranch); err != nil {
		return false, err
	}
	var exists int
	err := d.sql.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM bootstrap_test_retirements WHERE repository = ? AND base_branch = ?)`,
		repository, baseBranch,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("read bootstrap Test retirement: %w", err)
	}
	return exists != 0, nil
}
