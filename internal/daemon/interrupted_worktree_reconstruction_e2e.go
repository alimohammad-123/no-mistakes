//go:build e2e

package daemon

import (
	"context"
	"os"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

// The e2e binary is built with the e2e tag. This opt-in replaces only
// incident/environment attestations that cannot be reproduced with synthetic
// commit hashes or a local file:// remote. Path, Git topology, journal,
// evidence-token, same-run claim, and executor behavior remain production code.
func init() {
	if os.Getenv("NM_E2E_SYNTHETIC_INTERRUPTED_RECONSTRUCTION") != "1" {
		return
	}
	matchInterruptedWorktreeIncident = func(*db.Repo, *db.Run, *db.StepResult) error { return nil }
	probeInterruptedExternalState = func(context.Context, *db.Repo, string, string, string) error { return nil }
	probeInterruptedProcessOwners = func(context.Context, string, string) error { return nil }
	validateInterruptedRegisteredWorkingPath = func(context.Context, *db.Repo) error { return nil }
}
