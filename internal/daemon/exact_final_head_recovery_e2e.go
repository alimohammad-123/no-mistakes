//go:build e2e

package daemon

import (
	"context"
	"os"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
)

func init() {
	if os.Getenv("NM_E2E_SYNTHETIC_EXACT_FINAL_HEAD_RECOVERY") != "1" {
		return
	}
	validateExactFinalHeadRecoveryExternalState = func(context.Context, *db.Run, *db.Repo, string, *config.Config, bool) error {
		return nil
	}
}
