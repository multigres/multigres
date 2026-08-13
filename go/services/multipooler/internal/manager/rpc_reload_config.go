// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/timeouts"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

// ReloadConfig triggers a PostgreSQL configuration reload on this multipooler's
// local PostgreSQL and confirms it took effect.
//
// It issues the reload via pgctld (which sends SIGHUP), then waits for
// pg_conf_load_time() to advance past the moment the reload was triggered. The
// returned config_load_time is that confirmed-advanced value: a caller that
// wrote its config change before calling this can trust that a config_load_time
// newer than its write means PostgreSQL re-read the file.
//
// Triggering the reload through pgctld (an OS signal) rather than SQL
// pg_reload_conf() avoids needing a superuser connection; reading
// pg_conf_load_time() needs no elevated privilege.
//
// When the request carries expected_settings, ReloadConfig additionally reads
// pg_file_settings after the reload and reports, per setting, whether the file
// PostgreSQL just re-read carries the desired value and put it into effect. This
// lets a caller detect a stale config file (the SIGHUP fired but the file it read
// does not yet reflect the write) and a setting that needs a restart, rather than
// trusting that "a reload happened" means "my change is in effect".
//
// Contract:
//   - Operates on the local PostgreSQL only.
//   - If PostgreSQL is not running, pgctld's reload fails; this returns
//     reloaded=false with no error so the caller can treat it as retryable.
//   - Idempotent: calling it when nothing changed is a harmless reload + read.
func (pm *MultipoolerManager) ReloadConfig(ctx context.Context, req *multipoolermanagerdatapb.ReloadConfigRequest) (*multipoolermanagerdatapb.ReloadConfigResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// pgctld's ReloadConfig is a state-changing operation and requires the
	// action lock (enforced by protectedPgctldClient). Acquire it so we don't
	// race the monitor or another manual operation.
	ctx, err := pm.actionLock.Acquire(ctx, "ReloadConfig")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Capture a baseline on the local clock before triggering. multipooler and
	// PostgreSQL are colocated, so pg_conf_load_time() (server clock) is
	// comparable to this: any load time >= baseline reflects a reload at or
	// after our trigger, while every prior reload is strictly in the past. This
	// absolute reference is why a pooled poll is safe — backends that have not
	// yet processed our SIGHUP report their previous (pre-baseline) load time.
	baseline := time.Now()

	// Trigger the SIGHUP via pgctld. When PostgreSQL is not running pgctld
	// returns an error; surface that as an empty response (nil config_load_time)
	// rather than failing hard so the caller can retry.
	if _, err := pm.pgctldClient.ReloadConfig(ctx, &pgctldpb.ReloadConfigRequest{}); err != nil {
		pm.logger.WarnContext(ctx, "pgctld ReloadConfig failed, reporting not reloaded", "error", err)
		return &multipoolermanagerdatapb.ReloadConfigResponse{}, nil
	}

	// Confirm the reload took effect by waiting for pg_conf_load_time() to
	// advance to at or after the baseline captured before the trigger. This
	// reuses the same poll helper as the SQL-triggered consensus reload path,
	// supplying a baseline predicate instead of a before/after comparison.
	loadTime, err := consensus.WaitForConfigReload(ctx, pm.internalQueryService(), func(loadTime time.Time) bool {
		return !loadTime.Before(baseline)
	})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to confirm configuration reload")
	}

	resp := &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.New(loadTime),
		// With no settings to verify, the reload alone is the whole result: an
		// empty expectation is vacuously satisfied.
		AllApplied: true,
	}

	// If the caller supplied expected settings, verify what the reload actually
	// loaded against them. The reload above already succeeded, so a verification
	// read failure is surfaced as an error and the caller retries (the reload is
	// idempotent).
	if len(req.GetExpectedSettings()) > 0 {
		if err := pm.verifyExpectedSettings(ctx, req.GetExpectedSettings(), resp); err != nil {
			return nil, mterrors.Wrap(err, "failed to verify reloaded settings")
		}
	}

	return resp, nil
}

// verifyExpectedSettings reads pg_file_settings (joined with pg_settings for the
// pending_restart flag) and fills resp.AllApplied, resp.Mismatches, and
// resp.NeedsRestart by comparing the file PostgreSQL just re-read against the
// desired name->value map.
//
// It reads pg_file_settings rather than pg_settings for the value comparison for
// two reasons: pg_file_settings reflects the config file the SIGHUP actually read
// (so a not-yet-synced file shows the old value and the mismatch is visible), and
// its setting column is the raw file token ('32MB'), which matches what the caller
// wrote without unit normalization. pg_settings is consulted only for
// pending_restart, which pg_file_settings cannot express: a restart-requiring
// change reports the generic "setting could not be applied" error there.
func (pm *MultipoolerManager) verifyExpectedSettings(
	ctx context.Context,
	expected map[string]string,
	resp *multipoolermanagerdatapb.ReloadConfigResponse,
) error {
	qs := pm.internalQueryService()
	if qs == nil {
		return errors.New("internal query service not available")
	}

	// pg_file_settings has one row per occurrence of a setting across all config
	// files. Ordering by seqno lets us pick the effective occurrence per name
	// (see effectiveFileSetting). pg_settings.pending_restart is per-setting, so
	// a LEFT JOIN by name is sufficient; unknown/custom GUCs have no pg_settings
	// row and their pending_restart reads as NULL (treated as false).
	queryCtx, cancel := context.WithTimeout(ctx, timeouts.PostgresConfigTimeout)
	defer cancel()
	result, err := qs.Query(queryCtx, `SELECT fs.name, fs.setting, fs.applied, fs.error, s.pending_restart
FROM pg_file_settings fs
LEFT JOIN pg_settings s ON s.name = fs.name
ORDER BY fs.seqno`)
	if err != nil {
		return mterrors.Wrap(err, "failed to read pg_file_settings")
	}

	effective := effectiveFileSettings(result)

	allApplied := true
	for name, want := range expected {
		fs, present := effective[name]
		if present && fs.applied && fs.setting == want {
			// Present, matches, and in effect: nothing to report.
			continue
		}

		allApplied = false
		mismatch := &multipoolermanagerdatapb.SettingMismatch{
			Name:           name,
			Expected:       want,
			Actual:         fs.setting,
			Present:        present,
			Applied:        fs.applied,
			Error:          fs.errText,
			PendingRestart: fs.pendingRestart,
		}
		// The file carries the desired value but it did not take effect because
		// PostgreSQL wants a restart. A reload will never satisfy this; signal the
		// caller to escalate.
		if present && !fs.applied && fs.setting == want && fs.pendingRestart {
			resp.NeedsRestart = true
		}
		resp.Mismatches = append(resp.Mismatches, mismatch)
	}

	resp.AllApplied = allApplied
	return nil
}

// fileSetting is the effective state of one GUC as seen in pg_file_settings.
type fileSetting struct {
	setting        string
	applied        bool
	errText        string
	pendingRestart bool
}

// effectiveFileSettings collapses the per-occurrence rows of pg_file_settings
// into one entry per setting name: the occurrence PostgreSQL actually put into
// effect. Rows are assumed to arrive in seqno order. A name can appear multiple
// times (across files or duplicated within one); at most one occurrence is
// applied, and it is the effective one. Until an applied occurrence is seen we
// keep the latest (highest-seqno) row so a fully-unapplied setting still reports
// its last value and error.
func effectiveFileSettings(result *sqltypes.Result) map[string]fileSetting {
	effective := make(map[string]fileSetting, len(result.Rows))
	for _, row := range result.Rows {
		var (
			name           string
			setting        string
			applied        bool
			errText        *string
			pendingRestart *bool
		)
		// error and pending_restart are nullable: pg_file_settings.error is NULL
		// when the occurrence applied cleanly, and pending_restart is NULL for
		// custom GUCs absent from pg_settings.
		if err := executor.ScanRow(row, &name, &setting, &applied, &errText, &pendingRestart); err != nil {
			// A malformed row is skipped rather than failing the whole verdict;
			// an expected setting that relied on it will surface as a mismatch.
			continue
		}

		fs := fileSetting{
			setting:        setting,
			applied:        applied,
			errText:        derefString(errText),
			pendingRestart: derefBool(pendingRestart),
		}
		// Once we have recorded the applied occurrence for a name, keep it; a later
		// occurrence cannot displace the one PostgreSQL put into effect.
		if cur, ok := effective[name]; ok && cur.applied {
			continue
		}
		effective[name] = fs
	}
	return effective
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefBool(p *bool) bool {
	return p != nil && *p
}
