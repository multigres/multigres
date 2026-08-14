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
// When the request carries expected_settings, the order is reversed: ReloadConfig
// reads pg_file_settings FIRST and reloads only if the file it would read already
// carries every desired value and each is reload-applicable. Otherwise it does
// NOT reload and returns the reason (mismatch or needs_restart) so the caller can
// retry (or restart). Checking before reloading is what makes the result
// trustworthy when the config file is written asynchronously (e.g. a Kubernetes
// ConfigMap mount whose file the kubelet updates on its own schedule, with no
// signal for when the write has landed): we never reload — and never report
// success — against a file that has not caught up. Reading pg_file_settings after
// a reload could not give this guarantee, because pg_file_settings.applied means
// "this entry would apply cleanly on a reload," not "the running value equals it,"
// so a file written just after the reload would read back as a false success.
//
// Contract:
//   - Operates on the local PostgreSQL only.
//   - If PostgreSQL is not running, pgctld's reload fails; this returns
//     reloaded=false with no error so the caller can treat it as retryable.
//   - Idempotent: calling it when nothing changed is a harmless reload + read.
//   - With expected_settings, a reload happens only when the file already
//     satisfies all of them; a not-yet-synced file leaves config_load_time unset
//     (retryable) with mismatches, and a restart-only change yields
//     needs_restart=true.
func (pm *MultipoolerManager) ReloadConfig(ctx context.Context, req *multipoolermanagerdatapb.ReloadConfigRequest) (*multipoolermanagerdatapb.ReloadConfigResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// pgctld's ReloadConfig is a state-changing operation and requires the
	// action lock (enforced by protectedPgctldClient). Acquire it so we don't
	// race the monitor or another manual operation. Holding it across the
	// pre-reload check keeps the check and the reload consistent — no other
	// reload can slip in between them.
	ctx, err := pm.actionLock.Acquire(ctx, "ReloadConfig")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// When the caller supplied expected settings, gate the reload on the file
	// already carrying them. If it does not, report why and skip the reload.
	if len(req.GetExpectedSettings()) > 0 {
		verdict, err := pm.checkExpectedSettings(ctx, req.GetExpectedSettings())
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to check expected settings before reload")
		}
		if !verdict.allReloadReady {
			// Not reload-ready: leave config_load_time unset (no reload) and report
			// why so the caller retries or restarts.
			return &multipoolermanagerdatapb.ReloadConfigResponse{
				Mismatches:   verdict.mismatches,
				NeedsRestart: verdict.needsRestart,
			}, nil
		}
	}

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

	// A set config_load_time is the success signal: the reload ran because the
	// file already carried every expected value (or there was nothing to check),
	// so the desired reload-safe settings are now in effect.
	return &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.New(loadTime),
	}, nil
}

// settingsVerdict is the outcome of comparing the desired settings against the
// config file, before any reload.
type settingsVerdict struct {
	// allReloadReady is true when every expected setting is present in the file
	// with the desired value and a reload would put it into effect. Only then is
	// it worth reloading.
	allReloadReady bool
	// mismatches describes every expected setting that is not reload-ready.
	mismatches []*multipoolermanagerdatapb.SettingMismatch
	// needsRestart is true when at least one expected setting is written correctly
	// but is a postmaster-context GUC that a reload cannot apply.
	needsRestart bool
}

// checkExpectedSettings reads pg_file_settings (joined with pg_settings for the
// context) and compares the file PostgreSQL would read now against the desired
// name->value map, without triggering a reload.
//
// It reads pg_file_settings rather than pg_settings for the value comparison for
// two reasons: pg_file_settings reflects the config file (so a not-yet-synced
// file shows the old value and the mismatch is visible), and its setting column
// is the raw file token ('32MB'), which matches what the caller wrote without
// unit normalization. pg_settings is consulted only for context, used to tell a
// restart-only change apart from an ordinary reload-safe one.
func (pm *MultipoolerManager) checkExpectedSettings(
	ctx context.Context,
	expected map[string]string,
) (*settingsVerdict, error) {
	qs := pm.internalQueryService()
	if qs == nil {
		return nil, errors.New("internal query service not available")
	}

	// pg_file_settings has one row per occurrence of a setting across all config
	// files. Ordering by seqno lets us pick the effective occurrence per name
	// (see effectiveFileSettings). pg_settings.context is per-setting, so a LEFT
	// JOIN by name is sufficient; unknown/custom GUCs have no pg_settings row and
	// their context reads as NULL (empty, treated as non-postmaster).
	queryCtx, cancel := context.WithTimeout(ctx, timeouts.PostgresConfigTimeout)
	defer cancel()
	result, err := qs.Query(queryCtx, `SELECT fs.name, fs.setting, fs.applied, fs.error, s.context
FROM pg_file_settings fs
LEFT JOIN pg_settings s ON s.name = fs.name
ORDER BY fs.seqno`)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read pg_file_settings")
	}

	effective := effectiveFileSettings(result)

	verdict := &settingsVerdict{allReloadReady: true}
	for name, want := range expected {
		fs, present := effective[name]
		if present && fs.setting == want && fs.applied {
			// The file carries the desired value and a reload would put it into
			// effect (or it already is): reload-ready, nothing to report.
			continue
		}

		verdict.allReloadReady = false

		// A postmaster-context GUC whose file value matches the request but that a
		// reload cannot apply (applied=false) needs a restart. This distinguishes a
		// "reload won't help, restart" case from a stale/wrong file value.
		requiresRestart := present && fs.setting == want && !fs.applied && fs.context == "postmaster"
		if requiresRestart {
			verdict.needsRestart = true
		}

		verdict.mismatches = append(verdict.mismatches, &multipoolermanagerdatapb.SettingMismatch{
			Name:            name,
			Expected:        want,
			Actual:          fs.setting,
			Present:         present,
			Applied:         fs.applied,
			Error:           fs.errText,
			RequiresRestart: requiresRestart,
		})
	}

	return verdict, nil
}

// fileSetting is the effective state of one GUC as seen in pg_file_settings.
type fileSetting struct {
	setting string
	applied bool
	errText string
	// context is pg_settings.context for the GUC ("postmaster", "sighup", ...),
	// empty when the GUC has no pg_settings row.
	context string
}

// effectiveFileSettings collapses the per-occurrence rows of pg_file_settings
// into one entry per setting name: the occurrence that wins. Rows are assumed to
// arrive in seqno order. A name can appear multiple times (across files or
// duplicated within one); at most one occurrence is applied, and it is the
// effective one. Until an applied occurrence is seen we keep the latest
// (highest-seqno) row so a fully-unapplied setting still reports its last value
// and error.
func effectiveFileSettings(result *sqltypes.Result) map[string]fileSetting {
	effective := make(map[string]fileSetting, len(result.Rows))
	for _, row := range result.Rows {
		var (
			name    string
			setting string
			applied bool
			errText *string
			context *string
		)
		// error and context are nullable: pg_file_settings.error is NULL when the
		// occurrence would apply cleanly, and context is NULL for a custom GUC
		// absent from pg_settings.
		if err := executor.ScanRow(row, &name, &setting, &applied, &errText, &context); err != nil {
			// A malformed row is skipped rather than failing the whole verdict;
			// an expected setting that relied on it will surface as a mismatch.
			continue
		}

		fs := fileSetting{
			setting: setting,
			applied: applied,
			errText: derefString(errText),
			context: derefString(context),
		}
		// Once we have recorded the applied occurrence for a name, keep it; a later
		// occurrence cannot displace the one PostgreSQL would put into effect.
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
