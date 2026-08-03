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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
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
// Contract:
//   - Operates on the local PostgreSQL only.
//   - If PostgreSQL is not running, pgctld's reload fails; this returns
//     reloaded=false with no error so the caller can treat it as retryable.
//   - Idempotent: calling it when nothing changed is a harmless reload + read.
func (pm *MultipoolerManager) ReloadConfig(ctx context.Context) (*multipoolermanagerdatapb.ReloadConfigResponse, error) {
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

	return &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.New(loadTime),
	}, nil
}
