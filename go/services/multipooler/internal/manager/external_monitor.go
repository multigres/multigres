// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

const (
	// externalReadinessQuery reports whether the endpoint is a writable primary,
	// and the database the connection landed in.
	externalReadinessQuery = "SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off', current_database()"
	externalProbeTimeout   = 2 * time.Second
)

// errExternalDatabaseMismatch means the endpoint answered but the connection
// is in a database other than the one the backing connection names.
var errExternalDatabaseMismatch = errors.New("external backend database does not match its backing connection")

// probeExternalBackend checks that the endpoint is reachable, writable and in the
// backing connection's database. Run on every probe, so a reconnect that lands
// elsewhere withdraws readiness instead of silently serving from it.
func (pm *MultipoolerManager) probeExternalBackend(ctx context.Context) error {
	result, err := pm.adminQuery(ctx, externalReadinessQuery)
	if err != nil {
		return err
	}
	var (
		writable bool
		database string
	)
	if err := executor.ScanSingleRow(result, &writable, &database); err != nil {
		return err
	}
	if !writable {
		return errors.New("external backend is not writable")
	}
	// ExternalBackend is always set on an unmanaged pooler (validateUnmanagedConfig).
	if database != pm.config.ExternalBackend.Database {
		return errExternalDatabaseMismatch
	}
	return nil
}

func (pm *MultipoolerManager) checkExternalReadiness(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, externalProbeTimeout)
	err := pm.probeExternalBackend(probeCtx)
	cancel()
	ready := err == nil
	if err != nil {
		pm.logger.DebugContext(ctx, "external backend not ready", "error", err)
	}

	if !pm.applyExternalReadiness(ctx, ready) || !ready {
		return
	}
	// ACTIVE as a managed pooler's lifecycle is once its postgres is up. It stays ACTIVE when the
	// backend stops being ready: readiness is the serving status.
	pm.markPoolerActive(ctx)
}

// applyExternalReadiness sets the serving status and postgres mode from the probe, under the
// action lock, and reports whether it did.
func (pm *MultipoolerManager) applyExternalReadiness(ctx context.Context, ready bool) bool {
	lockCtx, lockErr := pm.actionLock.Acquire(ctx, "ExternalReadiness")
	if lockErr != nil {
		return false
	}
	defer pm.actionLock.Release(lockCtx)

	status := clustermetadatapb.PoolerServingStatus_DISABLED
	mode := pgmode.Unknown
	if ready {
		status = clustermetadatapb.PoolerServingStatus_SERVING
		mode = pgmode.Primary
	}
	if mutateErr := pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) {
		s.ServingStatus = status
		s.PostgresMode = mode
	}); mutateErr != nil {
		pm.logger.WarnContext(ctx, "external readiness transition failed", "error", mutateErr)
		return false
	}
	return true
}
