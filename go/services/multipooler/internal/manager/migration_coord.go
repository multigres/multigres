// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// migrationReconcileInterval is the period of the migration reconcile poller.
// The poller is the safety-net timer trigger; it also picks up a migration on a
// newly promoted primary within one interval (the become-primary path).
const migrationReconcileInterval = 10 * time.Second

// StartMigrationCoordinator opts this manager into the Multigres Migrator migration
// coordinator: openLocked will launch the reconcile poller, and the migration
// gRPC service can drive migrations via MigrationCoordinatorIfPrimary. Called by
// the service layer at registration, mirroring StartBackupHealth.
func (pm *MultipoolerManager) StartMigrationCoordinator() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.migrationEnabled {
		return
	}
	pm.migrationEnabled = true
	// Launch the poller now. StartMigrationCoordinator runs from the delayed gRPC
	// registration (senv.OnRunE), which fires AFTER the initial Open/openLocked —
	// so that openLocked already ran with migrationEnabled=false and did not start
	// the poller. Start it here (mirroring StartBackupHealth); openLocked
	// relaunches it on later re-opens (e.g. become-primary) on the fresh ctx.
	pm.startMigrationReconcilePollerLocked()
}

// startMigrationReconcilePollerLocked launches the migration reconcile poller on
// the current pm.ctx. Caller must hold pm.mu. Invoked from StartMigrationCoordinator
// (initial enable) and from openLocked on every re-open.
func (pm *MultipoolerManager) startMigrationReconcilePollerLocked() {
	go pm.runMigrationReconcile(pm.ctx, migrationReconcileInterval)
}

// defaultGatewayPostgresPort is the multigateway's default Postgres-protocol
// listen port (multigateway --pg-port), used as the advertise port when
// --migration-target-advertise-port is 0. It mirrors the gateway default rather
// than importing the gateway config package (a services/ package must not depend
// on another service).
const defaultGatewayPostgresPort = 5432

// targetConnInfo builds a libpq conninfo the external source uses to reach the
// Multigres shard for the EXPORT-direction reverse subscription. It advertises
// the *gateway* address (--migration-target-advertise-host/-port), not this
// pooler's topo Hostname: the gateway's replication=database tunnel
// (multigateway HandleReplicationStream) proxies the replication stream to the
// shard's current primary and re-pins it across failover, so a subscription
// CONNECTION pointed at the gateway is reachable from an external/standalone
// source and survives target failover with no ALTER SUBSCRIPTION CONNECTION.
// The pooler's own Hostname is a cluster-internal pod FQDN an outside source
// cannot resolve.
//
// EXPORT preconditions are enforced here so a set-direction→EXPORT fails fast
// with an actionable error rather than emitting an unreachable conninfo that
// only fails later at CREATE SUBSCRIPTION on the source:
//   - --migration-target-advertise-host must be set (the gateway address).
//   - Slot-based replication (--enable-slot-based-replication) must be enabled:
//     CREATE SUBSCRIPTION requests a non-temporary slot by default, and the
//     gateway's replication preamble admits a non-temporary slot only when that
//     feature is on.
//
// Credentials are the resolved Postgres superuser; returns an error if the
// password is unavailable. NOTE: v1 uses sslmode=disable — a TLS reverse path
// and a dedicated role are follow-ups.
func (pm *MultipoolerManager) targetConnInfo(database string) (string, error) {
	host := ""
	port := defaultGatewayPostgresPort
	if pm.config != nil {
		host = pm.config.MigrationTargetAdvertiseHost
		if pm.config.MigrationTargetAdvertisePort != 0 {
			port = pm.config.MigrationTargetAdvertisePort
		}
	}
	if host == "" {
		return "", errors.New("EXPORT requires --migration-target-advertise-host set to a gateway address reachable from the source")
	}
	if !pm.slotBasedReplicationEnabled() {
		return "", errors.New("EXPORT requires slot-based replication (--enable-slot-based-replication, on the gateway) enabled: CREATE SUBSCRIPTION requests a non-temporary slot, which the gateway's replication tunnel admits only when that feature is on")
	}
	user := constants.DefaultPostgresUser
	var password string
	if pm.connPoolMgr != nil {
		user = pm.connPoolMgr.PgUser()
		pw, ok := pm.connPoolMgr.PgPassword()
		if !ok {
			return "", errors.New("Postgres password not resolved for target conninfo")
		}
		password = pw
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, database), nil
}

// migrationCoordinator lazily builds the coordinator from the admin query
// service. Returns nil if the query service is not available yet.
func (pm *MultipoolerManager) migrationCoordinator() *migration.Coordinator {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.migrationCoord == nil {
		qs := pm.internalQueryService()
		if qs == nil {
			return nil
		}
		pm.migrationCoord = migration.NewCoordinator(qs, pm.logger, pm.targetConnInfo)
	}
	return pm.migrationCoord
}

// MigrationCoordinatorIfPrimary returns the migration coordinator only when this
// pooler is the shard primary (Postgres out of recovery). Migration operations
// mutate replication state and must run on the primary; a standby returns
// FAILED_PRECONDITION so the caller (multiadmin) re-resolves the current primary
// and retries. It ensures the migration table exists on first use.
func (pm *MultipoolerManager) MigrationCoordinatorIfPrimary(ctx context.Context) (*migration.Coordinator, error) {
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}
	coord := pm.migrationCoordinator()
	if coord == nil {
		return nil, errors.New("migration coordinator not available: query service not open")
	}
	pm.ensureMigrationSchema(ctx, coord)
	return coord, nil
}

// ensureMigrationSchema runs EnsureSchema at most once per process. The table is
// normally created at shard bootstrap (createSidecarSchema) and replicated in;
// this covers shards bootstrapped before the table existed. Primary-only (the
// caller has already passed checkPrimaryGuardrails).
func (pm *MultipoolerManager) ensureMigrationSchema(ctx context.Context, coord *migration.Coordinator) {
	pm.mu.Lock()
	ensured := pm.migrationSchemaEnsured
	pm.mu.Unlock()
	if ensured {
		return
	}
	if err := coord.EnsureSchema(ctx); err != nil {
		pm.logger.WarnContext(ctx, "ensure migration schema failed", "error", err)
		return // not marked ensured; retried on next call
	}
	pm.mu.Lock()
	pm.migrationSchemaEnsured = true
	pm.mu.Unlock()
}

// runMigrationReconcile is the reconcile poller. Each tick, if this pooler is
// the primary, it advances in-flight migrations (e.g. COPYING -> STREAMING) and
// resumes coordination — this is what picks a migration up on a newly promoted
// primary. On a standby every tick is a cheap no-op.
func (pm *MultipoolerManager) runMigrationReconcile(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			coord, err := pm.MigrationCoordinatorIfPrimary(ctx)
			if err != nil {
				continue // not primary, or query service not ready
			}
			if err := coord.Reconcile(ctx); err != nil {
				pm.logger.WarnContext(ctx, "migration reconcile failed", "error", err)
			}
		}
	}
}
