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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/retry"
)

// migrationReconcileInterval is the period of the migration reconcile poller.
// The poller is the safety-net timer trigger; it also picks up a migration on
// a newly promoted primary within one interval.
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
	// registration, which fires after the initial Open/openLocked, so openLocked
	// already ran with migrationEnabled=false and did not start the poller.
	// openLocked relaunches it on later re-opens on the fresh ctx.
	pm.startMigrationReconcilePollerLocked()
}

// startMigrationReconcilePollerLocked launches the migration reconcile poller
// on the current pm.ctx. Caller must hold pm.mu. Invoked from
// StartMigrationCoordinator on initial enable and from openLocked on every
// re-open.
func (pm *MultipoolerManager) startMigrationReconcilePollerLocked() {
	go pm.runMigrationReconcile(pm.ctx, migrationReconcileInterval)
}

// defaultGatewayPostgresPort is the multigateway's default Postgres-protocol
// listen port, used as the advertise port when
// --migration-target-advertise-port is 0. It mirrors the gateway default
// rather than importing the gateway config package, since a services/
// package must not depend on another service.
const defaultGatewayPostgresPort = 5432

// targetConnInfo builds a libpq conninfo the external source uses to reach the
// Multigres shard for the EXPORT-direction reverse subscription. It advertises
// the gateway address, not this pooler's topo Hostname: the gateway's
// replication tunnel proxies the stream to the shard's current primary and
// re-pins it across failover, so the subscription survives target failover
// without an ALTER SUBSCRIPTION CONNECTION. The pooler's own Hostname is a
// cluster-internal pod FQDN an outside source cannot resolve.
//
// EXPORT preconditions are enforced here so a set-direction to EXPORT fails
// fast with an actionable error instead of emitting an unreachable conninfo
// that only fails later at CREATE SUBSCRIPTION: the advertise host must be
// set, and slot-based replication must be enabled, since CREATE SUBSCRIPTION
// requests a non-temporary slot by default.
//
// Credentials are the resolved Postgres superuser; returns an error if the
// password is unavailable. v1 uses sslmode=disable; a TLS reverse path and a
// dedicated role are follow-ups.
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
	// database comes from the migration row, which an operator, or via
	// multiadmin's currently-unauthenticated gRPC surface any reachable caller,
	// controls. Every value here is quoted so an embedded space can never inject
	// an extra keyword=value pair that redirects this conninfo, which also
	// carries the real target superuser password, to an attacker-controlled
	// endpoint.
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		pgutil.QuoteConnInfoValue(host), port, pgutil.QuoteConnInfoValue(user),
		pgutil.QuoteConnInfoValue(password), pgutil.QuoteConnInfoValue(database)), nil
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
		pm.migrationCoord = migration.NewCoordinator(qs, pm.logger, pm.targetConnInfo, pm.drainForMigrationImport, pm.releaseForMigrationExport)
	}
	return pm.migrationCoord
}

// MigrationCoordinatorIfPrimary returns the migration coordinator only when
// this pooler is the shard primary. Migration operations mutate replication
// state and must run on the primary; a standby returns FAILED_PRECONDITION so
// the caller re-resolves the current primary and retries. It ensures the
// migration table exists on first use.
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

// ensureMigrationSchema runs EnsureSchema at most once per process. The table
// is normally created at shard bootstrap and replicated in; this covers
// shards bootstrapped before the table existed. Primary-only, since the
// caller has already passed primary guardrails.
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

// migrationHoldState is the three-state result of resolving the migration
// serving gate. The zero value, migrationHoldUnknown, is distinct from
// migrationHoldNotHeld: it means no read against multigres.migration has
// ever succeeded, as opposed to one having succeeded and found no hold.
// Nothing durable backs this across a process restart; it is purely an
// in-memory cache of the last successful read.
type migrationHoldState int32

const (
	migrationHoldUnknown migrationHoldState = iota
	migrationHoldNotHeld
	migrationHoldHeld
)

func (s migrationHoldState) String() string {
	switch s {
	case migrationHoldUnknown:
		return "unknown"
	case migrationHoldNotHeld:
		return "not_held"
	case migrationHoldHeld:
		return "held"
	default:
		return "invalid"
	}
}

// migrationHoldResolveTimeout/BaseWait/MaxWait bound the few quick, local
// retries resolveMigrationHoldBeforeServing makes to resolve
// migrationImportHold out of migrationHoldUnknown before openLocked publishes
// the pooler's first serving status. Deliberately tiny (low single-digit
// milliseconds total): the query is against the pooler's own admin
// connection, not a remote round trip, so this only needs to ride out one
// transient hiccup (e.g. the connection pool settling right after
// openConnectionsLocked), not a sustained outage. A larger bound here taxes
// every reopen in the unit test suite, not just real startups, since the
// mock query service's generic "no pattern matched" error for an unmocked
// query looks exactly like a transient failure and is retried the same way.
const (
	migrationHoldResolveTimeout  = 10 * time.Millisecond
	migrationHoldResolveBaseWait = 1 * time.Millisecond
	migrationHoldResolveMaxWait  = 4 * time.Millisecond
)

// migrationServingHold reports whether this pooler must withhold client
// serving because an active migration on this shard is confirmed still in
// the IMPORT direction. migrationHoldUnknown reads as false, the same as
// migrationHoldNotHeld: this is the general, every-tick check, so it must
// not regress ordinary bootstrap by holding a pooler that simply has not
// resolved the gate yet. The narrower startup case — a restart that lands
// mid-IMPORT before any read has succeeded — gets a few quick extra chances
// to resolve first, via resolveMigrationHoldBeforeServing, not here. It is
// the migration half of the serving gate, OR-combined with the divergence
// hold in the state-manager drift check. Read cheaply as an atomic so it can
// run under the monitor tick.
func (pm *MultipoolerManager) migrationServingHold() bool {
	return migrationHoldState(pm.migrationImportHold.Load()) == migrationHoldHeld
}

// refreshMigrationHold recomputes the migration serving gate from the
// replicated multigres.migration table and stores it in migrationImportHold.
// It runs on every postgres-monitor tick, on the primary and standbys alike,
// since the table is physically replicated, so the gate holds across the
// whole shard and survives failover. Serving is held as soon as a migration
// exists in any phase other than EXPORTING, including CREATED and FAILED;
// only an EXPORTING migration serves. A missing table clears the hold. Any
// other read failure leaves the hold at its last known value instead of
// resolving it, since we can't tell a genuinely-held migration from an
// unreadable table without a successful read, and clearing on an ambiguous
// failure could let a DRAINING pooler reconcile back to SERVING
// mid-migration. It is a single count over a tiny table and never touches
// the state-manager lock.
func (pm *MultipoolerManager) refreshMigrationHold(ctx context.Context) {
	qs := pm.internalQueryService()
	if qs == nil {
		// Connection pools not yet open at startup, or otherwise unavailable: the
		// same ambiguous-read-failure case the docstring above describes. Leave the
		// hold at its last known value rather than resolving it.
		return
	}
	res, err := qs.QueryAdmin(ctx,
		"SELECT count(*) FROM multigres.migration WHERE migration_phase != 'EXPORTING'")
	if err != nil {
		if mterrors.ExtractSQLSTATE(err) == mterrors.PgSSUndefinedTable {
			// multigres.migration absent: the migrator has never been used on this
			// shard, the common case for most poolers.
			pm.migrationImportHold.Store(int32(migrationHoldNotHeld))
		}
		return
	}
	var n int64
	if err := executor.ScanSingleRow(res, &n); err != nil {
		// A decode failure here is the same kind of ambiguous read failure the
		// docstring above describes for QueryAdmin itself: leave the hold at
		// its last known value rather than resolving it.
		return
	}
	if n > 0 {
		pm.migrationImportHold.Store(int32(migrationHoldHeld))
	} else {
		pm.migrationImportHold.Store(int32(migrationHoldNotHeld))
	}
}

// resolveMigrationHoldBeforeServing makes a few quick, local attempts to
// resolve migrationImportHold out of migrationHoldUnknown before openLocked
// publishes this pooler's first serving status. Without this, a pooler
// restarting while genuinely mid-IMPORT whose very first refreshMigrationHold
// read happens to fail transiently would briefly serve a half-copied target,
// since migrationServingHold treats an unresolved gate the same as a
// confirmed-clear one. The few near-immediate retries give that one read a
// real second chance to land before that first publish; if it still has not
// resolved once the (tiny) bound expires, this falls through to the same
// not-held behavior as an unresolved gate anywhere else — no worse than
// before this existed.
//
// Does not retry at all when there is no query service: that is not a
// transient query failure to ride out, it means connections are not open
// yet (or never will be, e.g. in a test harness with no pool configured), so
// retrying within this bound would just busy-wait. The next postgres-monitor
// tick resolves it once connections are up, same as before this existed.
func (pm *MultipoolerManager) resolveMigrationHoldBeforeServing(ctx context.Context) {
	if pm.internalQueryService() == nil {
		pm.refreshMigrationHold(ctx)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, migrationHoldResolveTimeout)
	defer cancel()
	r := retry.New(migrationHoldResolveBaseWait, migrationHoldResolveMaxWait)
	for _, err := range r.Attempts(timeoutCtx) {
		if err != nil {
			return
		}
		pm.refreshMigrationHold(timeoutCtx)
		if migrationHoldState(pm.migrationImportHold.Load()) != migrationHoldUnknown {
			return
		}
		if pm.internalQueryService() == nil {
			return
		}
	}
}

// drainForMigrationImport is the synchronous serving barrier the migration
// coordinator runs before an IMPORT setup drops target tables or starts
// streaming. It sets the import hold and reconciles serving inline, draining
// in-flight writes and leaving the pooler DRAINING, instead of waiting for the
// asynchronous ~5s postgres-monitor tick to observe the phase, which setup
// usually outruns. Injected into the coordinator via NewCoordinator.
//
// The hold atomic is set first so the monitor's drift check keeps the pooler
// DRAINING once ForceMigrationHold puts it there; otherwise a monitor tick
// landing before refreshMigrationHold re-reads the table could reconcile
// DRAINING back to SERVING. refreshMigrationHold then keeps it true on every
// tick because the phase has left CREATED.
func (pm *MultipoolerManager) drainForMigrationImport(ctx context.Context) error {
	pm.migrationImportHold.Store(int32(migrationHoldHeld))
	lockCtx, err := pm.actionLock.Acquire(ctx, "MigrationImportDrain")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(lockCtx)
	return pm.stateManager.ForceMigrationHold(lockCtx)
}

// releaseForMigrationExport is the synchronous serving flip the migration
// coordinator runs the moment the IMPORT->EXPORT cutover commits the EXPORTING
// phase. It recomputes the import hold from the now-EXPORTING migration table,
// so a second still-importing migration on the shard keeps the hold, and
// reconciles serving inline, completing the transient DRAINING back to
// SERVING instead of waiting for the ~5s postgres-monitor tick. Prompt
// serving-on lets the gateway's failover buffer replay the queries it held
// during the cutover within its window. Injected into the coordinator via
// NewCoordinator; symmetric to drainForMigrationImport.
func (pm *MultipoolerManager) releaseForMigrationExport(ctx context.Context) error {
	// Recompute from the table rather than blindly clearing: another migration on
	// this shard may still be importing and must keep the hold.
	pm.refreshMigrationHold(ctx)
	lockCtx, err := pm.actionLock.Acquire(ctx, "MigrationExportRelease")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(lockCtx)
	return pm.stateManager.ReconcileMigrationHold(lockCtx, pm.migrationServingHold())
}

// runMigrationReconcile is the reconcile poller. Each tick, if this pooler is
// the primary, it advances in-flight migrations and resumes coordination,
// which is what picks a migration up on a newly promoted primary. On a
// standby every tick is a cheap no-op.
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
