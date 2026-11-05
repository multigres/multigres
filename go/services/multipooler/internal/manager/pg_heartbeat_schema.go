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
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
)

// multigres.heartbeat is created once, at shard bootstrap, so a shard bootstrapped by an
// older release has the table that release defined. A release that adds a column to the
// heartbeat queries therefore has to upgrade the table itself, and has to notice when it
// cannot.
//
// Upgrade: the primary adds any missing newer columns (heartbeat.EnsureSchema) on every
// open and after every promotion. Standbys get the change through replication. Anything
// beyond missing newer columns (a renamed or retyped column, an extra NOT NULL column) is
// never repaired automatically; it is reported.
//
// Health: the result is kept in heartbeatSchema, exported as the
// mg.pooler.heartbeat.schema.compatible gauge and logged at ERROR, and the heartbeat
// writer and reader count their failures by reason (mg.pooler.heartbeat.errors).
//
// Failover policy: an incompatible heartbeat table never blocks promotion or failover.
// The table is replicated, so every candidate inherits the same table, and refusing to
// promote would leave the shard without a leader over a sidecar table. Instead, an
// incompatible table withholds the leader's quorum-commit proof from PrimaryStatus
// (see heartbeatSchemaBroken). The heartbeat writer is what keeps that proof fresh, so
// with a broken table the proof only ages, and multiorch would read the leader as
// LeaderQuorumWritesStalled and fail over to a node that has the same broken table. With
// the proof withheld, multiorch sees no evidence either way and does not convict. The
// cost is that the stalled-quorum backstop is off for this shard until the table is
// repaired; the metrics and the ERROR log are the signal for that.

const heartbeatSchemaTimeout = 5 * time.Second

// recordHeartbeatSchema stores the latest schema report, publishes it to the gauge and
// logs when the verdict changes.
func (pm *MultipoolerManager) recordHeartbeatSchema(ctx context.Context, report *heartbeat.SchemaReport) {
	prev := pm.heartbeatSchema.Swap(report)
	pm.metrics.setHeartbeatSchemaCompatible(report.Compatible())
	switch {
	case !report.Compatible():
		if prev == nil || prev.String() != report.String() {
			pm.logger.ErrorContext(ctx, "heartbeat table is incompatible with this release; heartbeats will fail and the leader's quorum-commit proof is withheld until it is repaired",
				"problems", report.String())
		}
	case prev != nil && !prev.Compatible():
		pm.logger.InfoContext(ctx, "heartbeat table is compatible again")
	}
}

// ensureHeartbeatSchema upgrades the heartbeat table on a primary and records the result.
// It never fails the caller: an incompatible table is reported, not fatal (see the policy
// above), and a failure to run the check leaves the previous verdict in place.
func (pm *MultipoolerManager) ensureHeartbeatSchema(ctx context.Context) {
	qs := pm.internalQueryService()
	if qs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, heartbeatSchemaTimeout)
	defer cancel()
	report, err := heartbeat.EnsureSchema(ctx, qs)
	if err != nil {
		pm.logger.WarnContext(ctx, "failed to ensure the heartbeat table schema", "error", err)
	}
	if report != nil {
		pm.recordHeartbeatSchema(ctx, report)
	}
}

// checkHeartbeatSchema is the read-only variant for a standby, which cannot alter the
// table and receives upgrades by replication.
func (pm *MultipoolerManager) checkHeartbeatSchema(ctx context.Context) {
	qs := pm.internalQueryService()
	if qs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, heartbeatSchemaTimeout)
	defer cancel()
	report, err := heartbeat.CheckSchema(ctx, qs)
	if err != nil {
		pm.logger.WarnContext(ctx, "failed to check the heartbeat table schema", "error", err)
		return
	}
	pm.recordHeartbeatSchema(ctx, report)
}

// refreshHeartbeatSchema re-evaluates the heartbeat table for the postgres' current role.
func (pm *MultipoolerManager) refreshHeartbeatSchema(ctx context.Context) {
	mode, err := pm.postgresMode(ctx)
	if err != nil {
		return
	}
	if mode.OutOfRecovery() {
		pm.ensureHeartbeatSchema(ctx)
	} else {
		pm.checkHeartbeatSchema(ctx)
	}
}

// heartbeatSchemaErrorCount is the number of heartbeat errors so far that an
// incompatible table caused.
func (pm *MultipoolerManager) heartbeatSchemaErrorCount() int64 {
	if pm.replTracker == nil {
		return 0
	}
	return pm.replTracker.HeartbeatWriter().WriteSchemaErrors() + pm.replTracker.HeartbeatReader().ReadSchemaErrors()
}

// maybeRefreshHeartbeatSchema re-checks the table when something suggests the last verdict
// is out of date: the heartbeat writer or reader hit a schema error since the last check
// (the table changed under us), or the last verdict was "incompatible" (an operator may
// have repaired it). A healthy steady state costs nothing.
func (pm *MultipoolerManager) maybeRefreshHeartbeatSchema(ctx context.Context) {
	count := pm.heartbeatSchemaErrorCount()
	last := pm.heartbeatSchema.Load()
	if count == pm.heartbeatSchemaErrorsSeen.Load() && (last == nil || last.Compatible()) {
		return
	}
	pm.heartbeatSchemaErrorsSeen.Store(count)
	pm.refreshHeartbeatSchema(ctx)
}

// heartbeatSchemaBroken reports whether the heartbeat table is known not to work with this
// release, either from the last schema check or from the writer's own failing writes. The
// latter covers the window before the next check.
func (pm *MultipoolerManager) heartbeatSchemaBroken() bool {
	if report := pm.heartbeatSchema.Load(); report != nil && !report.Compatible() {
		return true
	}
	return pm.replTracker != nil && pm.replTracker.HeartbeatWriter().SchemaBroken()
}
