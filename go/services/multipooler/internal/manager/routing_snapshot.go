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
	"encoding/hex"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// The read side of the sharding catalog: the snapshot gateways cache, and the
// routing_version that tells them when to fetch a new one.
//
// routing_version (multigres.catalog_state) advances by one in every catalog
// transaction that changes what a snapshot contains, so two snapshots at the
// same version are identical. Every catalog write goes through writeCatalog,
// which owns that protocol: it locks the catalog_state row before the writer
// touches anything else; when the writer reports a routing change, it checks
// the resulting snapshot and advances the version once (and plan_epoch with it
// when the change invalidates cached plans); then it commits and publishes the
// new versions. Writes outside the snapshot
// (operation rows, for instance) go through it too, and commit without
// advancing anything. Holding the lock for
// the whole transaction serializes catalog writers, and taking it first in
// every writer is the lock order: catalog_state, then the other catalog rows,
// never the other way round. A writer must not begin its own transaction.
// The one other catalog writer, ensureCatalogSchema, keeps the same lock
// order and advances the versions as its migrations declare.
//
// The catalog primary also publishes the versions on its health stream (see
// healthStreamer.observeCatalogVersions). It reads them only from committed
// state: under synchronous replication PostgreSQL makes a commit visible to
// other sessions only after the synchronous standbys have acknowledged it
// (RecordTransactionCommit waits in SyncRepWaitForLSN before
// ProcArrayEndTransaction), so a version read on the primary, or returned by a
// committed write, has been acknowledged under the shard's durability policy
// and survives a failover. The exception is a commit whose wait for the
// standbys was cancelled or whose backend was terminated while waiting:
// PostgreSQL then makes it visible locally without the acknowledgement. Such a
// version can be lost in a failover and reused by the next primary with
// different contents, so a gateway that cached it would not see the change
// until the version moves past it.

// Shard index and shard key states and roles, matching the CHECK constraints
// in pg_catalog_schema.go.
const (
	shardStateActive = "active"

	shardKeyRolePrimary   = "primary"
	shardKeyRoleSecondary = "secondary"
)

// The catalog stores states and roles as text; the snapshot carries them as
// enums. A value missing from these maps reads as UNSPECIFIED, which a reader
// treats as unknown.
var (
	snapshotTableGroupStates = map[string]shardingcatalogpb.TableGroupState{
		tableGroupStateCreating: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_CREATING,
		tableGroupStateActive:   shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE,
		tableGroupStateDropping: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_DROPPING,
	}
	snapshotShardStates = map[string]shardingcatalogpb.ShardState{
		shardStatePreparing: shardingcatalogpb.ShardState_SHARD_STATE_PREPARING,
		shardStateActive:    shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE,
		shardStateRetired:   shardingcatalogpb.ShardState_SHARD_STATE_RETIRED,
	}
	snapshotShardIndexStates = map[string]shardingcatalogpb.ShardIndexState{
		shardStatePreparing: shardingcatalogpb.ShardIndexState_SHARD_INDEX_STATE_PREPARING,
		shardStateActive:    shardingcatalogpb.ShardIndexState_SHARD_INDEX_STATE_ACTIVE,
		shardStateRetired:   shardingcatalogpb.ShardIndexState_SHARD_INDEX_STATE_RETIRED,
	}
	snapshotShardKeyStates = map[string]shardingcatalogpb.ShardKeyState{
		shardStatePreparing: shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_PREPARING,
		shardStateActive:    shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_ACTIVE,
		shardStateRetired:   shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_RETIRED,
	}
	snapshotShardKeyRoles = map[string]shardingcatalogpb.ShardKeyRole{
		shardKeyRolePrimary:   shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_PRIMARY,
		shardKeyRoleSecondary: shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_SECONDARY,
	}
)

// catalogVersionRefreshInterval is how often the catalog primary rereads
// catalog_state for its health stream. Its own writes publish at once; the
// interval is what surfaces a version written by a previous primary.
const catalogVersionRefreshInterval = 5 * time.Second

// catalogVersions is the catalog_state row's routing_version and plan_epoch.
type catalogVersions struct {
	routingVersion int64
	planEpoch      int64
}

const catalogVersionsQuery = "SELECT routing_version, plan_epoch FROM multigres.catalog_state"

// catalogChange is what a writeCatalog writer's change does to the catalog
// versions. The transaction commits whichever it is; only an error from the
// writer rolls it back. Values are ordered by strength, so the max of several
// changes is the change they make together (see ensureCatalogSchema).
type catalogChange int

const (
	// catalogNoSnapshotChange means the writer changed nothing a snapshot
	// contains: it wrote nothing, or only rows outside the snapshot such as
	// operation rows. No version advances.
	catalogNoSnapshotChange catalogChange = iota
	// catalogRoutingChange means the writer changed rows a snapshot contains;
	// routing_version advances.
	catalogRoutingChange
	// catalogInvalidatesPlans means the writer changed rows a snapshot
	// contains in a way that can change how an existing query is planned, so
	// cached plans must be invalidated: routing_version and plan_epoch both
	// advance. Every such change is also a routing change.
	catalogInvalidatesPlans
)

// catalogWriter is the body of a catalog write transaction. It runs with the
// catalog_state lock already held and must leave the transaction open: it
// neither commits nor rolls back. What it wrote commits unless it returns an
// error, which rolls everything back.
type catalogWriter func(ctx context.Context, tx executor.InternalTx) (catalogChange, error)

// writeCatalog runs write in a catalog write transaction on the admin pool,
// under tableGroupOpTimeout. See the file comment for the protocol it owns.
// It returns the advanced versions, or zero versions for
// catalogNoSnapshotChange, where nothing advances and nothing is published.
func (pm *MultipoolerManager) writeCatalog(ctx context.Context, write catalogWriter) (catalogVersions, error) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return catalogVersions{}, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	tx, err := qs.BeginAdmin(execCtx)
	if err != nil {
		return catalogVersions{}, mterrors.Wrap(err, "failed to begin catalog transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockCatalogState(execCtx, tx); err != nil {
		return catalogVersions{}, err
	}

	change, err := write(execCtx, tx)
	if err != nil {
		return catalogVersions{}, err
	}
	var invalidatePlans bool
	switch change {
	case catalogNoSnapshotChange:
		if err := tx.Commit(execCtx); err != nil {
			return catalogVersions{}, mterrors.Wrap(err, "failed to commit catalog transaction")
		}
		return catalogVersions{}, nil
	case catalogRoutingChange:
	case catalogInvalidatesPlans:
		invalidatePlans = true
	default:
		return catalogVersions{}, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown catalog change %d", change)
	}

	if err := validateCatalogWrite(execCtx, tx); err != nil {
		return catalogVersions{}, err
	}
	versions, err := advanceCatalogVersions(execCtx, tx, invalidatePlans)
	if err != nil {
		return catalogVersions{}, err
	}
	if err := tx.Commit(execCtx); err != nil {
		return catalogVersions{}, mterrors.Wrap(err, "failed to commit catalog transaction")
	}
	pm.publishCatalogWrite(versions)
	return versions, nil
}

// validateCatalogWrite reads the snapshot the writer's transaction would
// commit, its own writes included, and checks it the way gateways do
// (shardingcatalog.New). The catalog tables enforce none of the cross-row
// rules (shard layouts, the default tablegroup, one active primary shard key
// per table), so this is where a writer that breaks one fails, before its
// change commits, rather than every gateway rejecting the snapshot after it.
// Writes outside the snapshot skip it, so a catalog that is already invalid
// never blocks bookkeeping.
func validateCatalogWrite(ctx context.Context, tx executor.InternalTx) error {
	snapshot, err := loadRoutingSnapshot(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := shardingcatalog.New(snapshot); err != nil {
		return mterrors.WithCode(mterrors.Wrap(err, "catalog write would leave an invalid routing snapshot"), mtrpcpb.Code_INTERNAL)
	}
	return nil
}

// lockCatalogState locks the catalog_state row for the rest of the
// transaction. Only writeCatalog calls it, before the writer runs.
func lockCatalogState(ctx context.Context, tx executor.InternalTx) error {
	if _, err := tx.Query(ctx, "SELECT routing_version FROM multigres.catalog_state FOR UPDATE"); err != nil {
		return mterrors.Wrap(err, "failed to lock catalog state")
	}
	return nil
}

// advanceCatalogVersions advances routing_version by one, and plan_epoch too
// when invalidatePlans is set, in the caller's transaction, which holds the
// lock from lockCatalogState. It returns the new versions to publish once the
// transaction has committed. Only writeCatalog calls it.
func advanceCatalogVersions(ctx context.Context, tx executor.InternalTx, invalidatePlans bool) (catalogVersions, error) {
	query := "UPDATE multigres.catalog_state SET routing_version = routing_version + 1 RETURNING routing_version, plan_epoch"
	if invalidatePlans {
		query = "UPDATE multigres.catalog_state SET routing_version = routing_version + 1, plan_epoch = plan_epoch + 1 RETURNING routing_version, plan_epoch"
	}
	res, err := tx.Query(ctx, query)
	if err != nil {
		return catalogVersions{}, mterrors.Wrap(err, "failed to advance catalog versions")
	}
	var v catalogVersions
	if err := executor.ScanSingleRow(res, &v.routingVersion, &v.planEpoch); err != nil {
		return catalogVersions{}, mterrors.Wrap(err, "failed to read advanced catalog versions")
	}
	return v, nil
}

// GetRoutingSnapshot returns the sharding catalog, or reports that the
// caller's copy at since_version is current. See the RPC's proto comment.
func (pm *MultipoolerManager) GetRoutingSnapshot(ctx context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	if err := pm.checkCatalogShard(); err != nil {
		return nil, err
	}
	if err := pm.checkCatalogPrimary(ctx); err != nil {
		return nil, err
	}
	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	tx, err := qs.BeginAdmin(execCtx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to begin catalog snapshot transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// One snapshot for every table, so the versions and every row agree.
	if _, err := tx.Query(execCtx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
		return nil, mterrors.Wrap(err, "failed to set catalog snapshot isolation")
	}
	res, err := tx.Query(execCtx, catalogVersionsQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read catalog versions")
	}
	var v catalogVersions
	if err := executor.ScanSingleRow(res, &v.routingVersion, &v.planEpoch); err != nil {
		return nil, mterrors.Wrap(err, "failed to read catalog versions")
	}
	out := &multipoolermanagerdatapb.GetRoutingSnapshotResponse{RoutingVersion: v.routingVersion, PlanEpoch: v.planEpoch}
	if req.SinceVersion != nil && req.GetSinceVersion() == v.routingVersion {
		out.Unchanged = true
		return out, nil
	}
	snapshot, err := loadRoutingSnapshot(execCtx, tx)
	if err != nil {
		return nil, err
	}
	snapshot.RoutingVersion = v.routingVersion
	snapshot.PlanEpoch = v.planEpoch
	out.Snapshot = snapshot
	return out, nil
}

// The statements loadRoutingSnapshot reads the catalog with, one per table.
// NULL range bounds (the ends of the keyspace) read as empty text.
const (
	snapshotTableGroupsQuery     = "SELECT tablegroup_id, name, state FROM multigres.tablegroup ORDER BY tablegroup_id"
	snapshotShardsQuery          = "SELECT shard_id, tablegroup_id, shard_name, coalesce(encode(range_start, 'hex'), ''), coalesce(encode(range_end, 'hex'), ''), state FROM multigres.shard ORDER BY shard_id"
	snapshotTablesQuery          = "SELECT table_id, schema_name, table_name, tablegroup_id FROM multigres.tablegroup_table ORDER BY table_id"
	snapshotShardIndicesQuery    = "SELECT shard_index_id, name, type, params::text, state FROM multigres.shard_index ORDER BY shard_index_id"
	snapshotShardKeysQuery       = "SELECT shard_key_id, table_id, shard_index_id, name, role, state FROM multigres.shard_key ORDER BY shard_key_id"
	snapshotShardKeyColumnsQuery = "SELECT shard_key_id, column_name FROM multigres.shard_key_column ORDER BY shard_key_id, position"
)

// loadRoutingSnapshot reads every catalog entity in the caller's transaction.
// It leaves the versions for the caller to fill in.
func loadRoutingSnapshot(ctx context.Context, tx executor.InternalTx) (*shardingcatalogpb.Snapshot, error) {
	s := &shardingcatalogpb.Snapshot{}

	res, err := tx.Query(ctx, snapshotTableGroupsQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read tablegroups")
	}
	for _, row := range res.Rows {
		var tg shardingcatalogpb.TableGroup
		var state string
		if err := executor.ScanRow(row, &tg.TablegroupId, &tg.Name, &state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read tablegroup row")
		}
		tg.State = snapshotTableGroupStates[state]
		s.Tablegroups = append(s.Tablegroups, &tg)
	}

	res, err = tx.Query(ctx, snapshotShardsQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read shards")
	}
	for _, row := range res.Rows {
		var sh shardingcatalogpb.Shard
		var start, end, state string
		if err := executor.ScanRow(row, &sh.ShardId, &sh.TablegroupId, &sh.Name, &start, &end, &state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard row")
		}
		kr := &clustermetadatapb.KeyRange{}
		if kr.Start, err = hex.DecodeString(start); err != nil {
			return nil, mterrors.Wrapf(err, "shard %d has an unreadable range start", sh.ShardId)
		}
		if kr.End, err = hex.DecodeString(end); err != nil {
			return nil, mterrors.Wrapf(err, "shard %d has an unreadable range end", sh.ShardId)
		}
		sh.KeyRange = kr
		sh.State = snapshotShardStates[state]
		s.Shards = append(s.Shards, &sh)
	}

	res, err = tx.Query(ctx, snapshotTablesQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read tables")
	}
	for _, row := range res.Rows {
		var t shardingcatalogpb.Table
		if err := executor.ScanRow(row, &t.TableId, &t.SchemaName, &t.TableName, &t.TablegroupId); err != nil {
			return nil, mterrors.Wrap(err, "failed to read table row")
		}
		s.Tables = append(s.Tables, &t)
	}

	res, err = tx.Query(ctx, snapshotShardIndicesQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read shard indexes")
	}
	for _, row := range res.Rows {
		var si shardingcatalogpb.ShardIndex
		var state string
		if err := executor.ScanRow(row, &si.ShardIndexId, &si.Name, &si.Type, &si.Params, &state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard index row")
		}
		si.State = snapshotShardIndexStates[state]
		s.ShardIndices = append(s.ShardIndices, &si)
	}

	res, err = tx.Query(ctx, snapshotShardKeysQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read shard keys")
	}
	keys := make(map[int64]*shardingcatalogpb.ShardKey, len(res.Rows))
	for _, row := range res.Rows {
		var k shardingcatalogpb.ShardKey
		var role, state string
		if err := executor.ScanRow(row, &k.ShardKeyId, &k.TableId, &k.ShardIndexId, &k.Name, &role, &state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard key row")
		}
		k.Role = snapshotShardKeyRoles[role]
		k.State = snapshotShardKeyStates[state]
		keys[k.ShardKeyId] = &k
		s.ShardKeys = append(s.ShardKeys, &k)
	}

	res, err = tx.Query(ctx, snapshotShardKeyColumnsQuery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read shard key columns")
	}
	for _, row := range res.Rows {
		var id int64
		var column string
		if err := executor.ScanRow(row, &id, &column); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard key column row")
		}
		// shard_key_column references shard_key, so every row has its key.
		if k, ok := keys[id]; ok {
			k.Columns = append(k.Columns, column)
		}
	}
	return s, nil
}

// refreshCatalogVersions rereads catalog_state and publishes it on the health
// stream when this pooler is the catalog primary, and clears what it
// published otherwise.
func (pm *MultipoolerManager) refreshCatalogVersions(ctx context.Context) {
	if pm.healthStreamer == nil {
		return
	}
	if pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		pm.healthStreamer.clearCatalogVersions()
		return
	}
	qs := pm.internalQueryService()
	if qs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdmin(ctx, catalogVersionsQuery)
	if err != nil {
		pm.logger.DebugContext(ctx, "failed to read catalog versions", "error", err)
		return
	}
	var v catalogVersions
	if err := executor.ScanSingleRow(res, &v.routingVersion, &v.planEpoch); err != nil {
		pm.logger.WarnContext(ctx, "failed to read catalog versions", "error", err)
		return
	}
	pm.healthStreamer.observeCatalogVersions(v)
}

// runCatalogVersionRefresh rereads catalog_state every interval until ctx
// ends. It runs only on poolers of the catalog shard.
func (pm *MultipoolerManager) runCatalogVersionRefresh(ctx context.Context, interval time.Duration) {
	pm.refreshCatalogVersions(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.refreshCatalogVersions(ctx)
		}
	}
}

// publishCatalogWrite publishes the versions a committed catalog write
// produced, so gateways learn of it without waiting for the next refresh.
func (pm *MultipoolerManager) publishCatalogWrite(v catalogVersions) {
	if pm.healthStreamer != nil {
		pm.healthStreamer.observeCatalogVersions(v)
	}
}
