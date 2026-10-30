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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// requireCatalogPrimary checks that this pooler is the writable primary of
// database's default tablegroup, which holds the catalog and is the only
// place routing rules may be written. A standby's copy is never a source of
// truth.
func (pm *MultipoolerManager) requireCatalogPrimary(database string) error {
	if database == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database is required")
	}
	key := pm.record.ShardRef()
	if database != key.GetDatabase() {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"database %q does not match this pooler's database %q", database, key.GetDatabase())
	}
	// UNAVAILABLE, not FAILED_PRECONDITION: not being the catalog primary (a
	// pooler of another tablegroup, a standby, a demoted primary, one still
	// being promoted) says nothing about the catalog's state, so the caller
	// rediscovers the catalog primary and retries.
	if !constants.IsCatalogShard(key.GetTableGroup(), key.GetShard()) ||
		pm.stateManager == nil ||
		pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the catalog primary pooler")
	}
	return nil
}

// SetRoutingRules replaces every routing rule of an operation.
func (pm *MultipoolerManager) SetRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.SetRoutingRulesRequest) (*multipoolermanagerdatapb.SetRoutingRulesResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	change := routingRuleChange{operationID: req.GetOperationId(), expectedRoutingVersion: req.GetExpectedRoutingVersion()}
	for _, r := range req.GetRules() {
		change.rules = append(change.rules, shardRuleSpec{
			fromTablegroup: r.GetFromTablegroup(),
			toTablegroup:   r.GetToTablegroup(),
			toShard:        r.GetToShard(),
		})
	}
	if len(change.rules) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "no routing rules; use DeleteRoutingRules to delete an operation's rules")
	}
	versions, err := pm.writeRoutingRules(ctx, change)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.SetRoutingRulesResponse{RoutingVersion: versions.routingVersion}, nil
}

// DeleteRoutingRules deletes every routing rule of an operation.
func (pm *MultipoolerManager) DeleteRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.DeleteRoutingRulesRequest) (*multipoolermanagerdatapb.DeleteRoutingRulesResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	versions, err := pm.writeRoutingRules(ctx, routingRuleChange{
		operationID:            req.GetOperationId(),
		expectedRoutingVersion: req.GetExpectedRoutingVersion(),
	})
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.DeleteRoutingRulesResponse{RoutingVersion: versions.routingVersion}, nil
}

// EnsureImportTarget creates, if absent, an import operation and the
// tablegroup it imports into with its full-range shard, in one catalog write.
// It writes no routing rule; creating the tablegroup or its shard advances
// the routing version, as every catalog change does.
//
// It is temporary, for the first version of imports, and will be deprecated:
// tablegroup creation (the equivalent of CREATE TABLEGROUP) plus the import's
// PREPARE step (registering its tables, the default rule and fencing) will
// replace it.
func (pm *MultipoolerManager) EnsureImportTarget(ctx context.Context, req *multipoolermanagerdatapb.EnsureImportTargetRequest) (*multipoolermanagerdatapb.EnsureImportTargetResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	versions, err := pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, _ catalogVersions) (catalogChange, error) {
		return ensureImportTarget(ctx, tx, req.GetOperationId(), req.GetTablegroup())
	})
	if err != nil {
		return nil, err
	}
	pm.logger.InfoContext(ctx, "ensured import target",
		"operation_id", req.GetOperationId(), "tablegroup", req.GetTablegroup(), "routing_version", versions.routingVersion)
	return &multipoolermanagerdatapb.EnsureImportTargetResponse{}, nil
}

// writeRoutingRules writes change through changeRoutingRules and logs the
// routing version it leaves.
func (pm *MultipoolerManager) writeRoutingRules(ctx context.Context, change routingRuleChange) (catalogVersions, error) {
	versions, err := pm.changeRoutingRules(ctx, change)
	if err != nil {
		return catalogVersions{}, err
	}
	pm.logger.InfoContext(ctx, "routing rules changed",
		"operation_id", change.operationID, "rules", len(change.rules), "routing_version", versions.routingVersion)
	return versions, nil
}
