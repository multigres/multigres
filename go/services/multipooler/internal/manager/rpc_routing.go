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

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// checkRoutingRuleCaller checks that a rule RPC for database reached the
// catalog primary: the database is this pooler's, and this pooler is the
// elected primary of the catalog shard, by the same checkCatalogShard and
// checkCatalogPrimary every catalog writer uses.
func (pm *MultipoolerManager) checkRoutingRuleCaller(ctx context.Context, database string) error {
	if database == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database is required")
	}
	if own := pm.record.ShardRef().GetDatabase(); database != own {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"database %q does not match this pooler's database %q", database, own)
	}
	// UNAVAILABLE, not the FAILED_PRECONDITION the checks return: not being
	// the catalog primary (a pooler of another tablegroup, a standby, a
	// demoted primary, one still being promoted) says nothing about the
	// catalog's state, so the caller rediscovers the catalog primary and
	// retries.
	if err := pm.checkCatalogShard(); err != nil {
		return mterrors.WithCode(err, mtrpcpb.Code_UNAVAILABLE)
	}
	if err := pm.checkCatalogPrimary(ctx); err != nil {
		return mterrors.WithCode(err, mtrpcpb.Code_UNAVAILABLE)
	}
	return nil
}

// SetRoutingRules replaces every routing rule of an operation.
func (pm *MultipoolerManager) SetRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.SetRoutingRulesRequest) (*multipoolermanagerdatapb.SetRoutingRulesResponse, error) {
	if err := pm.checkRoutingRuleCaller(ctx, req.GetDatabase()); err != nil {
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
	if err := pm.checkRoutingRuleCaller(ctx, req.GetDatabase()); err != nil {
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
