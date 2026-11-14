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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// connectionDirectory is where the default primary reads how a tablegroup is served: the
// sharding catalog (connectionStore); the interface is the seam tests replace it through.
type connectionDirectory interface {
	// TableGroupBinding returns the backing connection of the tablegroup with the given
	// catalog id, which is nil when the tablegroup is managed, and false when there is no
	// such tablegroup. A connection's Name is the name it has in the catalog.
	TableGroupBinding(ctx context.Context, tablegroupID int64) (conn *multipoolerservicepb.BackingConnection, found bool, err error)
}

// connections returns the directory the default primary reads connections from: the
// sharding catalog, unless a test replaced it.
func (pm *MultipoolerManager) connections() connectionDirectory {
	if pm.connectionDir != nil {
		return pm.connectionDir
	}
	return newConnectionStore(pm)
}

// requireDefaultPrimary checks that this pooler is the current writable primary
// of the managed default tablegroup, the only place cluster metadata may be
// read. A follower's copy is never a source of truth.
func (pm *MultipoolerManager) requireDefaultPrimary(database string) error {
	key := pm.record.ShardRef()
	if database != key.GetDatabase() {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database does not match pooler")
	}
	// UNAVAILABLE, not FAILED_PRECONDITION: this pooler is not the default primary (a
	// replica, a demoted primary, one that is still being promoted) is something that can
	// change and says nothing about the state the call expects, so a caller rediscovers the
	// default primary and retries. FAILED_PRECONDITION is a definitive answer about the
	// persisted state, and a retry of a call that may have committed through a pooler that
	// has just lost its place would otherwise look definitive.
	if pm.IsUnmanaged() || !pm.isCatalogShard() {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary pooler")
	}
	if pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary pooler")
	}
	return nil
}

// GetBackingConnection says how a tablegroup is served, from the default primary's catalog:
// the backing connection of an external tablegroup, or that it is managed.
func (pm *MultipoolerManager) GetBackingConnection(ctx context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	return pm.tableGroupBinding(ctx, req.GetTablegroupId())
}

// tableGroupBinding reads the tablegroup's binding from the directory.
func (pm *MultipoolerManager) tableGroupBinding(ctx context.Context, tablegroupID int64) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	if tablegroupID <= 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup id is required")
	}
	conn, found, err := pm.connections().TableGroupBinding(ctx, tablegroupID)
	if err != nil {
		return nil, mterrors.Wrap(err, "read tablegroup binding")
	}
	if !found {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %d not found", tablegroupID)
	}
	if conn == nil {
		return &multipoolerservicepb.GetBackingConnectionResponse{Managed: true}, nil
	}
	return &multipoolerservicepb.GetBackingConnectionResponse{Connection: conn}, nil
}
