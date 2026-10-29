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
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// connectionDirectory holds the backing connections of the cluster, which the default
// primary serves to the poolers that front an external PostgreSQL. It is the sharding
// catalog (connectionStore); the interface is the seam tests replace it through.
type connectionDirectory interface {
	// BackingConnection returns the named connection, and false when there is none of
	// that name. Its Name is set by the caller.
	BackingConnection(ctx context.Context, name string) (conn *multipoolerservicepb.BackingConnection, found bool, err error)
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
	if pm.IsUnmanaged() || key.GetTableGroup() != constants.DefaultTableGroup || key.GetShard() != constants.DefaultShard {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary pooler")
	}
	if pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary pooler")
	}
	return nil
}

// GetBackingConnection returns the named backing connection from the default
// primary's metadata.
func (pm *MultipoolerManager) GetBackingConnection(ctx context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	return pm.backingConnection(ctx, req.GetConnectionName())
}

// backingConnection reads the named connection from the directory.
func (pm *MultipoolerManager) backingConnection(ctx context.Context, name string) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	if name == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "connection name is required")
	}
	conn, found, err := pm.connections().BackingConnection(ctx, name)
	if err != nil {
		return nil, mterrors.Wrap(err, "read backing connection")
	}
	if !found {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "backing connection %q not found", name)
	}
	conn.Name = name
	return &multipoolerservicepb.GetBackingConnectionResponse{Connection: conn}, nil
}
