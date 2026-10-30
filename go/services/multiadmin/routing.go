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
package multiadmin

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/netutil"
)

// The routing RPCs are thin forwarders to the primary of the database's
// default tablegroup, which holds the database's catalog and validates every
// request itself. multiadmin holds no routing state.

// catalogPrimary returns the pooler that holds database's catalog: the leader
// of its default tablegroup.
func (s *MultiadminServer) catalogPrimary(ctx context.Context, database string) (*clustermetadatapb.Multipooler, error) {
	if database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	pooler, err := s.findPooler(ctx, database, constants.DefaultTableGroup, constants.DefaultShard, true, false)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "no catalog primary for database %q: %v", database, err)
	}
	return pooler, nil
}

// GetRoutingSnapshot forwards to the query service of the database's catalog
// primary, where gateways read routing too.
func (s *MultiadminServer) GetRoutingSnapshot(ctx context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	addr := netutil.JoinHostPort(pooler.Hostname, pooler.PortMap["grpc"])
	conn, err := s.migrationDialer(ctx, addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial pooler %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	return multipoolerpb.NewMultipoolerServiceClient(conn).GetRoutingSnapshot(ctx, req)
}

// SetRoutingRules forwards to the database's catalog primary.
func (s *MultiadminServer) SetRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.SetRoutingRulesRequest) (*multipoolermanagerdatapb.SetRoutingRulesResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.SetRoutingRules(ctx, pooler, req)
}

// DeleteRoutingRules forwards to the database's catalog primary.
func (s *MultiadminServer) DeleteRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.DeleteRoutingRulesRequest) (*multipoolermanagerdatapb.DeleteRoutingRulesResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.DeleteRoutingRules(ctx, pooler, req)
}

// EnsureImportTarget forwards to the database's catalog primary.
func (s *MultiadminServer) EnsureImportTarget(ctx context.Context, req *multipoolermanagerdatapb.EnsureImportTargetRequest) (*multipoolermanagerdatapb.EnsureImportTargetResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.EnsureImportTarget(ctx, pooler, req)
}
