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

package multiadmin

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ListDesiredTableGroups returns the tablegroups a provisioner should have
// provisioned for a database. Multiadmin owns nothing here: it finds the
// primary pooler of the database's catalog shard, which is the sole writer
// and reader of the sharding catalog, and forwards the request to it.
func (s *MultiadminServer) ListDesiredTableGroups(ctx context.Context, req *multiadminpb.ListDesiredTableGroupsRequest) (*multiadminpb.ListDesiredTableGroupsResponse, error) {
	if req.GetDatabase() == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	catalogShard := &clustermetadatapb.ShardRef{
		Database:   req.GetDatabase(),
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}
	primary, _, err := s.findShardPrimary(ctx, catalogShard)
	if err != nil {
		return nil, err
	}
	resp, err := s.rpcClient.ListTableGroups(ctx, primary, &multipoolermanagerdatapb.ListTableGroupsRequest{})
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to list tablegroups from catalog primary", "database", req.GetDatabase(), "error", err)
		return nil, status.Errorf(codes.Unavailable, "failed to list tablegroups from catalog primary: %v", err)
	}
	return &multiadminpb.ListDesiredTableGroupsResponse{TableGroups: resp.GetTableGroups()}, nil
}
