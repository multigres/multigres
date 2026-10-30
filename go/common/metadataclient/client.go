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

// Package metadataclient discovers the default primary pooler and talks to it.
// The default primary owns cluster metadata, so other poolers read it there and
// never from a local follower.
package metadataclient

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// Poolers lists every pooler of database across all cells. A partial listing is
// an error: an unseen pooler could be the default primary.
func Poolers(ctx context.Context, ts topoclient.Store, database string) ([]*clustermetadatapb.Multipooler, error) {
	cells, err := ts.GetCellNames(ctx)
	if err != nil {
		return nil, err
	}
	var result []*clustermetadatapb.Multipooler
	for _, cell := range cells {
		list, err := ts.GetMultipoolersByCell(ctx, cell, &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{Database: database},
		})
		if err != nil {
			return nil, err
		}
		for _, item := range list {
			result = append(result, item.Multipooler)
		}
	}
	return result, nil
}

// isDefaultCohortMember reports whether p belongs to the managed default
// tablegroup and shard, the cohort that holds cluster metadata.
func isDefaultCohortMember(p *clustermetadatapb.Multipooler) bool {
	switch p.GetManagementMode() {
	case clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNSPECIFIED,
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED:
	default:
		return false
	}
	key := p.GetShardRef()
	return key.GetTableGroup() == constants.DefaultTableGroup && key.GetShard() == constants.DefaultShard
}

// DefaultPrimary returns the writable primary of the default tablegroup. Serving
// status is never a leadership condition. When several poolers claim primary the
// higher consensus rule wins; equal rules from different poolers fail closed
// rather than picking an arbitrary process.
func DefaultPrimary(poolers []*clustermetadatapb.Multipooler) (*clustermetadatapb.Multipooler, error) {
	var leader *clustermetadatapb.Multipooler
	ambiguous := false
	for _, p := range poolers {
		if !isDefaultCohortMember(p) || p.GetRoutingState().GetRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
			continue
		}
		if leader == nil {
			leader = p
			continue
		}
		switch compareRules(p.GetRoutingState().GetRule(), leader.GetRoutingState().GetRule()) {
		case 0:
			ambiguous = true
		case 1:
			leader = p
			ambiguous = false
		}
	}
	if leader == nil {
		return nil, errors.New("default primary unavailable")
	}
	if ambiguous {
		return nil, errors.New("ambiguous default primary")
	}
	return leader, nil
}

func compareRules(a, b *clustermetadatapb.RuleNumber) int {
	switch {
	case a.GetCoordinatorTerm() != b.GetCoordinatorTerm():
		if a.GetCoordinatorTerm() > b.GetCoordinatorTerm() {
			return 1
		}
		return -1
	case a.GetLeaderSubterm() != b.GetLeaderSubterm():
		if a.GetLeaderSubterm() > b.GetLeaderSubterm() {
			return 1
		}
		return -1
	default:
		return 0
	}
}

// Dial connects to p's gRPC endpoint with the given transport credentials.
func Dial(p *clustermetadatapb.Multipooler, transport grpc.DialOption) (*grpc.ClientConn, error) {
	return grpccommon.NewClient((&topoclient.MultipoolerInfo{Multipooler: p}).Addr(), grpccommon.WithDialOptions(transport))
}

// WithDefaultPrimary resolves the default primary of database from topology,
// dials it, and runs fn against its pooler service.
func WithDefaultPrimary(ctx context.Context, ts topoclient.Store, database string, transport grpc.DialOption, fn func(multipoolerservicepb.MultipoolerServiceClient) error) error {
	list, err := Poolers(ctx, ts, database)
	if err != nil {
		return err
	}
	primary, err := DefaultPrimary(list)
	if err != nil {
		return err
	}
	conn, err := Dial(primary, transport)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(multipoolerservicepb.NewMultipoolerServiceClient(conn))
}
