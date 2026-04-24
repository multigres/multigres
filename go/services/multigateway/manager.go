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

package multigateway

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/preparedstatement"
	multigatewaymanagerpb "github.com/multigres/multigres/go/pb/multigatewaymanager"
	multigatewaymanagerdatapb "github.com/multigres/multigres/go/pb/multigatewaymanagerdata"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"
)

// ManagerServer implements multigatewaymanagerpb.MultiGatewayManagerServer,
// exposing in-process diagnostic snapshots (registry, consolidator) for
// rendering in multiadmin-web.
type ManagerServer struct {
	multigatewaymanagerpb.UnimplementedMultiGatewayManagerServer

	registry *queryregistry.Registry
	handler  *handler.MultiGatewayHandler
}

// NewManagerServer constructs a ManagerServer wired to the gateway's shared
// registry and handler (for prepared-statement consolidator access).
func NewManagerServer(registry *queryregistry.Registry, h *handler.MultiGatewayHandler) *ManagerServer {
	return &ManagerServer{registry: registry, handler: h}
}

// RegisterWithGRPCServer registers the manager service on the given gRPC server.
func (s *ManagerServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multigatewaymanagerpb.RegisterMultiGatewayManagerServer(grpcServer, s)
}

// GetQueryRegistry returns the per-fingerprint registry snapshot.
func (s *ManagerServer) GetQueryRegistry(_ context.Context, _ *multigatewaymanagerpb.GetQueryRegistryRequest) (*multigatewaymanagerpb.GetQueryRegistryResponse, error) {
	snapshots := s.registry.Top(0, queryregistry.SortByCalls)
	out := make([]*multigatewaymanagerdatapb.QueryStatSnapshot, 0, len(snapshots))
	for i := range snapshots {
		out = append(out, snapshotToProto(&snapshots[i]))
	}
	return &multigatewaymanagerpb.GetQueryRegistryResponse{
		Snapshot: &multigatewaymanagerdatapb.QueryRegistrySnapshot{
			Queries:             out,
			TrackedFingerprints: uint32(s.registry.Len()),
		},
	}, nil
}

// GetConsolidatorStats returns a snapshot of the prepared-statement consolidator.
func (s *ManagerServer) GetConsolidatorStats(_ context.Context, _ *multigatewaymanagerpb.GetConsolidatorStatsRequest) (*multigatewaymanagerpb.GetConsolidatorStatsResponse, error) {
	stats := s.handler.Consolidator().Stats()
	return &multigatewaymanagerpb.GetConsolidatorStatsResponse{
		Stats: consolidatorStatsToProto(stats),
	}, nil
}

func snapshotToProto(s *queryregistry.Snapshot) *multigatewaymanagerdatapb.QueryStatSnapshot {
	return &multigatewaymanagerdatapb.QueryStatSnapshot{
		Fingerprint:       s.Fingerprint,
		NormalizedSql:     s.NormalizedSQL,
		Calls:             s.Calls,
		Errors:            s.Errors,
		TotalDuration:     durationpb.New(s.TotalDuration),
		AverageDuration:   durationpb.New(s.AverageDuration),
		MinDuration:       durationpb.New(s.MinDuration),
		MaxDuration:       durationpb.New(s.MaxDuration),
		P50Duration:       durationpb.New(s.P50Duration),
		P99Duration:       durationpb.New(s.P99Duration),
		TotalRows:         s.TotalRows,
		LastSeen:          timestamppb.New(s.LastSeen),
		SampleInterval:    durationpb.New(time.Duration(s.SampleIntervalSeconds * float64(time.Second))),
		CallRateTrends:    s.CallRateTrend,
		TotalTimeMsTrends: s.TotalTimeMsTrend,
		P50MsTrends:       s.P50MsTrend,
		P99MsTrends:       s.P99MsTrend,
		RowsRateTrends:    s.RowsRateTrend,
	}
}

func consolidatorStatsToProto(s preparedstatement.ConsolidatorStats) *multigatewaymanagerdatapb.ConsolidatorStats {
	out := &multigatewaymanagerdatapb.ConsolidatorStats{
		UniqueStatements: uint32(s.UniqueStatements),
		TotalReferences:  uint32(s.TotalReferences),
		ConnectionCount:  uint32(s.ConnectionCount),
	}
	for _, st := range s.Statements {
		out.PreparedStatements = append(out.PreparedStatements, &multigatewaymanagerdatapb.ConsolidatorPreparedStatement{
			Name:  st.Name,
			Query: st.Query,
			Refs:  uint32(st.UsageCount),
		})
	}
	return out
}
