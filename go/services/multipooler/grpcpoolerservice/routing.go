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
package grpcpoolerservice

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// GetRoutingSnapshot returns a database's routing rules. Only the catalog
// primary serves it; the manager enforces that. It is control traffic, so it
// is not admitted through StartRequest: it is served while the pooler drains
// and is never held by query admission.
func (s *poolerService) GetRoutingSnapshot(ctx context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	if s.pooler == nil {
		return nil, status.Error(codes.Unavailable, "pooler not initialized")
	}
	provider, err := s.pooler.RoutingProvider()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resp, err := provider.GetRoutingSnapshot(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
