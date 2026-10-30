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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// fakeRoutingManager is a health provider that also serves routing, as the
// manager does.
type fakeRoutingManager struct {
	poolerserver.HealthProvider
	resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse
	err  error
	got  *multipoolermanagerdatapb.GetRoutingSnapshotRequest
}

func (f *fakeRoutingManager) GetRoutingSnapshot(_ context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	f.got = req
	return f.resp, f.err
}

func newRoutingTestService(provider poolerserver.HealthProvider) *poolerService {
	// The pooler is not serving: a snapshot read does not go through query
	// admission, so it is answered anyway.
	p := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, &clustermetadatapb.ID{Name: "p"}, "default", "0-inf", provider, 0, false)
	return &poolerService{pooler: p}
}

func TestGetRoutingSnapshot_ServedByTheManager(t *testing.T) {
	want := &multipoolermanagerdatapb.GetRoutingSnapshotResponse{RoutingVersion: 3, Full: true}
	mgr := &fakeRoutingManager{resp: want}
	req := &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: "db1", SinceVersion: 2}

	resp, err := newRoutingTestService(mgr).GetRoutingSnapshot(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, want, resp)
	assert.Equal(t, req, mgr.got)
}

func TestGetRoutingSnapshot_ManagerErrorKeepsItsCode(t *testing.T) {
	mgr := &fakeRoutingManager{err: mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the catalog primary pooler")}
	_, err := newRoutingTestService(mgr).GetRoutingSnapshot(context.Background(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: "db1"})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Contains(t, err.Error(), "not the catalog primary pooler")
}

func TestGetRoutingSnapshot_NoRoutingProvider(t *testing.T) {
	_, err := newRoutingTestService(nil).GetRoutingSnapshot(context.Background(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: "db1"})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}
