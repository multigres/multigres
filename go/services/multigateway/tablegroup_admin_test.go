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

package multigateway

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// fakeManagerClient answers the tablegroup RPCs and records what it was sent.
// Any other RPC panics on the nil embedded client.
type fakeManagerClient struct {
	multipoolermanagerpb.MultipoolerManagerClient
	err      error
	got      []any
	deadline bool
}

func (f *fakeManagerClient) record(ctx context.Context, req any) {
	f.got = append(f.got, req)
	_, f.deadline = ctx.Deadline()
}

func (f *fakeManagerClient) CreateTableGroup(ctx context.Context, req *multipoolermanagerdatapb.CreateTableGroupRequest, _ ...grpc.CallOption) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	f.record(ctx, req)
	if f.err != nil {
		return nil, f.err
	}
	return &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: "create-op"}, nil
}

func (f *fakeManagerClient) DropTableGroup(ctx context.Context, req *multipoolermanagerdatapb.DropTableGroupRequest, _ ...grpc.CallOption) (*multipoolermanagerdatapb.DropTableGroupResponse, error) {
	f.record(ctx, req)
	if f.err != nil {
		return nil, f.err
	}
	return &multipoolermanagerdatapb.DropTableGroupResponse{OperationId: "drop-op"}, nil
}

func (f *fakeManagerClient) GetOperation(ctx context.Context, req *multipoolermanagerdatapb.GetOperationRequest, _ ...grpc.CallOption) (*multipoolermanagerdatapb.GetOperationResponse, error) {
	f.record(ctx, req)
	if f.err != nil {
		return nil, f.err
	}
	return &multipoolermanagerdatapb.GetOperationResponse{Operation: &multipoolermanagerdatapb.Operation{OperationId: req.OperationId}}, nil
}

func TestTableGroupAdmin_ForwardsToTheCatalogPooler(t *testing.T) {
	fake := &fakeManagerClient{}
	var databases []string
	a := &tableGroupAdmin{client: func(database string) (multipoolermanagerpb.MultipoolerManagerClient, error) {
		databases = append(databases, database)
		return fake, nil
	}}
	ctx := t.Context()

	createReq := &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"}
	created, err := a.CreateTableGroup(ctx, "db", createReq)
	require.NoError(t, err)
	assert.Equal(t, "create-op", created.GetOperationId())

	dropReq := &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"}
	dropped, err := a.DropTableGroup(ctx, "db", dropReq)
	require.NoError(t, err)
	assert.Equal(t, "drop-op", dropped.GetOperationId())

	op, err := a.GetOperation(ctx, "db", "op-1")
	require.NoError(t, err)
	assert.Equal(t, "op-1", op.GetOperationId())

	assert.Equal(t, []string{"db", "db", "db"}, databases)
	require.Len(t, fake.got, 3)
	assert.Same(t, createReq, fake.got[0])
	assert.Same(t, dropReq, fake.got[1])
	assert.Equal(t, "op-1", fake.got[2].(*multipoolermanagerdatapb.GetOperationRequest).GetOperationId())
	assert.True(t, fake.deadline, "every call is bounded by tableGroupAdminCallTimeout")
}

func TestTableGroupAdmin_PassesErrorsThrough(t *testing.T) {
	ctx := t.Context()

	t.Run("RPC error", func(t *testing.T) {
		rpcErr := status.Error(codes.FailedPrecondition, "not the catalog primary")
		a := &tableGroupAdmin{client: func(string) (multipoolermanagerpb.MultipoolerManagerClient, error) {
			return &fakeManagerClient{err: rpcErr}, nil
		}}
		_, err := a.CreateTableGroup(ctx, "db", &multipoolermanagerdatapb.CreateTableGroupRequest{})
		assert.Equal(t, rpcErr, err)
		_, err = a.DropTableGroup(ctx, "db", &multipoolermanagerdatapb.DropTableGroupRequest{})
		assert.Equal(t, rpcErr, err)
		_, err = a.GetOperation(ctx, "db", "op-1")
		assert.Equal(t, rpcErr, err)
	})

	t.Run("no client", func(t *testing.T) {
		connErr := errors.New("no pooler")
		a := &tableGroupAdmin{client: func(string) (multipoolermanagerpb.MultipoolerManagerClient, error) {
			return nil, connErr
		}}
		_, err := a.CreateTableGroup(ctx, "db", &multipoolermanagerdatapb.CreateTableGroupRequest{})
		assert.Equal(t, connErr, err)
		_, err = a.DropTableGroup(ctx, "db", &multipoolermanagerdatapb.DropTableGroupRequest{})
		assert.Equal(t, connErr, err)
		_, err = a.GetOperation(ctx, "db", "op-1")
		assert.Equal(t, connErr, err)
	})
}

func TestNewTableGroupAdmin_NoCatalogPoolerIsUnavailable(t *testing.T) {
	mg, _ := newTestGateway(t, "zone1")
	a := newTableGroupAdmin(mg.poolerGateway)

	_, err := a.GetOperation(t.Context(), "db", "op-1")
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Contains(t, err.Error(), `catalog shard of database "db"`)
}
