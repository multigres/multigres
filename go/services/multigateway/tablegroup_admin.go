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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
)

// tableGroupAdminCallTimeout bounds one RPC to the catalog primary. Each is a
// short catalog transaction or read; the statement's own wait loop retries.
const tableGroupAdminCallTimeout = 30 * time.Second

// tableGroupAdmin implements engine.TableGroupAdmin by calling the pooler
// manager service on the catalog shard's writable pooler, over the gateway's
// existing connection to it. GetOperation goes to the same pooler, so a
// statement never reads a replica that lags its own write.
type tableGroupAdmin struct {
	pg *poolergateway.PoolerGateway
}

var _ engine.TableGroupAdmin = (*tableGroupAdmin)(nil)

func (a *tableGroupAdmin) client(database string) (multipoolermanagerpb.MultipoolerManagerClient, error) {
	conn, err := a.pg.GetConnection(&querypb.Target{
		ShardRef: &clustermetadatapb.ShardRef{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
		Mode: querypb.Mode_MODE_WRITABLE,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "no writable pooler for the catalog shard of database %q: %v", database, err)
	}
	if conn == nil {
		return nil, status.Errorf(codes.Unavailable, "no writable pooler for the catalog shard of database %q", database)
	}
	return conn.ManagerClient(), nil
}

func (a *tableGroupAdmin) CreateTableGroup(ctx context.Context, database string, req *multipoolermanagerdatapb.CreateTableGroupRequest) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	c, err := a.client(database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, tableGroupAdminCallTimeout)
	defer cancel()
	return c.CreateTableGroup(ctx, req)
}

func (a *tableGroupAdmin) DropTableGroup(ctx context.Context, database string, req *multipoolermanagerdatapb.DropTableGroupRequest) (*multipoolermanagerdatapb.DropTableGroupResponse, error) {
	c, err := a.client(database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, tableGroupAdminCallTimeout)
	defer cancel()
	return c.DropTableGroup(ctx, req)
}

func (a *tableGroupAdmin) GetOperation(ctx context.Context, database, operationID string) (*multipoolermanagerdatapb.Operation, error) {
	c, err := a.client(database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, tableGroupAdminCallTimeout)
	defer cancel()
	resp, err := c.GetOperation(ctx, &multipoolermanagerdatapb.GetOperationRequest{OperationId: operationID})
	if err != nil {
		return nil, err
	}
	return resp.GetOperation(), nil
}
