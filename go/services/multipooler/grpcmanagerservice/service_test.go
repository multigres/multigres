// Copyright 2025 Supabase, Inc.
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

package grpcmanagerservice

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/manager"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestManagerServiceMethods_ManagerNotReady(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create manager but DON'T create multipooler in topo and DON'T start it
	// This keeps the manager in "starting" state
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	multipooler := &clustermetadata.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	config := &manager.Config{
		TopoClient:     ts,
		ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	defer pm.Shutdown()

	// Do NOT start the manager - keep it in starting state
	assert.Equal(t, manager.ManagerStateStarting, pm.GetState())

	svc := &managerService{
		manager: pm,
	}

	tests := []struct {
		name   string
		method func() error
	}{
		{
			name: "StartReplication",
			method: func() error {
				_, err := svc.StartReplication(ctx, &multipoolermanagerdata.StartReplicationRequest{})
				return err
			},
		},
		{
			name: "StopReplication",
			method: func() error {
				_, err := svc.StopReplication(ctx, &multipoolermanagerdata.StopReplicationRequest{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Manager state before %s: %s", tt.name, pm.GetState())

			err := tt.method()

			t.Logf("Error from %s: %v", tt.name, err)

			assert.Error(t, err, "Method %s should return an error when manager is not ready", tt.name)

			mterr := mterrors.FromGRPC(err)
			code := mterrors.Code(mterr)
			assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, code, "Should return UNAVAILABLE code when manager is not ready")
			assert.Contains(t, err.Error(), "manager is still starting up", "Error message should indicate manager is starting")
		})
	}
}
