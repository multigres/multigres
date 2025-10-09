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
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/servenv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

func TestManagerServiceMethods_NotImplemented(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create the multipooler in topology so manager can reach ready state
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_PRIMARY,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv()
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &managerService{
		manager: pm,
	}

	tests := []struct {
		name           string
		method         func() error
		expectedMethod string
	}{
		{
			name: "WaitForLSN",
			method: func() error {
				req := &multipoolermanagerdata.WaitForLSNRequest{
					TargetLsn: "0/1000000",
					Timeout:   &durationpb.Duration{Seconds: 30},
				}
				_, err := svc.WaitForLSN(ctx, req)
				return err
			},
			expectedMethod: "WaitForLSN",
		},
		{
			name: "SetReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.SetReadOnlyRequest{}
				_, err := svc.SetReadOnly(ctx, req)
				return err
			},
			expectedMethod: "SetReadOnly",
		},
		{
			name: "IsReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.IsReadOnlyRequest{}
				_, err := svc.IsReadOnly(ctx, req)
				return err
			},
			expectedMethod: "IsReadOnly",
		},
		{
			name: "SetPrimaryConnInfo",
			method: func() error {
				req := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
					Host: "primary.example.com",
					Port: 5432,
				}
				_, err := svc.SetPrimaryConnInfo(ctx, req)
				return err
			},
			expectedMethod: "SetPrimaryConnInfo",
		},
		{
			name: "StartReplication",
			method: func() error {
				req := &multipoolermanagerdata.StartReplicationRequest{}
				_, err := svc.StartReplication(ctx, req)
				return err
			},
			expectedMethod: "StartReplication",
		},
		{
			name: "StopReplication",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationRequest{}
				_, err := svc.StopReplication(ctx, req)
				return err
			},
			expectedMethod: "StopReplication",
		},
		{
			name: "ReplicationStatus",
			method: func() error {
				req := &multipoolermanagerdata.ReplicationStatusRequest{}
				_, err := svc.ReplicationStatus(ctx, req)
				return err
			},
			expectedMethod: "ReplicationStatus",
		},
		{
			name: "ResetReplication",
			method: func() error {
				req := &multipoolermanagerdata.ResetReplicationRequest{}
				_, err := svc.ResetReplication(ctx, req)
				return err
			},
			expectedMethod: "ResetReplication",
		},
		{
			name: "ConfigureSynchronousReplication",
			method: func() error {
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
					SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					NumSync:           1,
					StandbyNames:      []string{"standby1"},
				}
				_, err := svc.ConfigureSynchronousReplication(ctx, req)
				return err
			},
			expectedMethod: "ConfigureSynchronousReplication",
		},
		{
			name: "PrimaryStatus",
			method: func() error {
				req := &multipoolermanagerdata.PrimaryStatusRequest{}
				_, err := svc.PrimaryStatus(ctx, req)
				return err
			},
			expectedMethod: "PrimaryStatus",
		},
		{
			name: "StopReplicationAndGetStatus",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationAndGetStatusRequest{}
				_, err := svc.StopReplicationAndGetStatus(ctx, req)
				return err
			},
			expectedMethod: "StopReplicationAndGetStatus",
		},
		{
			name: "GetFollowers",
			method: func() error {
				req := &multipoolermanagerdata.GetFollowersRequest{}
				_, err := svc.GetFollowers(ctx, req)
				return err
			},
			expectedMethod: "GetFollowers",
		},
		{
			name: "Demote",
			method: func() error {
				req := &multipoolermanagerdata.DemoteRequest{}
				_, err := svc.Demote(ctx, req)
				return err
			},
			expectedMethod: "Demote",
		},
		{
			name: "UndoDemote",
			method: func() error {
				req := &multipoolermanagerdata.UndoDemoteRequest{}
				_, err := svc.UndoDemote(ctx, req)
				return err
			},
			expectedMethod: "UndoDemote",
		},
		{
			name: "Promote",
			method: func() error {
				req := &multipoolermanagerdata.PromoteRequest{}
				_, err := svc.Promote(ctx, req)
				return err
			},
			expectedMethod: "Promote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.method()

			// Assert that all methods return an error
			assert.Error(t, err, "Method %s should return an error", tt.name)

			// Assert that the error is a gRPC Unimplemented error
			code := mterrors.Code(err)
			assert.Equal(t, mtrpcpb.Code_UNIMPLEMENTED, code, "Should return Unimplemented code")
			if !strings.Contains(err.Error(), fmt.Sprintf("method %s not implemented", tt.expectedMethod)) {
				t.Errorf("Error message should include: method %s not implemented, got: %s", tt.expectedMethod, err.Error())
			}
		})
	}
}

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

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Do NOT start the manager - keep it in starting state
	// Verify manager is in starting state
	assert.Equal(t, manager.ManagerStateStarting, pm.GetState())

	svc := &managerService{
		manager: pm,
	}

	tests := []struct {
		name   string
		method func() error
	}{
		{
			name: "WaitForLSN",
			method: func() error {
				req := &multipoolermanagerdata.WaitForLSNRequest{
					TargetLsn: "0/1000000",
					Timeout:   &durationpb.Duration{Seconds: 30},
				}
				_, err := svc.WaitForLSN(ctx, req)
				return err
			},
		},
		{
			name: "SetReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.SetReadOnlyRequest{}
				_, err := svc.SetReadOnly(ctx, req)
				return err
			},
		},
		{
			name: "IsReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.IsReadOnlyRequest{}
				_, err := svc.IsReadOnly(ctx, req)
				return err
			},
		},
		{
			name: "SetPrimaryConnInfo",
			method: func() error {
				req := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
					Host: "primary.example.com",
					Port: 5432,
				}
				_, err := svc.SetPrimaryConnInfo(ctx, req)
				return err
			},
		},
		{
			name: "StartReplication",
			method: func() error {
				req := &multipoolermanagerdata.StartReplicationRequest{}
				_, err := svc.StartReplication(ctx, req)
				return err
			},
		},
		{
			name: "StopReplication",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationRequest{}
				_, err := svc.StopReplication(ctx, req)
				return err
			},
		},
		{
			name: "ReplicationStatus",
			method: func() error {
				req := &multipoolermanagerdata.ReplicationStatusRequest{}
				_, err := svc.ReplicationStatus(ctx, req)
				return err
			},
		},
		{
			name: "ResetReplication",
			method: func() error {
				req := &multipoolermanagerdata.ResetReplicationRequest{}
				_, err := svc.ResetReplication(ctx, req)
				return err
			},
		},
		{
			name: "ConfigureSynchronousReplication",
			method: func() error {
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
					SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					NumSync:           1,
					StandbyNames:      []string{"standby1"},
				}
				_, err := svc.ConfigureSynchronousReplication(ctx, req)
				return err
			},
		},
		{
			name: "PrimaryStatus",
			method: func() error {
				req := &multipoolermanagerdata.PrimaryStatusRequest{}
				_, err := svc.PrimaryStatus(ctx, req)
				return err
			},
		},
		{
			name: "StopReplicationAndGetStatus",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationAndGetStatusRequest{}
				_, err := svc.StopReplicationAndGetStatus(ctx, req)
				return err
			},
		},
		{
			name: "ChangeType",
			method: func() error {
				req := &multipoolermanagerdata.ChangeTypeRequest{
					PoolerType: clustermetadata.PoolerType_PRIMARY,
				}
				_, err := svc.ChangeType(ctx, req)
				return err
			},
		},
		{
			name: "GetFollowers",
			method: func() error {
				req := &multipoolermanagerdata.GetFollowersRequest{}
				_, err := svc.GetFollowers(ctx, req)
				return err
			},
		},
		{
			name: "Demote",
			method: func() error {
				req := &multipoolermanagerdata.DemoteRequest{}
				_, err := svc.Demote(ctx, req)
				return err
			},
		},
		{
			name: "UndoDemote",
			method: func() error {
				req := &multipoolermanagerdata.UndoDemoteRequest{}
				_, err := svc.UndoDemote(ctx, req)
				return err
			},
		},
		{
			name: "Promote",
			method: func() error {
				req := &multipoolermanagerdata.PromoteRequest{}
				_, err := svc.Promote(ctx, req)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check manager state before calling method
			t.Logf("Manager state before %s: %s", tt.name, pm.GetState())

			err := tt.method()

			// Print the full error for debugging
			t.Logf("Error from %s: %v", tt.name, err)

			// Assert that all methods return an error
			assert.Error(t, err, "Method %s should return an error when manager is not ready", tt.name)

			// Check the mterrors code directly
			code := mterrors.Code(err)
			// Should return UNAVAILABLE when manager is starting
			assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, code, "Should return UNAVAILABLE code when manager is not ready")
			assert.Contains(t, err.Error(), "manager is still starting up", "Error message should indicate manager is starting")
		})
	}
}
