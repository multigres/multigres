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

	"github.com/multigres/multigres/go/multipooler/manager"

	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

func TestManagerServiceMethods_NotImplemented(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		TopoClient: nil,
		ServiceID:  "test-service",
	}
	pm := manager.NewMultiPoolerManager(logger, config)

	svc := &managerService{
		manager: pm,
	}

	ctx := context.Background()

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
			name: "PgPromote",
			method: func() error {
				req := &multipoolermanagerdata.PgPromoteRequest{
					Wait:        true,
					WaitTimeout: &durationpb.Duration{Seconds: 60},
				}
				_, err := svc.PgPromote(ctx, req)
				return err
			},
			expectedMethod: "PromoteStandby",
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
			expectedMethod: "SetStandbyPrimaryConnInfo",
		},
		{
			name: "StartReplication",
			method: func() error {
				req := &multipoolermanagerdata.StartReplicationRequest{}
				_, err := svc.StartReplication(ctx, req)
				return err
			},
			expectedMethod: "StartStandbyReplication",
		},
		{
			name: "StopReplication",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationRequest{}
				_, err := svc.StopReplication(ctx, req)
				return err
			},
			expectedMethod: "StopStandbyReplication",
		},
		{
			name: "ReplicationStatus",
			method: func() error {
				req := &multipoolermanagerdata.ReplicationStatusRequest{}
				_, err := svc.ReplicationStatus(ctx, req)
				return err
			},
			expectedMethod: "StandbyReplicationStatus",
		},
		{
			name: "ResetReplication",
			method: func() error {
				req := &multipoolermanagerdata.ResetReplicationRequest{}
				_, err := svc.ResetReplication(ctx, req)
				return err
			},
			expectedMethod: "ResetStandbyReplication",
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
			name: "ChangeType",
			method: func() error {
				req := &multipoolermanagerdata.ChangeTypeRequest{
					PoolerType: clustermetadata.PoolerType_PRIMARY,
				}
				_, err := svc.ChangeType(ctx, req)
				return err
			},
			expectedMethod: "ChangeType",
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
			expectedMethod: "DemoteLeader",
		},
		{
			name: "UndoDemote",
			method: func() error {
				req := &multipoolermanagerdata.UndoDemoteRequest{}
				_, err := svc.UndoDemote(ctx, req)
				return err
			},
			expectedMethod: "UndoDemoteLeader",
		},
		{
			name: "Promote",
			method: func() error {
				req := &multipoolermanagerdata.PromoteRequest{}
				_, err := svc.Promote(ctx, req)
				return err
			},
			expectedMethod: "PromoteFollower",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.method()

			// Assert that all methods return an error
			assert.Error(t, err, "Method %s should return an error", tt.name)

			// Assert that the error is a gRPC Unimplemented error
			st, ok := status.FromError(err)
			assert.True(t, ok, "Error should be a gRPC status error")
			assert.Equal(t, codes.Unimplemented, st.Code(), "Should return Unimplemented code")
			assert.Contains(t, st.Message(), "method "+tt.expectedMethod+" not implemented",
				"Error message should indicate method is not implemented")
		})
	}
}
