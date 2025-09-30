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

package server

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

func TestNewMultiPoolerManagerServer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &Config{
		SocketFilePath: "/tmp/test.sock",
		Database:       "testdb",
	}

	server := NewMultiPoolerManagerServer(logger, config)

	assert.NotNil(t, server)
	assert.Equal(t, config, server.config)
	assert.Equal(t, logger, server.logger)
}

func TestMultiPoolerManagerMethods_NotImplemented(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &Config{}
	server := NewMultiPoolerManagerServer(logger, config)
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
				_, err := server.WaitForLSN(ctx, req)
				return err
			},
			expectedMethod: "WaitForLSN",
		},
		{
			name: "SetReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.SetReadOnlyRequest{}
				_, err := server.SetReadOnly(ctx, req)
				return err
			},
			expectedMethod: "SetReadOnly",
		},
		{
			name: "PromoteStandby",
			method: func() error {
				req := &multipoolermanagerdata.PromoteStandbyRequest{
					Wait:        true,
					WaitTimeout: &durationpb.Duration{Seconds: 60},
				}
				_, err := server.PromoteStandby(ctx, req)
				return err
			},
			expectedMethod: "PromoteStandby",
		},
		{
			name: "GetPrimaryLSN",
			method: func() error {
				req := &multipoolermanagerdata.GetPrimaryLSNRequest{}
				_, err := server.GetPrimaryLSN(ctx, req)
				return err
			},
			expectedMethod: "GetPrimaryLSN",
		},
		{
			name: "IsReadOnly",
			method: func() error {
				req := &multipoolermanagerdata.IsReadOnlyRequest{}
				_, err := server.IsReadOnly(ctx, req)
				return err
			},
			expectedMethod: "IsReadOnly",
		},
		{
			name: "SetStandbyPrimaryConnInfo",
			method: func() error {
				req := &multipoolermanagerdata.SetStandbyPrimaryConnInfoRequest{
					Host: "primary.example.com",
					Port: 5432,
				}
				_, err := server.SetStandbyPrimaryConnInfo(ctx, req)
				return err
			},
			expectedMethod: "SetStandbyPrimaryConnInfo",
		},
		{
			name: "StartStandbyReplication",
			method: func() error {
				req := &multipoolermanagerdata.StartReplicationRequest{}
				_, err := server.StartStandbyReplication(ctx, req)
				return err
			},
			expectedMethod: "StartStandbyReplication",
		},
		{
			name: "StopStandbyReplication",
			method: func() error {
				req := &multipoolermanagerdata.StopStandbyReplicationRequest{}
				_, err := server.StopStandbyReplication(ctx, req)
				return err
			},
			expectedMethod: "StopStandbyReplication",
		},
		{
			name: "StandbyReplicationStatus",
			method: func() error {
				req := &multipoolermanagerdata.StandbyReplicationStatusRequest{}
				_, err := server.StandbyReplicationStatus(ctx, req)
				return err
			},
			expectedMethod: "StandbyReplicationStatus",
		},
		{
			name: "ResetStandbyReplication",
			method: func() error {
				req := &multipoolermanagerdata.ResetStandbyReplicationRequest{}
				_, err := server.ResetStandbyReplication(ctx, req)
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
				_, err := server.ConfigureSynchronousReplication(ctx, req)
				return err
			},
			expectedMethod: "ConfigureSynchronousReplication",
		},
		{
			name: "PrimaryStatus",
			method: func() error {
				req := &multipoolermanagerdata.PrimaryStatusRequest{}
				_, err := server.PrimaryStatus(ctx, req)
				return err
			},
			expectedMethod: "PrimaryStatus",
		},
		{
			name: "PrimaryPosition",
			method: func() error {
				req := &multipoolermanagerdata.PrimaryPositionRequest{}
				_, err := server.PrimaryPosition(ctx, req)
				return err
			},
			expectedMethod: "PrimaryPosition",
		},
		{
			name: "StopReplicationAndGetStatus",
			method: func() error {
				req := &multipoolermanagerdata.StopReplicationAndGetStatusRequest{}
				_, err := server.StopReplicationAndGetStatus(ctx, req)
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
				_, err := server.ChangeType(ctx, req)
				return err
			},
			expectedMethod: "ChangeType",
		},
		{
			name: "GetFollowers",
			method: func() error {
				req := &multipoolermanagerdata.GetFollowersRequest{}
				_, err := server.GetFollowers(ctx, req)
				return err
			},
			expectedMethod: "GetFollowers",
		},
		{
			name: "DemoteLeader",
			method: func() error {
				req := &multipoolermanagerdata.DemoteLeaderRequest{}
				_, err := server.DemoteLeader(ctx, req)
				return err
			},
			expectedMethod: "DemoteLeader",
		},
		{
			name: "UndoDemoteLeader",
			method: func() error {
				req := &multipoolermanagerdata.UndoDemoteLeaderRequest{}
				_, err := server.UndoDemoteLeader(ctx, req)
				return err
			},
			expectedMethod: "UndoDemoteLeader",
		},
		{
			name: "PromoteFollower",
			method: func() error {
				req := &multipoolermanagerdata.PromoteFollowerRequest{}
				_, err := server.PromoteFollower(ctx, req)
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
