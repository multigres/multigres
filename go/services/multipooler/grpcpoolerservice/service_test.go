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

package grpcpoolerservice

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
)

func TestGetAuthCredentials_Validation(t *testing.T) {
	srv := &poolerService{pooler: nil}

	t.Run("missing username", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "testdb",
			Username: "",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "username is required")
	})

	t.Run("missing database", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "",
			Username: "testuser",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "database is required")
	})

	t.Run("nil pooler", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "testdb",
			Username: "testuser",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Contains(t, st.Message(), "pooler not initialized")
	})
}

func TestStreamPoolerHealth_Validation(t *testing.T) {
	t.Run("nil pooler", func(t *testing.T) {
		srv := &poolerService{pooler: nil}
		req := &multipoolerpb.StreamPoolerHealthRequest{}

		// Create a mock stream that captures the error
		mockStream := &mockHealthStream{ctx: context.Background()}
		err := srv.StreamPoolerHealth(req, mockStream)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Contains(t, st.Message(), "pooler not initialized")
	})
}

// mockHealthStream is a minimal mock for testing StreamPoolerHealth validation.
type mockHealthStream struct {
	multipoolerpb.MultiPoolerService_StreamPoolerHealthServer
	ctx context.Context
}

func (m *mockHealthStream) Context() context.Context {
	return m.ctx
}

func (m *mockHealthStream) Send(*multipoolerpb.StreamPoolerHealthResponse) error {
	return nil
}

// fakeHealthProvider returns a fixed HealthState on subscription so tests can
// assert how health state flows through healthStateToProto.
type fakeHealthProvider struct {
	state *poolerserver.HealthState
}

func (f *fakeHealthProvider) GetHealthState(ctx context.Context) (*poolerserver.HealthState, error) {
	return f.state, nil
}

func (f *fakeHealthProvider) SubscribeHealth(ctx context.Context) (*poolerserver.HealthState, <-chan *poolerserver.HealthState, error) {
	ch := make(chan *poolerserver.HealthState)
	// Close immediately so StreamPoolerHealth returns after sending the initial state.
	close(ch)
	return f.state, ch, nil
}

// capturingHealthStream records every response sent by StreamPoolerHealth.
type capturingHealthStream struct {
	multipoolerpb.MultiPoolerService_StreamPoolerHealthServer
	ctx       context.Context
	responses []*multipoolerpb.StreamPoolerHealthResponse
}

func (c *capturingHealthStream) Context() context.Context {
	return c.ctx
}

func (c *capturingHealthStream) Send(resp *multipoolerpb.StreamPoolerHealthResponse) error {
	c.responses = append(c.responses, resp)
	return nil
}

func TestStreamPoolerHealth_PublishesServingSignal(t *testing.T) {
	fake := &fakeHealthProvider{
		state: &poolerserver.HealthState{
			ServingSignal: clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING,
		},
	}

	pooler := poolerserver.NewQueryPoolerServer(
		slog.Default(),
		nil, // poolManager
		nil, // poolerID
		"tg",
		"shard",
		fake,
		0,
	)
	srv := &poolerService{pooler: pooler}

	stream := &capturingHealthStream{ctx: context.Background()}
	err := srv.StreamPoolerHealth(&multipoolerpb.StreamPoolerHealthRequest{}, stream)
	require.NoError(t, err)
	require.NotEmpty(t, stream.responses)

	resp := stream.responses[0]
	require.NotNil(t, resp.QueryServingStatus, "QueryServingStatus should be populated")
	assert.Equal(t,
		clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING,
		resp.QueryServingStatus.Signal,
	)
}
