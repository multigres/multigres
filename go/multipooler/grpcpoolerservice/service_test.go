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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
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

func TestEscapeString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with'quote", "with''quote"},
		{"multiple'quotes'here", "multiple''quotes''here"},
		{"", ""},
		{"no special chars", "no special chars"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
