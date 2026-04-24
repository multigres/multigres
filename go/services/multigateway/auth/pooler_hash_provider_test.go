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

package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// mockPoolerSystemClient is a mock implementation of AuthCredentialsClient for testing.
type mockPoolerSystemClient struct {
	response *multipoolerpb.GetAuthCredentialsResponse
	err      error
}

func (m *mockPoolerSystemClient) GetAuthCredentials(_ context.Context, _ *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	return m.response, m.err
}

func TestPoolerHashProvider_GetPasswordHash(t *testing.T) {
	// Valid SCRAM hash from PostgreSQL.
	// Format: SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
	// Salt needs to be at least 8 bytes, keys are base64-encoded.
	validScramHash := "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0$c3RvcmVka2V5MTIzNDU2Nzg5MDEyMw==:c2VydmVya2V5MTIzNDU2Nzg5MDEyMw=="

	t.Run("successful lookup", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			response: &multipoolerpb.GetAuthCredentialsResponse{
				ScramHash: validScramHash,
			},
		}
		provider := NewPoolerHashProvider(client)

		hash, err := provider.GetPasswordHash(context.Background(), "testuser", "testdb")
		require.NoError(t, err)
		require.NotNil(t, hash)
		assert.Equal(t, 4096, hash.Iterations)
	})

	t.Run("user not found", func(t *testing.T) {
		// Use FromGRPC to match production: PoolerGateway.GetAuthCredentials
		// converts gRPC errors via mterrors.FromGRPC before returning.
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.NotFound, "user not found")),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "nonexistent", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrUserNotFound)
	})

	t.Run("login disabled", func(t *testing.T) {
		// Pooler flags rolcanlogin=false via PermissionDenied — gateway must
		// surface scram.ErrLoginDisabled so startup.go emits the 28000 FATAL.
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.PermissionDenied, "user is not permitted to log in")),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "nologin_user", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrLoginDisabled)
	})

	t.Run("password expired", func(t *testing.T) {
		// Pooler flags rolvaliduntil elapsed via Unauthenticated — gateway must
		// map to scram.ErrPasswordExpired so the client sees the opaque
		// "password authentication failed" error (28P01).
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.Unauthenticated, "password expired")),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "expired_user", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrPasswordExpired)
	})

	t.Run("user exists but no password", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			response: &multipoolerpb.GetAuthCredentialsResponse{
				ScramHash: "", // No password set
			},
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "nopassword", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrUserNotFound)
	})

	t.Run("grpc error", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			err: errors.New("connection refused"),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "testuser", "testdb")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get auth credentials")
	})

	t.Run("invalid hash format", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			response: &multipoolerpb.GetAuthCredentialsResponse{
				ScramHash: "invalid-hash-format",
			},
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "testuser", "testdb")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse SCRAM hash")
	})
}
