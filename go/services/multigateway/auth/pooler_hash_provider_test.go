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

	t.Run("login disabled (via PgDiagnostic 28000)", func(t *testing.T) {
		// The pooler surfaces rolcanlogin=false as a PgDiagnostic with
		// SQLSTATE 28000 attached to the gRPC status. The gateway must key
		// on the SQLSTATE — not the gRPC code — so transport-level
		// PermissionDenied errors don't get misclassified as app-level
		// "role not permitted to log in" rejections.
		diag := mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec,
			"role \"nologin_user\" is not permitted to log in", "")
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(mterrors.ToGRPC(diag)),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "nologin_user", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrLoginDisabled)
	})

	t.Run("password expired (via PgDiagnostic 28P01)", func(t *testing.T) {
		// SQLSTATE 28P01 signals expired password. Client sees the same
		// opaque "password authentication failed" message PG emits for a
		// wrong password.
		diag := mterrors.NewPgError("FATAL", mterrors.PgSSAuthFailed,
			"password authentication failed for user \"expired_user\"", "")
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(mterrors.ToGRPC(diag)),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "expired_user", "testdb")
		require.Error(t, err)
		assert.ErrorIs(t, err, scram.ErrPasswordExpired)
	})

	t.Run("transport-level PermissionDenied does NOT misclassify as login-disabled", func(t *testing.T) {
		// Regression guard for the code-vs-PgDiagnostic distinction: a bare
		// PermissionDenied (such as a gRPC authz middleware rejection) carries
		// no PgDiagnostic and must not be translated to scram.ErrLoginDisabled.
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.PermissionDenied, "authz: caller not in role")),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "alice", "testdb")
		require.Error(t, err)
		assert.NotErrorIs(t, err, scram.ErrLoginDisabled)
		assert.NotErrorIs(t, err, scram.ErrPasswordExpired)
		assert.NotErrorIs(t, err, scram.ErrUserNotFound)
	})

	t.Run("transport-level Unauthenticated does NOT misclassify as password-expired", func(t *testing.T) {
		// Regression guard: an mTLS / interceptor Unauthenticated with no
		// PgDiagnostic must remain generic so it doesn't surface to end users
		// as "password authentication failed".
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.Unauthenticated, "mTLS: bad client cert")),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.GetPasswordHash(context.Background(), "alice", "testdb")
		require.Error(t, err)
		assert.NotErrorIs(t, err, scram.ErrLoginDisabled)
		assert.NotErrorIs(t, err, scram.ErrPasswordExpired)
		assert.NotErrorIs(t, err, scram.ErrUserNotFound)
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

func TestPoolerHashProvider_IsReplicationRole(t *testing.T) {
	t.Run("rolreplication=true", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			response: &multipoolerpb.GetAuthCredentialsResponse{
				IsReplicationRole: true,
			},
		}
		provider := NewPoolerHashProvider(client)

		ok, err := provider.IsReplicationRole(context.Background(), "repluser", "postgres")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("rolreplication=false", func(t *testing.T) {
		client := &mockPoolerSystemClient{
			response: &multipoolerpb.GetAuthCredentialsResponse{
				IsReplicationRole: false,
			},
		}
		provider := NewPoolerHashProvider(client)

		ok, err := provider.IsReplicationRole(context.Background(), "regular", "postgres")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("user not found returns false without error", func(t *testing.T) {
		// Defensive: by the time IsReplicationRole runs, SCRAM has succeeded
		// for this user, so NotFound here is unexpected. We still treat it
		// as "not a replication role" rather than propagate, so the gateway
		// emits the standard 42501 rejection rather than a generic error.
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(status.Error(codes.NotFound, "user not found")),
		}
		provider := NewPoolerHashProvider(client)

		ok, err := provider.IsReplicationRole(context.Background(), "ghost", "postgres")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("login-disabled diagnostic treated as not-replication", func(t *testing.T) {
		// Same defensive reasoning: a role rolcanlogin=false couldn't have
		// passed SCRAM, but if we ever see this here we don't want to leak
		// it as a transport error.
		diag := mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec,
			"role \"x\" is not permitted to log in", "")
		client := &mockPoolerSystemClient{
			err: mterrors.FromGRPC(mterrors.ToGRPC(diag)),
		}
		provider := NewPoolerHashProvider(client)

		ok, err := provider.IsReplicationRole(context.Background(), "x", "postgres")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("transport error propagated", func(t *testing.T) {
		// Lookup failure: surface the error so the gateway can fail closed
		// (the verifier converts a non-nil error into a 42501 FATAL).
		client := &mockPoolerSystemClient{
			err: errors.New("connection refused"),
		}
		provider := NewPoolerHashProvider(client)

		_, err := provider.IsReplicationRole(context.Background(), "u", "postgres")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get auth credentials")
	})
}
