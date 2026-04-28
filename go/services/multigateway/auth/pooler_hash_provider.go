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

// Package auth provides authentication utilities for multigateway.
package auth

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// PoolerSystemClient is the interface for system-level operations on multipooler.
// This is used for authentication and other tasks that act as the system rather
// than as a specific user, bypassing per-user connection pools.
// PoolerGateway implements this with full failover buffering support.
type PoolerSystemClient interface {
	GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error)
}

// PoolerHashProvider implements scram.PasswordHashProvider by fetching
// credentials from multipooler via gRPC.
type PoolerHashProvider struct {
	client PoolerSystemClient
}

// NewPoolerHashProvider creates a new PoolerHashProvider.
// The client is typically a PoolerGateway, which handles pooler selection
// and failover buffering internally.
func NewPoolerHashProvider(client PoolerSystemClient) *PoolerHashProvider {
	return &PoolerHashProvider{
		client: client,
	}
}

// GetPasswordHash retrieves the SCRAM-SHA-256 hash for a user from multipooler.
// Returns scram.ErrUserNotFound if the user does not exist or has no password,
// scram.ErrLoginDisabled for NOLOGIN roles, or scram.ErrPasswordExpired for
// roles whose rolvaliduntil has elapsed. Callers (ScramAuthenticator and the
// gateway startup flow) use these sentinels to emit the matching native-PG
// error with the correct SQLSTATE.
//
// Login-disabled and password-expired rejections are carried from the pooler
// as a PgDiagnostic attached to the gRPC status; we match on SQLSTATE rather
// than gRPC code because codes.PermissionDenied / codes.Unauthenticated are
// also used by gRPC auth interceptors for mTLS / authz failures, so keying
// on code alone would misclassify those transport errors as user auth errors.
func (p *PoolerHashProvider) GetPasswordHash(ctx context.Context, username, database string) (*scram.ScramHash, error) {
	resp, err := p.client.GetAuthCredentials(ctx, &multipoolerpb.GetAuthCredentialsRequest{
		Database: database,
		Username: username,
	})
	if err != nil {
		// App-level auth rejections travel as PgDiagnostic (via RPCError
		// details). Transport-level failures — mTLS handshake errors, authz
		// middleware rejections — don't carry a PgDiagnostic and fall through
		// to the generic wrap below. This keeps the two layers distinct even
		// when the underlying gRPC code happens to collide.
		var diag *mterrors.PgDiagnostic
		if errors.As(err, &diag) {
			switch diag.Code {
			case mterrors.PgSSInvalidAuthSpec:
				return nil, scram.ErrLoginDisabled
			case mterrors.PgSSAuthFailed:
				return nil, scram.ErrPasswordExpired
			}
		}
		// User-not-found keeps the gRPC-code mapping: it's returned as a
		// plain codes.NotFound (no PgDiagnostic) and NotFound isn't used by
		// typical auth middleware, so the collision risk is negligible.
		if mterrors.Code(err) == mtrpcpb.Code(codes.NotFound) {
			return nil, scram.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to get auth credentials: %w", err)
	}

	// User exists but has no password set.
	if resp.ScramHash == "" {
		return nil, scram.ErrUserNotFound
	}

	// Parse the SCRAM hash.
	hash, err := scram.ParseScramSHA256Hash(resp.ScramHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SCRAM hash: %w", err)
	}

	return hash, nil
}

// Ensure PoolerHashProvider implements scram.PasswordHashProvider.
var _ scram.PasswordHashProvider = (*PoolerHashProvider)(nil)
