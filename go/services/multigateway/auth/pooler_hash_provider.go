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
func (p *PoolerHashProvider) GetPasswordHash(ctx context.Context, username, database string) (*scram.ScramHash, error) {
	resp, err := p.client.GetAuthCredentials(ctx, &multipoolerpb.GetAuthCredentialsRequest{
		Database: database,
		Username: username,
	})
	if err != nil {
		// PoolerGateway.GetAuthCredentials converts gRPC status errors via
		// mterrors.FromGRPC, so the native gRPC code surfaces through mterrors.Code.
		switch mterrors.Code(err) {
		case mtrpcpb.Code(codes.NotFound):
			return nil, scram.ErrUserNotFound
		case mtrpcpb.Code(codes.PermissionDenied):
			return nil, scram.ErrLoginDisabled
		case mtrpcpb.Code(codes.Unauthenticated):
			return nil, scram.ErrPasswordExpired
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
