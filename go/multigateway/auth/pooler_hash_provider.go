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
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// PoolerClient is the interface for calling multipooler's GetAuthCredentials.
// This allows for easy mocking in tests.
type PoolerClient interface {
	GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error)
}

// PoolerDiscoverer provides pooler clients for authentication.
// At authentication time, the provider selects an appropriate pooler
// from what discovery knows is currently available.
type PoolerDiscoverer interface {
	// GetPoolerClient returns a PoolerClient for the given database.
	// The implementation chooses which pooler to use based on availability.
	GetPoolerClient(ctx context.Context, database string) (PoolerClient, error)
}

// PoolerHashProvider implements scram.PasswordHashProvider by fetching
// credentials from multipooler via gRPC.
type PoolerHashProvider struct {
	discoverer PoolerDiscoverer
}

// NewPoolerHashProvider creates a new PoolerHashProvider.
// The discoverer is used at authentication time to select an available
// pooler for the target database.
func NewPoolerHashProvider(discoverer PoolerDiscoverer) *PoolerHashProvider {
	return &PoolerHashProvider{
		discoverer: discoverer,
	}
}

// GetPasswordHash retrieves the SCRAM-SHA-256 hash for a user from multipooler.
// Returns scram.ErrUserNotFound if the user does not exist or has no password.
func (p *PoolerHashProvider) GetPasswordHash(ctx context.Context, username, database string) (*scram.ScramHash, error) {
	client, err := p.discoverer.GetPoolerClient(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooler client for database %q: %w", database, err)
	}

	resp, err := client.GetAuthCredentials(ctx, &multipoolerpb.GetAuthCredentialsRequest{
		Database: database,
		Username: username,
	})
	if err != nil {
		// User not found is returned as NotFound error.
		if status.Code(err) == codes.NotFound {
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
