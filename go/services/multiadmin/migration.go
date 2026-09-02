// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiadmin

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/tools/netutil"
)

// These RPCs are thin forwarders: the migration coordinator lives inside
// multipooler, so multiadmin resolves the shard's current primary multipooler
// and forwards the migration RPC to its Migrator service. multiadmin holds no
// migration state.

// primaryMigrationClient resolves the shard's primary multipooler (highest
// consensus rule) and returns a Migrator client dialed to it. Multigres is
// single-shard in the MVP, so the primary is resolved across all poolers;
// multi-shard targeting by migration id is a follow-up.
func (s *MultiadminServer) primaryMigrationClient(ctx context.Context) (migratorpb.MigratorClient, func(), error) {
	pooler, err := s.findPoolerForBackup(ctx, "", "", "", true)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "no primary pooler for migration: %v", err)
	}
	addr := netutil.JoinHostPort(pooler.Hostname, pooler.PortMap["grpc"])
	conn, err := s.migrationDialer(ctx, addr)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial pooler %s: %v", addr, err)
	}
	return migratorpb.NewMigratorClient(conn), func() { _ = conn.Close() }, nil
}

// CreateMigration forwards to Multigres Migrator.
func (s *MultiadminServer) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.CreateMigration(ctx, req)
}

// StartMigration forwards to Multigres Migrator.
func (s *MultiadminServer) StartMigration(ctx context.Context, req *migratorpb.StartMigrationRequest) (*migratorpb.StartMigrationResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.StartMigration(ctx, req)
}

// UpdateMigration forwards to the primary multipooler.
func (s *MultiadminServer) UpdateMigration(ctx context.Context, req *migratorpb.UpdateMigrationRequest) (*migratorpb.UpdateMigrationResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.UpdateMigration(ctx, req)
}

// SetMigrationDirection forwards to the primary multipooler.
func (s *MultiadminServer) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.SetMigrationDirection(ctx, req)
}

// GetMigrations forwards to Multigres Migrator.
func (s *MultiadminServer) GetMigrations(ctx context.Context, req *migratorpb.GetMigrationsRequest) (*migratorpb.GetMigrationsResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.GetMigrations(ctx, req)
}

// DropMigration forwards to Multigres Migrator.
func (s *MultiadminServer) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	c, closer, err := s.primaryMigrationClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()
	return c.DropMigration(ctx, req)
}
