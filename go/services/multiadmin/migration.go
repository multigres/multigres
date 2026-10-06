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
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/tools/netutil"
)

// These RPCs are thin forwarders: the migration coordinator lives inside
// multipooler, so multiadmin resolves the shard's current leader multipooler
// and forwards the migration RPC to its Migrator service. multiadmin holds no
// migration state.

// leaderMigrationClient dials the Migrator service on target's leader pooler.
func (s *MultiadminServer) leaderMigrationClient(ctx context.Context, target *clustermetadatapb.ShardKey) (migratorpb.MigratorClient, func(), error) {
	pooler, err := s.findPoolerForMigration(ctx, target)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "no leader pooler for migration: %v", err)
	}
	addr := netutil.JoinHostPort(pooler.Hostname, pooler.PortMap["grpc"])
	conn, err := s.migrationDialer(ctx, addr)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial pooler %s: %v", addr, err)
	}
	return migratorpb.NewMigratorClient(conn), func() { _ = conn.Close() }, nil
}

// forwardMigration forwards one RPC to target's leader Migrator. A nil target
// matches any pooler in the cluster.
func forwardMigration[Req, Resp any](
	ctx context.Context,
	s *MultiadminServer,
	call func(migratorpb.MigratorClient, context.Context, Req, ...grpc.CallOption) (Resp, error),
	req Req,
	target *clustermetadatapb.ShardKey,
) (Resp, error) {
	c, closer, err := s.leaderMigrationClient(ctx, target)
	if err != nil {
		var zero Resp
		return zero, err
	}
	defer closer()
	return call(c, ctx, req)
}

// CreateMigration forwards to Multigres Migrator, routed by the migration's
// target shard key.
func (s *MultiadminServer) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.CreateMigration, req, req.GetMigration().GetTarget())
}

// SetMigrationDirection forwards to the leader multipooler.
func (s *MultiadminServer) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.SetMigrationDirection, req, nil)
}

// GetMigration forwards to Multigres Migrator.
func (s *MultiadminServer) GetMigration(ctx context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.GetMigration, req, nil)
}

// GetMigrationJournal forwards to Multigres Migrator.
func (s *MultiadminServer) GetMigrationJournal(ctx context.Context, req *migratorpb.GetMigrationJournalRequest) (*migratorpb.GetMigrationJournalResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.GetMigrationJournal, req, nil)
}

// ListMigrations forwards to Multigres Migrator.
func (s *MultiadminServer) ListMigrations(ctx context.Context, req *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.ListMigrations, req, nil)
}

// DropMigration forwards to Multigres Migrator.
func (s *MultiadminServer) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.DropMigration, req, nil)
}

// CreateConnection forwards to Multigres Migrator.
func (s *MultiadminServer) CreateConnection(ctx context.Context, req *migratorpb.CreateConnectionRequest) (*migratorpb.CreateConnectionResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.CreateConnection, req, nil)
}

// GetConnection forwards to Multigres Migrator and redacts the password:
// multiadmin's gRPC surface is unauthenticated. The Migrator can't redact
// itself because SHOW CONNECTION needs the full DSN.
func (s *MultiadminServer) GetConnection(ctx context.Context, req *migratorpb.GetConnectionRequest) (*migratorpb.GetConnectionResponse, error) {
	resp, err := forwardMigration(ctx, s, migratorpb.MigratorClient.GetConnection, req, nil)
	if err != nil {
		return nil, err
	}
	redactConnectionDSN(resp.GetConnection())
	return resp, nil
}

// ListConnections forwards to Multigres Migrator, then redacts every returned
// connection's password.
func (s *MultiadminServer) ListConnections(ctx context.Context, req *migratorpb.ListConnectionsRequest) (*migratorpb.ListConnectionsResponse, error) {
	resp, err := forwardMigration(ctx, s, migratorpb.MigratorClient.ListConnections, req, nil)
	if err != nil {
		return nil, err
	}
	for _, c := range resp.GetConnections() {
		redactConnectionDSN(c)
	}
	return resp, nil
}

// redactConnectionDSN blanks the password in c's DSN in place.
func redactConnectionDSN(c *migratorpb.Connection) {
	if c == nil || c.Dsn == "" {
		return
	}
	cfg, err := pgx.ParseConfig(c.Dsn)
	if err != nil {
		c.Dsn = ""
		return
	}
	c.Dsn = fmt.Sprintf("host=%s port=%d dbname=%s user=%s", cfg.Host, cfg.Port, cfg.Database, cfg.User)
}

// DropConnection forwards to Multigres Migrator.
func (s *MultiadminServer) DropConnection(ctx context.Context, req *migratorpb.DropConnectionRequest) (*migratorpb.DropConnectionResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.DropConnection, req, nil)
}
