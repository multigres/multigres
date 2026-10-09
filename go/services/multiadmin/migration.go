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

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/tools/netutil"
)

// These RPCs are thin forwarders: the migration coordinator lives inside
// multipooler, so multiadmin resolves the shard's current leader multipooler
// and forwards the migration RPC to its Migrator service. multiadmin holds no
// migration state.

// leaderMigrationClient resolves the shard's consensus leader multipooler
// (highest consensus rule) and returns a Migrator client dialed to it.
// Multigres is single-shard in the MVP, so the leader is resolved across all
// poolers; multi-shard targeting by migration id is a follow-up.
func (s *MultiadminServer) leaderMigrationClient(ctx context.Context) (migratorpb.MigratorClient, func(), error) {
	pooler, err := s.findPoolerForBackup(ctx, "", "", "", true, false)
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

// forwardMigration resolves the shard's leader Migrator client and forwards
// one RPC to it, via a method expression (e.g.
// migratorpb.MigratorClient.CreateMigration) naming which call to make. This
// is the one place the resolve/dial/defer-close/forward shape lives; every
// RPC method below is a one-line call into it.
func forwardMigration[Req, Resp any](
	ctx context.Context,
	s *MultiadminServer,
	call func(migratorpb.MigratorClient, context.Context, Req, ...grpc.CallOption) (Resp, error),
	req Req,
) (Resp, error) {
	c, closer, err := s.leaderMigrationClient(ctx)
	if err != nil {
		var zero Resp
		return zero, err
	}
	defer closer()
	return call(c, ctx, req)
}

// CreateMigration forwards to Multigres Migrator.
func (s *MultiadminServer) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.CreateMigration, req)
}

// SetMigrationDirection forwards to the leader multipooler.
func (s *MultiadminServer) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.SetMigrationDirection, req)
}

// GetMigration forwards to Multigres Migrator.
func (s *MultiadminServer) GetMigration(ctx context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.GetMigration, req)
}

// ListMigrations forwards to Multigres Migrator.
func (s *MultiadminServer) ListMigrations(ctx context.Context, req *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.ListMigrations, req)
}

// DropMigration forwards to Multigres Migrator.
func (s *MultiadminServer) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.DropMigration, req)
}

// CreateConnection forwards to Multigres Migrator.
func (s *MultiadminServer) CreateConnection(ctx context.Context, req *migratorpb.CreateConnectionRequest) (*migratorpb.CreateConnectionResponse, error) {
	return forwardMigration(ctx, s, migratorpb.MigratorClient.CreateConnection, req)
}

// GetConnection forwards to Multigres Migrator, then redacts the stored
// connection's password: unlike the other forwarders (which only round-trip a
// connection *name* or a DSN the caller itself just supplied), this RPC reads
// back a previously-stored DSN, and multiadmin's gRPC surface has no
// authentication of its own even when --enable-auth is set (it only covers
// HTTP/Connect — see init.go's SetHTTPOnlyAuth), so any reachable gRPC caller
// could otherwise retrieve another operator's source credentials. The
// Migrator service itself cannot redact this at the source (connToProto):
// the gateway's own SHOW CONNECTION rendering (showConnections) depends on
// reading back the full, unredacted DSN to parse out and display its
// non-sensitive parts (host/port/dbname/user/sslmode), so redacting there
// would break that display for every caller, not just this one.
func (s *MultiadminServer) GetConnection(ctx context.Context, req *migratorpb.GetConnectionRequest) (*migratorpb.GetConnectionResponse, error) {
	resp, err := forwardMigration(ctx, s, migratorpb.MigratorClient.GetConnection, req)
	if err != nil {
		return nil, err
	}
	redactConnectionDSN(resp.GetConnection())
	return resp, nil
}

// ListConnections forwards to Multigres Migrator, then redacts every returned
// connection's password (see GetConnection).
func (s *MultiadminServer) ListConnections(ctx context.Context, req *migratorpb.ListConnectionsRequest) (*migratorpb.ListConnectionsResponse, error) {
	resp, err := forwardMigration(ctx, s, migratorpb.MigratorClient.ListConnections, req)
	if err != nil {
		return nil, err
	}
	for _, c := range resp.GetConnections() {
		redactConnectionDSN(c)
	}
	return resp, nil
}

// redactConnectionDSN blanks the password in c's DSN in place, replacing it
// with a plain host/port/dbname/user summary (useful for identifying which
// source a connection points at; only the secret is stripped). Built by hand
// from the parsed fields rather than via ConnConfig.ConnString(), which
// returns pgx's cached copy of the original input and ignores any field
// mutated after parsing (confirmed empirically — clearing cfg.Password does
// not change its output). On a parse failure the whole Dsn is blanked rather
// than risk leaking it verbatim.
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
	return forwardMigration(ctx, s, migratorpb.MigratorClient.DropConnection, req)
}
