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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// fakeMigratorServer is a bufconn-served Migrator that records the last request
// per RPC and returns a canned response, so the multiadmin forwarders can be
// driven end to end without a live multipooler.
type fakeMigratorServer struct {
	migratorpb.UnimplementedMigratorServer
	create     *migratorpb.CreateMigrationRequest
	direction  *migratorpb.SetMigrationDirectionRequest
	get        *migratorpb.GetMigrationRequest
	list       *migratorpb.ListMigrationsRequest
	drop       *migratorpb.DropMigrationRequest
	createConn *migratorpb.CreateConnectionRequest
	getConn    *migratorpb.GetConnectionRequest
	listConn   *migratorpb.ListConnectionsRequest
	dropConn   *migratorpb.DropConnectionRequest
}

func (f *fakeMigratorServer) CreateMigration(_ context.Context, in *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	f.create = in
	return &migratorpb.CreateMigrationResponse{Migration: &migratorpb.MigrationRecord{Name: in.GetMigration().GetName()}}, nil
}

func (f *fakeMigratorServer) SetMigrationDirection(_ context.Context, in *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	f.direction = in
	return &migratorpb.SetMigrationDirectionResponse{Migration: &migratorpb.MigrationRecord{Id: in.GetRef().GetId()}}, nil
}

func (f *fakeMigratorServer) GetMigration(_ context.Context, in *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
	f.get = in
	return &migratorpb.GetMigrationResponse{}, nil
}

func (f *fakeMigratorServer) ListMigrations(_ context.Context, in *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
	f.list = in
	return &migratorpb.ListMigrationsResponse{}, nil
}

func (f *fakeMigratorServer) DropMigration(_ context.Context, in *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	f.drop = in
	return &migratorpb.DropMigrationResponse{Migration: &migratorpb.MigrationRecord{Id: in.GetRef().GetId()}}, nil
}

func (f *fakeMigratorServer) CreateConnection(_ context.Context, in *migratorpb.CreateConnectionRequest) (*migratorpb.CreateConnectionResponse, error) {
	f.createConn = in
	return &migratorpb.CreateConnectionResponse{Connection: &migratorpb.Connection{Name: in.GetConnection().GetName()}}, nil
}

// fakeConnectionDSN is a stand-in for a stored connection's full, unredacted
// DSN, carrying a password the forwarder must never return to the caller.
const fakeConnectionDSN = "host=src.example.com port=5432 user=postgres password=supersecret dbname=postgres"

func (f *fakeMigratorServer) GetConnection(_ context.Context, in *migratorpb.GetConnectionRequest) (*migratorpb.GetConnectionResponse, error) {
	f.getConn = in
	return &migratorpb.GetConnectionResponse{Connection: &migratorpb.Connection{Id: in.GetRef().GetId(), Dsn: fakeConnectionDSN}}, nil
}

func (f *fakeMigratorServer) ListConnections(_ context.Context, in *migratorpb.ListConnectionsRequest) (*migratorpb.ListConnectionsResponse, error) {
	f.listConn = in
	return &migratorpb.ListConnectionsResponse{Connections: []*migratorpb.Connection{{Id: 1, Dsn: fakeConnectionDSN}}}, nil
}

func (f *fakeMigratorServer) DropConnection(_ context.Context, in *migratorpb.DropConnectionRequest) (*migratorpb.DropConnectionResponse, error) {
	f.dropConn = in
	return &migratorpb.DropConnectionResponse{}, nil
}

// startFakeMigrator serves fakeMigratorServer over an in-process bufconn and
// returns a dialer that reaches it (ignoring the requested target address).
func startFakeMigrator(t *testing.T) (func(context.Context, string) (*grpc.ClientConn, error), *fakeMigratorServer) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	fake := &fakeMigratorServer{}
	migratorpb.RegisterMigratorServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	return dialer, fake
}

// registerPrimaryPooler adds a PRIMARY-routed pooler so findPoolerForBackup
// (forceLeader=true) resolves a target for the forwarders.
func registerPrimaryPooler(t *testing.T, s *MultiadminServer) {
	t.Helper()
	p := makeRoutedPooler("cell1", "leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	require.NoError(t, s.ts.CreateMultipooler(t.Context(), p))
}

func TestMultiadminMigrationForwarders(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	registerPrimaryPooler(t, server)
	dialer, fake := startFakeMigrator(t)
	server.migrationDialer = dialer

	t.Run("CreateMigration", func(t *testing.T) {
		resp, err := server.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{Migration: &migratorpb.MigrationRecord{Name: "m1"}})
		require.NoError(t, err)
		assert.Equal(t, "m1", resp.GetMigration().GetName())
		require.NotNil(t, fake.create)
		assert.Equal(t, "m1", fake.create.GetMigration().GetName())
	})
	t.Run("SetMigrationDirection", func(t *testing.T) {
		_, err := server.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(1), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT})
		require.NoError(t, err)
		assert.Equal(t, int64(1), fake.direction.GetRef().GetId())
		assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, fake.direction.GetDirection())
	})
	t.Run("GetMigration", func(t *testing.T) {
		_, err := server.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(1)})
		require.NoError(t, err)
		require.NotNil(t, fake.get)
	})
	t.Run("ListMigrations", func(t *testing.T) {
		_, err := server.ListMigrations(ctx, &migratorpb.ListMigrationsRequest{})
		require.NoError(t, err)
		require.NotNil(t, fake.list)
	})
	t.Run("DropMigration", func(t *testing.T) {
		_, err := server.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(1)})
		require.NoError(t, err)
		assert.Equal(t, int64(1), fake.drop.GetRef().GetId())
	})
	t.Run("CreateConnection", func(t *testing.T) {
		resp, err := server.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{Connection: &migratorpb.Connection{Name: "c1"}})
		require.NoError(t, err)
		assert.Equal(t, "c1", resp.GetConnection().GetName())
		require.NotNil(t, fake.createConn)
		assert.Equal(t, "c1", fake.createConn.GetConnection().GetName())
	})
	t.Run("GetConnection", func(t *testing.T) {
		resp, err := server.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connIDRef(1)})
		require.NoError(t, err)
		require.NotNil(t, fake.getConn)
		assert.Equal(t, int64(1), fake.getConn.GetRef().GetId())
		assert.NotContains(t, resp.GetConnection().GetDsn(), "supersecret", "GetConnection must redact the password")
	})
	t.Run("ListConnections", func(t *testing.T) {
		resp, err := server.ListConnections(ctx, &migratorpb.ListConnectionsRequest{})
		require.NoError(t, err)
		require.NotNil(t, fake.listConn)
		require.Len(t, resp.GetConnections(), 1)
		assert.NotContains(t, resp.GetConnections()[0].GetDsn(), "supersecret", "ListConnections must redact the password")
	})
	t.Run("DropConnection", func(t *testing.T) {
		_, err := server.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(1)})
		require.NoError(t, err)
		assert.Equal(t, int64(1), fake.dropConn.GetRef().GetId())
	})
}

// TestRedactConnectionDSN covers the password-redaction helper directly: the
// non-secret fields survive (useful for identifying which source a connection
// points at), the password never does, and a DSN that fails to parse is
// blanked outright rather than risk passing it through verbatim.
func TestRedactConnectionDSN(t *testing.T) {
	t.Run("nil connection is a no-op", func(t *testing.T) {
		redactConnectionDSN(nil)
	})
	t.Run("empty DSN is left alone", func(t *testing.T) {
		c := &migratorpb.Connection{}
		redactConnectionDSN(c)
		assert.Empty(t, c.GetDsn())
	})
	t.Run("password stripped, other fields kept", func(t *testing.T) {
		c := &migratorpb.Connection{Dsn: fakeConnectionDSN}
		redactConnectionDSN(c)
		assert.NotContains(t, c.GetDsn(), "supersecret")
		assert.Contains(t, c.GetDsn(), "src.example.com")
		assert.Contains(t, c.GetDsn(), "postgres")
	})
	t.Run("unparseable DSN is blanked rather than passed through", func(t *testing.T) {
		c := &migratorpb.Connection{Dsn: "not a valid conninfo \x00"}
		redactConnectionDSN(c)
		assert.Empty(t, c.GetDsn())
	})
}

// idRef builds a MigrationRef addressed by id, for tests.
func idRef(id int64) *migratorpb.MigrationRef {
	return &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: id}}
}

func connIDRef(id int64) *migratorpb.ConnectionRef {
	return &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: id}}
}

// callAllForwarders invokes every migration and connection forwarder and
// returns the errors, so a shared failure mode can be asserted across all of
// them.
func callAllForwarders(ctx context.Context, s *MultiadminServer) []error {
	return []error{
		firstErr(s.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{Migration: &migratorpb.MigrationRecord{Name: "m1"}})),
		firstErr(s.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(1), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT})),
		firstErr(s.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(1)})),
		firstErr(s.ListMigrations(ctx, &migratorpb.ListMigrationsRequest{})),
		firstErr(s.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(1)})),
		firstErr(s.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{Connection: &migratorpb.Connection{Name: "c1"}})),
		firstErr(s.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connIDRef(1)})),
		firstErr(s.ListConnections(ctx, &migratorpb.ListConnectionsRequest{})),
		firstErr(s.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(1)})),
	}
}

// firstErr discards a forwarder's response value, keeping only its error.
func firstErr[T any](_ T, err error) error { return err }

// TestMultiadminMigrationNoLeader covers the error path shared by every
// forwarder: when no leader pooler is registered, leaderMigrationClient
// fails and each RPC returns Unavailable.
func TestMultiadminMigrationNoLeader(t *testing.T) {
	server := newTestServer(t, "cell1")
	for _, err := range callAllForwarders(t.Context(), server) {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no leader pooler")
	}
}

// TestMultiadminMigrationDialError covers the branch where a primary pooler is
// found but dialing it fails.
func TestMultiadminMigrationDialError(t *testing.T) {
	server := newTestServer(t, "cell1")
	registerPrimaryPooler(t, server)
	server.migrationDialer = func(context.Context, string) (*grpc.ClientConn, error) {
		return nil, assert.AnError
	}
	for _, err := range callAllForwarders(t.Context(), server) {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dial pooler")
	}
}
