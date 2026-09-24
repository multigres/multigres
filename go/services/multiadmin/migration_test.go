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
	start      *migratorpb.StartMigrationRequest
	update     *migratorpb.UpdateMigrationRequest
	activate   *migratorpb.ActivateMigrationRequest
	deactivate *migratorpb.DeactivateMigrationRequest
	get        *migratorpb.GetMigrationsRequest
	drop       *migratorpb.DropMigrationRequest
}

func (f *fakeMigratorServer) CreateMigration(_ context.Context, in *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	f.create = in
	return &migratorpb.CreateMigrationResponse{Migration: &migratorpb.Migration{Name: in.GetName()}}, nil
}

func (f *fakeMigratorServer) StartMigration(_ context.Context, in *migratorpb.StartMigrationRequest) (*migratorpb.StartMigrationResponse, error) {
	f.start = in
	return &migratorpb.StartMigrationResponse{Migration: &migratorpb.Migration{Id: in.GetId()}}, nil
}

func (f *fakeMigratorServer) UpdateMigration(_ context.Context, in *migratorpb.UpdateMigrationRequest) (*migratorpb.UpdateMigrationResponse, error) {
	f.update = in
	return &migratorpb.UpdateMigrationResponse{Migration: &migratorpb.Migration{Id: in.GetId()}}, nil
}

func (f *fakeMigratorServer) ActivateMigration(_ context.Context, in *migratorpb.ActivateMigrationRequest) (*migratorpb.ActivateMigrationResponse, error) {
	f.activate = in
	return &migratorpb.ActivateMigrationResponse{Migration: &migratorpb.Migration{Id: in.GetId()}}, nil
}

func (f *fakeMigratorServer) DeactivateMigration(_ context.Context, in *migratorpb.DeactivateMigrationRequest) (*migratorpb.DeactivateMigrationResponse, error) {
	f.deactivate = in
	return &migratorpb.DeactivateMigrationResponse{Migration: &migratorpb.Migration{Id: in.GetId()}}, nil
}

func (f *fakeMigratorServer) GetMigrations(_ context.Context, in *migratorpb.GetMigrationsRequest) (*migratorpb.GetMigrationsResponse, error) {
	f.get = in
	return &migratorpb.GetMigrationsResponse{}, nil
}

func (f *fakeMigratorServer) DropMigration(_ context.Context, in *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	f.drop = in
	return &migratorpb.DropMigrationResponse{Migration: &migratorpb.Migration{Id: in.GetId()}}, nil
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
		resp, err := server.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{Name: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", resp.GetMigration().GetName())
		require.NotNil(t, fake.create)
		assert.Equal(t, "m1", fake.create.GetName())
	})
	t.Run("StartMigration", func(t *testing.T) {
		_, err := server.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", fake.start.GetId())
	})
	t.Run("UpdateMigration", func(t *testing.T) {
		_, err := server.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{Id: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", fake.update.GetId())
	})
	t.Run("ActivateMigration", func(t *testing.T) {
		_, err := server.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", fake.activate.GetId())
	})
	t.Run("DeactivateMigration", func(t *testing.T) {
		_, err := server.DeactivateMigration(ctx, &migratorpb.DeactivateMigrationRequest{Id: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", fake.deactivate.GetId())
	})
	t.Run("GetMigrations", func(t *testing.T) {
		_, err := server.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{})
		require.NoError(t, err)
		require.NotNil(t, fake.get)
	})
	t.Run("DropMigration", func(t *testing.T) {
		_, err := server.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: "m1"})
		require.NoError(t, err)
		assert.Equal(t, "m1", fake.drop.GetId())
	})
}

// callAllForwarders invokes every migration forwarder and returns the errors,
// so a shared failure mode can be asserted across all of them.
func callAllForwarders(ctx context.Context, s *MultiadminServer) []error {
	return []error{
		firstErr(s.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{Name: "m1"})),
		firstErr(s.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: "m1"})),
		firstErr(s.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{Id: "m1"})),
		firstErr(s.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: "m1"})),
		firstErr(s.DeactivateMigration(ctx, &migratorpb.DeactivateMigrationRequest{Id: "m1"})),
		firstErr(s.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{})),
		firstErr(s.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: "m1"})),
	}
}

// firstErr discards a forwarder's response value, keeping only its error.
func firstErr[T any](_ T, err error) error { return err }

// TestMultiadminMigrationNoPrimary covers the error path shared by every
// forwarder: when no primary pooler is registered, primaryMigrationClient
// fails and each RPC returns Unavailable.
func TestMultiadminMigrationNoPrimary(t *testing.T) {
	server := newTestServer(t, "cell1")
	for _, err := range callAllForwarders(t.Context(), server) {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no primary pooler")
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
