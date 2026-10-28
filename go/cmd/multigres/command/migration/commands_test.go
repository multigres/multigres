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

package migration

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// runE executes a command's RunE with the given args, capturing output and
// returning the error. Usage and error printing are silenced so the returned
// error is exactly what RunE produced.
func runE(cmd *cobra.Command, args ...string) error {
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(args)
	return cmd.Execute()
}

func TestMarkersToSelection(t *testing.T) {
	// "*" -> the all arm.
	obj, err := markersToSelection([]string{"*"})
	require.NoError(t, err)
	require.True(t, obj.GetAll())

	// "schema.*" markers fold into one SchemaSpec.
	obj, err = markersToSelection([]string{"sales.*", "reporting.*"})
	require.NoError(t, err)
	require.Equal(t, []string{"sales", "reporting"}, obj.GetSchema().GetSchemata())

	// Plain "schema.table" markers fold into one TableSpec.
	obj, err = markersToSelection([]string{"public.orders", "public.customers"})
	require.NoError(t, err)
	require.Equal(t, []string{"public.orders", "public.customers"}, obj.GetTable().GetQualifiedNames())

	// Mixing forms is rejected.
	_, err = markersToSelection([]string{"*", "public.orders"})
	require.ErrorContains(t, err, "cannot mix")
	_, err = markersToSelection([]string{"sales.*", "public.orders"})
	require.ErrorContains(t, err, "cannot mix")

	// Empty input.
	obj, err = markersToSelection(nil)
	require.NoError(t, err)
	require.Nil(t, obj)
}

// TestCommandConstruction checks that each subcommand constructor wires its use
// string, its flags, and (where applicable) the required --id flag.
func TestCommandConstruction(t *testing.T) {
	tests := []struct {
		name     string
		cmd      *cobra.Command
		use      string
		flags    []string
		required []string
	}{
		{
			name:  "create",
			cmd:   AddCreateMigrationCommand(),
			use:   "create-migration",
			flags: []string{"admin-server", "connection", "target-database", "target-table-group", "target-shard", "name", "tables", "copy-data", "skip-schema-copy", "quiesce-roles"},
		},
		{
			name:     "set-direction",
			cmd:      AddSetMigrationDirectionCommand(),
			use:      "set-migration-direction",
			flags:    []string{"admin-server", "id", "direction", "max-lag-bytes", "wait-timeout"},
			required: []string{"id", "direction"},
		},
		{
			name:  "list",
			cmd:   AddListMigrationsCommand(),
			use:   "list-migrations",
			flags: []string{"admin-server"},
		},
		{
			name:     "get",
			cmd:      AddGetMigrationCommand(),
			use:      "get-migration",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
		{
			name:     "drop",
			cmd:      AddDropMigrationCommand(),
			use:      "drop-migration",
			flags:    []string{"admin-server", "id", "wait", "wait-timeout", "force"},
			required: []string{"id"},
		},
		{
			name:     "reseed-reverse-link",
			cmd:      AddReseedReverseLinkCommand(),
			use:      "reseed-reverse-link",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.use, tt.cmd.Use)
			assert.NotEmpty(t, tt.cmd.Short)
			assert.NotNil(t, tt.cmd.RunE)
			for _, f := range tt.flags {
				assert.NotNil(t, tt.cmd.Flags().Lookup(f), "flag %q must be defined", f)
			}
			for _, r := range tt.required {
				ann := tt.cmd.Flags().Lookup(r).Annotations[cobra.BashCompOneRequiredFlag]
				assert.NotEmpty(t, ann, "flag %q must be marked required", r)
			}
		})
	}
}

// TestCommandRequiredFlags checks that omitting a required flag fails before RunE.
func TestCommandRequiredFlags(t *testing.T) {
	for _, cmd := range []*cobra.Command{
		AddSetMigrationDirectionCommand(),
		AddGetMigrationCommand(),
		AddDropMigrationCommand(),
		AddReseedReverseLinkCommand(),
	} {
		err := runE(cmd)
		require.Error(t, err, "%s must require a flag", cmd.Use)
		assert.Contains(t, err.Error(), "required flag")
	}
}

// TestCommandValidationErrors covers the RunE argument-validation branches that
// run before any admin client is created.
func TestCommandValidationErrors(t *testing.T) {
	t.Run("drop wait and force are mutually exclusive", func(t *testing.T) {
		err := runE(AddDropMigrationCommand(), "--id", "m1", "--wait", "--force")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})
	t.Run("set-migration-direction rejects an unrecognized direction", func(t *testing.T) {
		err := runE(AddSetMigrationDirectionCommand(), "--id", "m1", "--direction", "sideways")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"import" or "export"`)
	})
}

// TestCommandClientError drives each command's RunE past flag parsing to the
// admin-client construction, which fails deterministically offline (no
// --admin-server and no --config-path flag), exercising the flag-reading and
// error-return paths without a live cluster.
func TestCommandClientError(t *testing.T) {
	cases := []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{"create", AddCreateMigrationCommand(), []string{"--tables", "*"}},
		{"set-direction import", AddSetMigrationDirectionCommand(), []string{"--id", "m1", "--direction", "import"}},
		{"set-direction export", AddSetMigrationDirectionCommand(), []string{"--id", "m1", "--direction", "export"}},
		{"list", AddListMigrationsCommand(), nil},
		{"get", AddGetMigrationCommand(), []string{"--id", "m1"}},
		{"drop", AddDropMigrationCommand(), []string{"--id", "m1", "--wait", "--wait-timeout", "5"}},
		{"reseed-reverse-link", AddReseedReverseLinkCommand(), []string{"--id", "m1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runE(tc.cmd, tc.args...)
			require.Error(t, err)
			// Address resolution fails because neither --admin-server nor a
			// --config-path is available to the standalone command.
			assert.Contains(t, err.Error(), "config-path")
		})
	}
}

// TestPrintJSON checks the protojson rendering helper writes the message to the
// command's output using proto field names.
func TestPrintJSON(t *testing.T) {
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	err := printJSON(cmd, &migratorpb.MigrationRecord{Id: 12345, Name: "nightly"})
	require.NoError(t, err)
	// protojson deliberately randomizes the whitespace after the colon
	// (one or two spaces) run-to-run, so match with a tolerant pattern rather
	// than an exact substring. protojson encodes int64 as a JSON string.
	assert.Regexp(t, `"id": +"12345"`, out.String())
	assert.Regexp(t, `"name": +"nightly"`, out.String())
}

func TestSplitRef(t *testing.T) {
	id, name := splitRef("42")
	assert.Equal(t, int64(42), id)
	assert.Empty(t, name)

	id, name = splitRef("nightly")
	assert.Zero(t, id)
	assert.Equal(t, "nightly", name)
}

func TestToRef(t *testing.T) {
	assert.Equal(t, &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Name{Name: "nightly"}}, toRef(0, "nightly"))
	assert.Equal(t, &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: 42}}, toRef(42, ""))
}

// runEOut is runE but also returns everything the command wrote to its
// output stream, so a test can inspect the printed JSON.
func runEOut(cmd *cobra.Command, args ...string) (string, error) {
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// fakeMultiadminServer is a scriptable multiadminpb.MultiadminServiceServer:
// every Migration RPC method the CLI commands call delegates to the
// corresponding field when set, else falls back to the embedded
// Unimplemented default. This lets a test stand up a real gRPC server (the
// CLI's --admin-server flag needs a real address to dial) without faking the
// whole multiadmin surface.
type fakeMultiadminServer struct {
	multiadminpb.UnimplementedMultiadminServiceServer

	createMigration       func(context.Context, *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error)
	getMigration          func(context.Context, *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error)
	listMigrations        func(context.Context, *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error)
	setMigrationDirection func(context.Context, *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error)
	dropMigration         func(context.Context, *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error)
	reseedReverseLink     func(context.Context, *migratorpb.ReseedReverseLinkRequest) (*migratorpb.ReseedReverseLinkResponse, error)
}

func (f *fakeMultiadminServer) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	if f.createMigration != nil {
		return f.createMigration(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.CreateMigration(ctx, req)
}

func (f *fakeMultiadminServer) GetMigration(ctx context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
	if f.getMigration != nil {
		return f.getMigration(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.GetMigration(ctx, req)
}

func (f *fakeMultiadminServer) ListMigrations(ctx context.Context, req *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
	if f.listMigrations != nil {
		return f.listMigrations(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.ListMigrations(ctx, req)
}

func (f *fakeMultiadminServer) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	if f.setMigrationDirection != nil {
		return f.setMigrationDirection(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.SetMigrationDirection(ctx, req)
}

func (f *fakeMultiadminServer) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	if f.dropMigration != nil {
		return f.dropMigration(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.DropMigration(ctx, req)
}

func (f *fakeMultiadminServer) ReseedReverseLink(ctx context.Context, req *migratorpb.ReseedReverseLinkRequest) (*migratorpb.ReseedReverseLinkResponse, error) {
	if f.reseedReverseLink != nil {
		return f.reseedReverseLink(ctx, req)
	}
	return f.UnimplementedMultiadminServiceServer.ReseedReverseLink(ctx, req)
}

// startFakeAdminServer serves srv on a real local TCP listener and returns its
// address, so a test can point a command's --admin-server flag at it: the CLI
// dials a real address rather than accepting an injectable client.
func startFakeAdminServer(t *testing.T, srv multiadminpb.MultiadminServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	multiadminpb.RegisterMultiadminServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// TestCommandSuccessPaths drives each command's RunE all the way through a
// real (fake) admin server: request construction, the RPC call, and response
// printing — the part TestCommandClientError stops short of, since there the
// client never dials out.
func TestCommandSuccessPaths(t *testing.T) {
	t.Run("create-migration", func(t *testing.T) {
		var gotReq *migratorpb.CreateMigrationRequest
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			createMigration: func(_ context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
				gotReq = req
				return &migratorpb.CreateMigrationResponse{Migration: &migratorpb.MigrationRecord{Id: 42, Name: req.GetMigration().GetName()}}, nil
			},
		})
		out, err := runEOut(AddCreateMigrationCommand(), "--admin-server", addr,
			"--connection", "src", "--target-database", "db1", "--name", "nightly",
			"--tables", "public.orders,public.items", "--quiesce-roles", "app,reporting",
			"--copy-data=false")
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"42"`, out)
		require.NotNil(t, gotReq)
		assert.Equal(t, "src", gotReq.GetMigration().GetConnectionName())
		assert.Equal(t, "db1", gotReq.GetMigration().GetTarget().GetDatabase())
		assert.Equal(t, []string{"app", "reporting"}, gotReq.GetQuiesceRoles())
		assert.Equal(t, []string{"public.orders", "public.items"}, gotReq.GetMigration().GetObjects().GetTable().GetQualifiedNames())
		assert.True(t, gotReq.GetOptions().GetSkipCopyData(), "--copy-data=false must set SkipCopyData")
	})

	t.Run("create-migration leaves skip_copy_data untouched when --copy-data is not set", func(t *testing.T) {
		var gotReq *migratorpb.CreateMigrationRequest
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			createMigration: func(_ context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
				gotReq = req
				return &migratorpb.CreateMigrationResponse{Migration: &migratorpb.MigrationRecord{}}, nil
			},
		})
		_, err := runEOut(AddCreateMigrationCommand(), "--admin-server", addr, "--tables", "*")
		require.NoError(t, err)
		require.NotNil(t, gotReq)
		assert.False(t, gotReq.GetOptions().GetSkipCopyData())
		assert.True(t, gotReq.GetMigration().GetObjects().GetAll())
	})

	t.Run("create-migration rejects a bad --tables selection", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{})
		_, err := runEOut(AddCreateMigrationCommand(), "--admin-server", addr, "--tables", "*,public.orders")
		require.ErrorContains(t, err, "cannot mix")
	})

	t.Run("create-migration wraps the RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			createMigration: func(context.Context, *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
				return nil, errors.New("boom")
			},
		})
		_, err := runEOut(AddCreateMigrationCommand(), "--admin-server", addr, "--tables", "*")
		require.ErrorContains(t, err, "failed to create migration")
	})

	t.Run("set-migration-direction", func(t *testing.T) {
		var gotReq *migratorpb.SetMigrationDirectionRequest
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			setMigrationDirection: func(_ context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
				gotReq = req
				return &migratorpb.SetMigrationDirectionResponse{Migration: &migratorpb.MigrationRecord{Id: 7}}, nil
			},
		})
		out, err := runEOut(AddSetMigrationDirectionCommand(), "--admin-server", addr,
			"--id", "7", "--direction", "EXPORT", "--max-lag-bytes", "8 MiB", "--wait-timeout", "30")
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"7"`, out)
		require.NotNil(t, gotReq)
		assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, gotReq.GetDirection())
		assert.Equal(t, uint64(8*1024*1024), gotReq.GetMaxLagBytes())
		assert.Equal(t, int64(30), gotReq.GetWaitTimeoutSeconds())
		assert.Equal(t, int64(7), gotReq.GetRef().GetId())
	})

	t.Run("set-migration-direction by name, with no --max-lag-bytes", func(t *testing.T) {
		var gotReq *migratorpb.SetMigrationDirectionRequest
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			setMigrationDirection: func(_ context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
				gotReq = req
				return &migratorpb.SetMigrationDirectionResponse{Migration: &migratorpb.MigrationRecord{Name: "nightly"}}, nil
			},
		})
		_, err := runEOut(AddSetMigrationDirectionCommand(), "--admin-server", addr, "--id", "nightly", "--direction", "import")
		require.NoError(t, err)
		require.NotNil(t, gotReq)
		assert.Equal(t, "nightly", gotReq.GetRef().GetName())
		assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, gotReq.GetDirection())
		assert.Zero(t, gotReq.GetMaxLagBytes())
	})

	t.Run("set-migration-direction rejects an unparsable --max-lag-bytes", func(t *testing.T) {
		_, err := runEOut(AddSetMigrationDirectionCommand(), "--id", "1", "--direction", "export", "--max-lag-bytes", "not-a-size")
		require.ErrorContains(t, err, "--max-lag-bytes")
	})

	t.Run("set-migration-direction wraps the RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			setMigrationDirection: func(context.Context, *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
				return nil, errors.New("boom")
			},
		})
		_, err := runEOut(AddSetMigrationDirectionCommand(), "--admin-server", addr, "--id", "1", "--direction", "import")
		require.ErrorContains(t, err, "failed to set migration direction")
	})

	t.Run("get-migration", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			getMigration: func(_ context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
				return &migratorpb.GetMigrationResponse{Migration: &migratorpb.MigrationRecord{Id: req.GetRef().GetId()}}, nil
			},
		})
		out, err := runEOut(AddGetMigrationCommand(), "--admin-server", addr, "--id", "9")
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"9"`, out)
	})

	t.Run("get-migration wraps the RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			getMigration: func(context.Context, *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
				return nil, errors.New("not found")
			},
		})
		_, err := runEOut(AddGetMigrationCommand(), "--admin-server", addr, "--id", "9")
		require.ErrorContains(t, err, "failed to get migration")
	})

	t.Run("list-migrations", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			listMigrations: func(context.Context, *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
				return &migratorpb.ListMigrationsResponse{Ids: []int64{1, 2}}, nil
			},
			getMigration: func(_ context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
				return &migratorpb.GetMigrationResponse{Migration: &migratorpb.MigrationRecord{Id: req.GetRef().GetId()}}, nil
			},
		})
		out, err := runEOut(AddListMigrationsCommand(), "--admin-server", addr)
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"1"`, out)
		assert.Regexp(t, `"id": +"2"`, out)
	})

	t.Run("list-migrations wraps a per-id GetMigration error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			listMigrations: func(context.Context, *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
				return &migratorpb.ListMigrationsResponse{Ids: []int64{5}}, nil
			},
			getMigration: func(context.Context, *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
				return nil, errors.New("gone")
			},
		})
		_, err := runEOut(AddListMigrationsCommand(), "--admin-server", addr)
		require.ErrorContains(t, err, "failed to get migration 5")
	})

	t.Run("list-migrations wraps the ListMigrations RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			listMigrations: func(context.Context, *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
				return nil, errors.New("boom")
			},
		})
		_, err := runEOut(AddListMigrationsCommand(), "--admin-server", addr)
		require.ErrorContains(t, err, "failed to list migrations")
	})

	t.Run("drop-migration", func(t *testing.T) {
		var gotReq *migratorpb.DropMigrationRequest
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			dropMigration: func(_ context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
				gotReq = req
				return &migratorpb.DropMigrationResponse{Migration: &migratorpb.MigrationRecord{Id: req.GetRef().GetId()}}, nil
			},
		})
		out, err := runEOut(AddDropMigrationCommand(), "--admin-server", addr, "--id", "3", "--wait", "--wait-timeout", "15")
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"3"`, out)
		require.NotNil(t, gotReq)
		assert.True(t, gotReq.GetWait())
		assert.Equal(t, int64(15), gotReq.GetWaitTimeoutSeconds())
		assert.False(t, gotReq.GetForce())
	})

	t.Run("drop-migration wraps the RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			dropMigration: func(context.Context, *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
				return nil, errors.New("boom")
			},
		})
		_, err := runEOut(AddDropMigrationCommand(), "--admin-server", addr, "--id", "3")
		require.ErrorContains(t, err, "failed to drop migration")
	})

	t.Run("reseed-reverse-link", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			reseedReverseLink: func(_ context.Context, req *migratorpb.ReseedReverseLinkRequest) (*migratorpb.ReseedReverseLinkResponse, error) {
				return &migratorpb.ReseedReverseLinkResponse{Migration: &migratorpb.MigrationRecord{Id: req.GetRef().GetId()}}, nil
			},
		})
		out, err := runEOut(AddReseedReverseLinkCommand(), "--admin-server", addr, "--id", "4")
		require.NoError(t, err)
		assert.Regexp(t, `"id": +"4"`, out)
	})

	t.Run("reseed-reverse-link wraps the RPC error", func(t *testing.T) {
		addr := startFakeAdminServer(t, &fakeMultiadminServer{
			reseedReverseLink: func(context.Context, *migratorpb.ReseedReverseLinkRequest) (*migratorpb.ReseedReverseLinkResponse, error) {
				return nil, errors.New("boom")
			},
		})
		_, err := runEOut(AddReseedReverseLinkCommand(), "--admin-server", addr, "--id", "4")
		require.ErrorContains(t, err, "failed to reseed reverse link")
	})
}
