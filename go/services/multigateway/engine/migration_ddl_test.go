// Copyright 2026 Supabase, Inc.
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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// fakeMigrator records the last request per RPC and returns canned responses.
// conns backs the Connection RPCs with a simple in-memory map, keyed by name,
// standing in for multigres.migration_connection.
type fakeMigrator struct {
	create     *migratorpb.CreateMigrationRequest
	start      *migratorpb.StartMigrationRequest
	activate   *migratorpb.ActivateMigrationRequest
	deactivate *migratorpb.DeactivateMigrationRequest
	update     *migratorpb.UpdateMigrationRequest
	drop       *migratorpb.DropMigrationRequest
	get        *migratorpb.GetMigrationRequest
	list       *migratorpb.ListMigrationsRequest
	getJournal *migratorpb.GetMigrationJournalRequest
	migrations []*migratorpb.GetMigrationResponse

	conns      map[string]*migratorpb.Connection
	nextConnID int64
}

// checkConnectionExists simulates the real coordinator's CreateMigration/
// UpdateMigration validation: a non-empty connection_name must name a
// connection already created via CreateConnection (or seeded into f.conns).
func (f *fakeMigrator) checkConnectionExists(name string) error {
	if name == "" {
		return nil
	}
	if _, ok := f.conns[name]; !ok {
		return status.Errorf(codes.NotFound, "connection %q does not exist", name)
	}
	return nil
}

func (f *fakeMigrator) CreateMigration(_ context.Context, in *migratorpb.CreateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.CreateMigrationResponse, error) {
	if err := f.checkConnectionExists(in.GetMigration().GetConnectionName()); err != nil {
		return nil, err
	}
	f.create = in
	return &migratorpb.CreateMigrationResponse{}, nil
}

func (f *fakeMigrator) StartMigration(_ context.Context, in *migratorpb.StartMigrationRequest, _ ...grpc.CallOption) (*migratorpb.StartMigrationResponse, error) {
	f.start = in
	return &migratorpb.StartMigrationResponse{}, nil
}

func (f *fakeMigrator) UpdateMigration(_ context.Context, in *migratorpb.UpdateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.UpdateMigrationResponse, error) {
	if err := f.checkConnectionExists(in.GetMigration().GetConnectionName()); err != nil {
		return nil, err
	}
	f.update = in
	return &migratorpb.UpdateMigrationResponse{}, nil
}

func (f *fakeMigrator) GetMigration(_ context.Context, in *migratorpb.GetMigrationRequest, _ ...grpc.CallOption) (*migratorpb.GetMigrationResponse, error) {
	f.get = in
	if len(f.migrations) > 0 {
		return f.migrations[0], nil
	}
	return &migratorpb.GetMigrationResponse{}, nil
}

func (f *fakeMigrator) ListMigrations(_ context.Context, in *migratorpb.ListMigrationsRequest, _ ...grpc.CallOption) (*migratorpb.ListMigrationsResponse, error) {
	f.list = in
	ids := make([]int64, len(f.migrations))
	for i, mi := range f.migrations {
		ids[i] = mi.GetMigration().GetId()
	}
	return &migratorpb.ListMigrationsResponse{Ids: ids}, nil
}

func (f *fakeMigrator) ActivateMigration(_ context.Context, in *migratorpb.ActivateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.ActivateMigrationResponse, error) {
	f.activate = in
	return &migratorpb.ActivateMigrationResponse{}, nil
}

func (f *fakeMigrator) DeactivateMigration(_ context.Context, in *migratorpb.DeactivateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.DeactivateMigrationResponse, error) {
	f.deactivate = in
	return &migratorpb.DeactivateMigrationResponse{}, nil
}

func (f *fakeMigrator) DropMigration(_ context.Context, in *migratorpb.DropMigrationRequest, _ ...grpc.CallOption) (*migratorpb.DropMigrationResponse, error) {
	f.drop = in
	return &migratorpb.DropMigrationResponse{}, nil
}

func (f *fakeMigrator) GetMigrationJournal(_ context.Context, in *migratorpb.GetMigrationJournalRequest, _ ...grpc.CallOption) (*migratorpb.GetMigrationJournalResponse, error) {
	f.getJournal = in
	return &migratorpb.GetMigrationJournalResponse{}, nil
}

func (f *fakeMigrator) CreateConnection(_ context.Context, in *migratorpb.CreateConnectionRequest, _ ...grpc.CallOption) (*migratorpb.CreateConnectionResponse, error) {
	if f.conns == nil {
		f.conns = map[string]*migratorpb.Connection{}
	}
	f.nextConnID++
	c := &migratorpb.Connection{
		Id: f.nextConnID, Name: in.GetConnection().GetName(), Dsn: in.GetConnection().GetDsn(),
	}
	f.conns[c.Name] = c
	return &migratorpb.CreateConnectionResponse{Connection: c}, nil
}

func (f *fakeMigrator) UpdateConnection(_ context.Context, in *migratorpb.UpdateConnectionRequest, _ ...grpc.CallOption) (*migratorpb.UpdateConnectionResponse, error) {
	for _, c := range f.conns {
		if c.GetId() == in.GetConnection().GetId() {
			for _, p := range in.GetUpdateMask().GetPaths() {
				if p == "dsn" {
					c.Dsn = in.GetConnection().GetDsn()
				}
			}
			return &migratorpb.UpdateConnectionResponse{Connection: c}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "connection not found")
}

func (f *fakeMigrator) GetConnection(_ context.Context, in *migratorpb.GetConnectionRequest, _ ...grpc.CallOption) (*migratorpb.GetConnectionResponse, error) {
	if c, ok := f.conns[in.GetRef().GetName()]; ok {
		return &migratorpb.GetConnectionResponse{Connection: c}, nil
	}
	return nil, status.Error(codes.NotFound, "connection not found")
}

func (f *fakeMigrator) ListConnections(_ context.Context, _ *migratorpb.ListConnectionsRequest, _ ...grpc.CallOption) (*migratorpb.ListConnectionsResponse, error) {
	out := make([]*migratorpb.Connection, 0, len(f.conns))
	for _, c := range f.conns {
		out = append(out, c)
	}
	return &migratorpb.ListConnectionsResponse{Connections: out}, nil
}

func (f *fakeMigrator) DropConnection(_ context.Context, in *migratorpb.DropConnectionRequest, _ ...grpc.CallOption) (*migratorpb.DropConnectionResponse, error) {
	name := in.GetRef().GetName()
	if _, ok := f.conns[name]; !ok {
		if in.GetIfExists() {
			return &migratorpb.DropConnectionResponse{}, nil
		}
		return nil, status.Error(codes.NotFound, "connection not found")
	}
	delete(f.conns, name)
	return &migratorpb.DropConnectionResponse{}, nil
}

func newTestBackend(fake migratorpb.MigratorClient) *MigrationBackend {
	return &MigrationBackend{
		Client:         func() migratorpb.MigratorClient { return fake },
		TargetDatabase: "appdb",
		TargetShard:    "0",
	}
}

// runSQL parses one statement and executes the migration primitive, returning
// the synthesized result.
func runSQL(t *testing.T, backend *MigrationBackend, sql string) (*sqltypes.Result, error) {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	p := NewMigrationDDL(sql, stmts[0], backend)
	var got *sqltypes.Result
	execErr := p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })
	return got, execErr
}

func TestMigrationDDL_Connections(t *testing.T) {
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)

	res, err := runSQL(t, backend, "CREATE CONNECTION onprem OPTIONS (host 'db.example.com', dbname 'app', user 'repl')")
	require.NoError(t, err)
	assert.Equal(t, "CREATE CONNECTION", res.CommandTag)
	dsn := fake.conns["onprem"].GetDsn()
	assert.Contains(t, dsn, "host='db.example.com'")
	assert.Contains(t, dsn, "dbname='app'")

	// SHOW CONNECTIONS (list) — the planner turns the plural into a name-less show.
	show, err := runSQL(t, backend, "SHOW CONNECTION onprem")
	require.NoError(t, err)
	require.Len(t, show.Rows, 1)
	assert.Equal(t, "onprem", string(show.Rows[0].Values[0]))
	assert.Equal(t, "db.example.com", string(show.Rows[0].Values[1]))

	_, err = runSQL(t, backend, "DROP CONNECTION onprem")
	require.NoError(t, err)
	_, ok := fake.conns["onprem"]
	assert.False(t, ok)

	// DROP of a missing connection errors unless IF EXISTS.
	_, err = runSQL(t, backend, "DROP CONNECTION missing")
	assert.Error(t, err)
	_, err = runSQL(t, backend, "DROP CONNECTION IF EXISTS missing")
	assert.NoError(t, err)
}

func TestMigrationDDL_ConnectionDefaultsAndHostRequired(t *testing.T) {
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)

	// host is required; a host-less CREATE CONNECTION errors and stores nothing.
	_, err := runSQL(t, backend, "CREATE CONNECTION nohost OPTIONS (user 'u')")
	require.ErrorContains(t, err, "host")
	_, ok := fake.conns["nohost"]
	assert.False(t, ok)

	// Only a host given: port=5432, dbname=postgres, sslmode=disable are filled in.
	_, err = runSQL(t, backend, "CREATE CONNECTION only_host OPTIONS (host 'src')")
	require.NoError(t, err)
	assert.Equal(t, "host='src' port=5432 dbname=postgres sslmode=disable", fake.conns["only_host"].GetDsn())

	// Explicitly-supplied options win over the defaults.
	_, err = runSQL(t, backend, "CREATE CONNECTION custom OPTIONS (host 'src', port '6543', dbname 'app', sslmode 'require')")
	require.NoError(t, err)
	assert.Equal(t, "host='src' port='6543' dbname='app' sslmode='require'", fake.conns["custom"].GetDsn())
}

func TestMigrationDDL_CreateMigration(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"onprem": {Id: 1, Name: "onprem", Dsn: "host=src dbname=app"}}}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "CREATE MIGRATION m CONNECTION onprem FOR ALL TABLES")
	require.NoError(t, err)
	require.NotNil(t, fake.create)
	assert.Equal(t, "m", fake.create.GetMigration().GetName())
	assert.Equal(t, "onprem", fake.create.GetMigration().GetConnectionName())
	assert.Equal(t, "appdb", fake.create.GetMigration().GetTargetDatabase())
	assert.Equal(t, "0", fake.create.GetMigration().GetTargetShard())
	assert.True(t, fake.create.GetMigration().GetObjects().GetAll())

	_, err = runSQL(t, backend, "CREATE MIGRATION m2 CONNECTION onprem FOR TABLE orders, customers WITH (copy_data = false, sequence_margin = 5)")
	require.NoError(t, err)
	assert.Equal(t, []string{"orders", "customers"}, fake.create.GetMigration().GetObjects().GetTable().GetQualifiedNames())
	assert.True(t, fake.create.GetSkipCopyData())
	assert.Equal(t, int64(5), fake.create.GetMigration().GetSequenceMargin())

	// schema selection
	_, err = runSQL(t, backend, "CREATE MIGRATION m3 CONNECTION onprem FOR TABLES IN SCHEMA public")
	require.NoError(t, err)
	assert.Equal(t, []string{"public"}, fake.create.GetMigration().GetObjects().GetSchema().GetSchemata())

	// unknown connection
	_, err = runSQL(t, backend, "CREATE MIGRATION bad CONNECTION nope FOR ALL TABLES")
	assert.ErrorContains(t, err, "nope")
}

func TestMigrationDDL_LifecycleAndDrop(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"c": {Id: 1, Name: "c", Dsn: "host=x"}}}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "ALTER MIGRATION m START")
	require.NoError(t, err)
	require.NotNil(t, fake.start)
	assert.Equal(t, "m", fake.start.GetRef().GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m ACTIVATE")
	require.NoError(t, err)
	assert.Equal(t, "m", fake.activate.GetRef().GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m DEACTIVATE")
	require.NoError(t, err)
	assert.Equal(t, "m", fake.deactivate.GetRef().GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m CONNECTION c")
	require.NoError(t, err)
	require.NotNil(t, fake.update)
	assert.Equal(t, "c", fake.update.GetMigration().GetConnectionName())
	assert.Equal(t, []string{"connection_name"}, fake.update.GetUpdateMask().GetPaths())

	_, err = runSQL(t, backend, "ALTER MIGRATION m SET (sequence_margin = 42)")
	require.NoError(t, err)
	assert.Equal(t, int64(42), fake.update.GetMigration().GetSequenceMargin())
	assert.Equal(t, []string{"sequence_margin"}, fake.update.GetUpdateMask().GetPaths())

	_, err = runSQL(t, backend, "DROP MIGRATION m FORCE")
	require.NoError(t, err)
	require.NotNil(t, fake.drop)
	assert.True(t, fake.drop.GetForce())

	_, err = runSQL(t, backend, "DROP MIGRATION m WAIT (30)")
	require.NoError(t, err)
	assert.True(t, fake.drop.GetWait())
	assert.Equal(t, int64(30), fake.drop.GetWaitTimeoutSeconds())
}

func TestMigrationDDL_ShowMigrations(t *testing.T) {
	copyDone := true
	fake := &fakeMigrator{migrations: []*migratorpb.GetMigrationResponse{
		{
			Migration: &migratorpb.Migration{
				Name: "orders_move", Id: 1, ConnectionName: "src",
				TargetDatabase: "appdb", TargetShard: "0",
			},
			Status: &migratorpb.MigrationStatus{
				Id:              1,
				Phase:           migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING,
				ActiveDirection: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT,
				CaughtUp:        copyDone,
				LagBytes:        4096,
				LagSeconds:      1.5,
			},
		},
	}}
	backend := newTestBackend(fake)

	res, err := runSQL(t, backend, "SHOW MIGRATION orders_move")
	require.NoError(t, err)
	assert.Equal(t, "orders_move", fake.get.GetRef().GetName())
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Fields, 13)
	assert.Equal(t, "lag_bytes", res.Fields[10].Name)
	assert.Equal(t, "lag_seconds", res.Fields[11].Name)
	vals := res.Rows[0].Values
	assert.Equal(t, "orders_move", string(vals[0]))
	assert.Equal(t, "1", string(vals[1]))
	assert.Equal(t, "4096", string(vals[10]))
	assert.Equal(t, "1.500", string(vals[11]))
}

func TestMigrationDDL_UnconfiguredBackend(t *testing.T) {
	stmts, err := parser.ParseSQL("CREATE CONNECTION x OPTIONS (host 'h')")
	require.NoError(t, err)
	p := NewMigrationDDL("sql", stmts[0], nil)
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")
}
