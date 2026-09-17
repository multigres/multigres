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

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// fakeMigrator records the last request per RPC and returns canned responses.
type fakeMigrator struct {
	create     *migratorpb.CreateMigrationRequest
	start      *migratorpb.StartMigrationRequest
	activate   *migratorpb.ActivateMigrationRequest
	deactivate *migratorpb.DeactivateMigrationRequest
	update     *migratorpb.UpdateMigrationRequest
	drop       *migratorpb.DropMigrationRequest
	get        *migratorpb.GetMigrationsRequest
	migrations []*migratorpb.Migration
}

func (f *fakeMigrator) CreateMigration(_ context.Context, in *migratorpb.CreateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.CreateMigrationResponse, error) {
	f.create = in
	return &migratorpb.CreateMigrationResponse{}, nil
}

func (f *fakeMigrator) StartMigration(_ context.Context, in *migratorpb.StartMigrationRequest, _ ...grpc.CallOption) (*migratorpb.StartMigrationResponse, error) {
	f.start = in
	return &migratorpb.StartMigrationResponse{}, nil
}

func (f *fakeMigrator) UpdateMigration(_ context.Context, in *migratorpb.UpdateMigrationRequest, _ ...grpc.CallOption) (*migratorpb.UpdateMigrationResponse, error) {
	f.update = in
	return &migratorpb.UpdateMigrationResponse{}, nil
}

func (f *fakeMigrator) GetMigrations(_ context.Context, in *migratorpb.GetMigrationsRequest, _ ...grpc.CallOption) (*migratorpb.GetMigrationsResponse, error) {
	f.get = in
	return &migratorpb.GetMigrationsResponse{Migrations: f.migrations}, nil
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

func newTestBackend(fake migratorpb.MigratorClient) *MigrationBackend {
	return &MigrationBackend{
		Client:         func() migratorpb.MigratorClient { return fake },
		Conns:          NewInMemoryConnectionStore(),
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
	backend := newTestBackend(&fakeMigrator{})

	res, err := runSQL(t, backend, "CREATE CONNECTION onprem OPTIONS (host 'db.example.com', dbname 'app', user 'repl')")
	require.NoError(t, err)
	assert.Equal(t, "CREATE CONNECTION", res.CommandTag)
	dsn, ok := backend.Conns.Get("onprem")
	require.True(t, ok)
	assert.Contains(t, dsn, "host=db.example.com")
	assert.Contains(t, dsn, "dbname=app")

	// SHOW CONNECTIONS (list) — the planner turns the plural into a name-less show.
	show, err := runSQL(t, backend, "SHOW CONNECTION onprem")
	require.NoError(t, err)
	require.Len(t, show.Rows, 1)
	assert.Equal(t, "onprem", string(show.Rows[0].Values[0]))
	assert.Equal(t, "db.example.com", string(show.Rows[0].Values[1]))

	_, err = runSQL(t, backend, "DROP CONNECTION onprem")
	require.NoError(t, err)
	_, ok = backend.Conns.Get("onprem")
	assert.False(t, ok)

	// DROP of a missing connection errors unless IF EXISTS.
	_, err = runSQL(t, backend, "DROP CONNECTION missing")
	assert.Error(t, err)
	_, err = runSQL(t, backend, "DROP CONNECTION IF EXISTS missing")
	assert.NoError(t, err)
}

func TestMigrationDDL_CreateMigration(t *testing.T) {
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)
	backend.Conns.Set("onprem", "host=src dbname=app")

	_, err := runSQL(t, backend, "CREATE MIGRATION m CONNECTION onprem FOR ALL TABLES")
	require.NoError(t, err)
	require.NotNil(t, fake.create)
	assert.Equal(t, "m", fake.create.GetName())
	assert.Equal(t, "host=src dbname=app", fake.create.GetSourceDsn())
	assert.Equal(t, "appdb", fake.create.GetTargetDatabase())
	assert.Equal(t, "0", fake.create.GetTargetShard())
	assert.True(t, fake.create.GetAllTables())
	assert.Empty(t, fake.create.GetObjects())

	_, err = runSQL(t, backend, "CREATE MIGRATION m2 CONNECTION onprem FOR TABLE orders, customers WITH (copy_data = false, sequence_margin = 5)")
	require.NoError(t, err)
	assert.False(t, fake.create.GetAllTables())
	require.Len(t, fake.create.GetObjects(), 2)
	assert.Equal(t, "orders", fake.create.GetObjects()[0].GetTable().GetQualifiedName())
	require.NotNil(t, fake.create.CopyData)
	assert.False(t, fake.create.GetCopyData())
	assert.Equal(t, int64(5), fake.create.GetSequenceMargin())

	// schema selection
	_, err = runSQL(t, backend, "CREATE MIGRATION m3 CONNECTION onprem FOR TABLES IN SCHEMA public")
	require.NoError(t, err)
	require.Len(t, fake.create.GetObjects(), 1)
	assert.Equal(t, "public", fake.create.GetObjects()[0].GetSchema())

	// unknown connection
	_, err = runSQL(t, backend, "CREATE MIGRATION bad CONNECTION nope FOR ALL TABLES")
	assert.ErrorContains(t, err, "nope")
}

func TestMigrationDDL_LifecycleAndDrop(t *testing.T) {
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)
	backend.Conns.Set("c", "host=x")

	_, err := runSQL(t, backend, "ALTER MIGRATION m START")
	require.NoError(t, err)
	require.NotNil(t, fake.start)
	assert.Equal(t, "m", fake.start.GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m ACTIVATE")
	require.NoError(t, err)
	assert.Equal(t, "m", fake.activate.GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m DEACTIVATE")
	require.NoError(t, err)
	assert.Equal(t, "m", fake.deactivate.GetName())

	_, err = runSQL(t, backend, "ALTER MIGRATION m CONNECTION c")
	require.NoError(t, err)
	require.NotNil(t, fake.update)
	assert.Equal(t, "host=x", fake.update.GetSourceDsn())
	assert.Equal(t, []string{"source_dsn"}, fake.update.GetUpdateMask().GetPaths())

	_, err = runSQL(t, backend, "ALTER MIGRATION m SET (sequence_margin = 42)")
	require.NoError(t, err)
	assert.Equal(t, int64(42), fake.update.GetSequenceMargin())
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
	fake := &fakeMigrator{migrations: []*migratorpb.Migration{
		{
			Name: "orders_move", Id: "m1", Source: "host=src",
			TargetDatabase: "appdb", TargetShard: "0",
			Phase:           migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING,
			ActiveDirection: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT,
			CaughtUp:        copyDone,
		},
	}}
	backend := newTestBackend(fake)

	res, err := runSQL(t, backend, "SHOW MIGRATION orders_move")
	require.NoError(t, err)
	assert.Equal(t, "orders_move", fake.get.GetName())
	require.Len(t, res.Rows, 1)
	assert.NotEmpty(t, res.Fields)
	vals := res.Rows[0].Values
	assert.Equal(t, "orders_move", string(vals[0]))
	assert.Equal(t, "m1", string(vals[1]))
}

func TestMigrationDDL_UnconfiguredBackend(t *testing.T) {
	stmts, err := parser.ParseSQL("CREATE CONNECTION x OPTIONS (host 'h')")
	require.NoError(t, err)
	p := NewMigrationDDL("sql", stmts[0], nil)
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")
}
