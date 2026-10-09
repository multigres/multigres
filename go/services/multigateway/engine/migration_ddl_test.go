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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// privilegedTestConn returns a *server.Conn whose cached credentials satisfy
// checkMigrationDDLPrivilege, standing in for an authenticated role that could
// itself run CREATE PUBLICATION/CREATE SUBSCRIPTION. Tests that aren't
// exercising the privilege check itself use this so they still reach the
// behavior under test.
func privilegedTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{},
		server.WithTestCredentials(&server.Credentials{CanCreateMigration: true})).Conn
}

// privilegedTestConnAs is privilegedTestConn with a specific authenticated
// username, for tests asserting that identity reaches a downstream request.
func privilegedTestConnAs(user string) *server.Conn {
	return server.NewTestConn(&bytes.Buffer{},
		server.WithTestUser(user),
		server.WithTestCredentials(&server.Credentials{CanCreateMigration: true})).Conn
}

// fakeMigrator records the last request per RPC and returns canned responses.
// conns backs the Connection RPCs with a simple in-memory map, keyed by name,
// standing in for multigres.migration_connection.
type fakeMigrator struct {
	create     *migratorpb.CreateMigrationRequest
	direction  *migratorpb.SetMigrationDirectionRequest
	drop       *migratorpb.DropMigrationRequest
	get        *migratorpb.GetMigrationRequest
	list       *migratorpb.ListMigrationsRequest
	getJournal *migratorpb.GetMigrationJournalRequest
	migrations []*migratorpb.GetMigrationResponse

	conns      map[string]*migratorpb.Connection
	nextConnID int64
}

// checkConnectionExists simulates the real coordinator's CreateMigration
// validation: a non-empty connection_name must name a connection already
// created via CreateConnection (or seeded into f.conns).
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

func (f *fakeMigrator) SetMigrationDirection(_ context.Context, in *migratorpb.SetMigrationDirectionRequest, _ ...grpc.CallOption) (*migratorpb.SetMigrationDirectionResponse, error) {
	f.direction = in
	return &migratorpb.SetMigrationDirectionResponse{}, nil
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
	execErr := p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })
	return got, execErr
}

// TestMigrationDDL_RequiresPrivilege is the regression test for the HIGH
// finding this check guards against: StreamExecute/PortalStreamExecute used
// to discard the caller's identity entirely, so migration/connection DDL
// executed with no authorization check at all. An unprivileged or unknown
// (nil) caller must now be rejected with SQLSTATE 42501 before the statement
// ever dispatches to the migrator backend; a privileged caller must still go
// through untouched (covered by every other test in this file via
// privilegedTestConn).
func TestMigrationDDL_RequiresPrivilege(t *testing.T) {
	stmts, err := parser.ParseSQL("SHOW MIGRATIONS")
	require.NoError(t, err)
	backend := newTestBackend(&fakeMigrator{})

	unprivileged := server.NewTestConn(&bytes.Buffer{},
		server.WithTestCredentials(&server.Credentials{CanCreateMigration: false})).Conn

	for name, conn := range map[string]*server.Conn{
		"unprivileged role": unprivileged,
		"nil conn":          nil,
	} {
		t.Run(name, func(t *testing.T) {
			p := NewMigrationDDL("SHOW MIGRATIONS", stmts[0], backend)
			called := false
			err := p.StreamExecute(context.Background(), nil, conn, nil, nil, PlanExecInfo{},
				func(context.Context, *sqltypes.Result) error { called = true; return nil })
			require.Error(t, err)
			assert.Equal(t, "42501", mterrors.ExtractSQLSTATE(err))
			assert.False(t, called, "the backend must never be reached for an unprivileged caller")
		})
	}
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

	// Only a host given: port=5432, dbname=postgres, sslmode=require are filled in.
	_, err = runSQL(t, backend, "CREATE CONNECTION only_host OPTIONS (host 'src')")
	require.NoError(t, err)
	assert.Equal(t, "host='src' port=5432 dbname=postgres sslmode=require", fake.conns["only_host"].GetDsn())

	// Explicitly-supplied options win over the defaults.
	_, err = runSQL(t, backend, "CREATE CONNECTION custom OPTIONS (host 'src', port '6543', dbname 'app', sslmode 'disable')")
	require.NoError(t, err)
	assert.Equal(t, "host='src' port='6543' dbname='app' sslmode='disable'", fake.conns["custom"].GetDsn())
}

func TestMigrationDDL_CreateMigration(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"onprem": {Id: 1, Name: "onprem", Dsn: "host=src dbname=app"}}}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "CREATE MIGRATION m CONNECTION onprem FOR ALL TABLES")
	require.NoError(t, err)
	require.NotNil(t, fake.create)
	assert.Equal(t, "m", fake.create.GetMigration().GetName())
	assert.Equal(t, "onprem", fake.create.GetMigration().GetConnectionName())
	assert.Equal(t, "appdb", fake.create.GetMigration().GetTarget().GetDatabase())
	assert.Equal(t, "0", fake.create.GetMigration().GetTarget().GetShard())
	assert.True(t, fake.create.GetMigration().GetObjects().GetAll())

	_, err = runSQL(t, backend, "CREATE MIGRATION m2 CONNECTION onprem FOR TABLE orders, customers WITH (copy_data = false, sequence_margin = 5)")
	require.NoError(t, err)
	assert.Equal(t, []string{"orders", "customers"}, fake.create.GetMigration().GetObjects().GetTable().GetQualifiedNames())
	assert.True(t, fake.create.GetOptions().GetSkipCopyData())
	assert.Equal(t, int64(5), fake.create.GetMigration().GetSequenceMargin())

	// schema selection
	_, err = runSQL(t, backend, "CREATE MIGRATION m3 CONNECTION onprem FOR TABLES IN SCHEMA public")
	require.NoError(t, err)
	assert.Equal(t, []string{"public"}, fake.create.GetMigration().GetObjects().GetSchema().GetSchemata())

	// unknown connection
	_, err = runSQL(t, backend, "CREATE MIGRATION bad CONNECTION nope FOR ALL TABLES")
	assert.ErrorContains(t, err, "nope")
}

// TestMigrationDDL_PhaseImportSetsCallerRole is the regression test for the
// HIGH finding this plumbing guards against: SetMigrationDirectionRequest
// must carry the gateway's SCRAM-authenticated identity for PHASE IMPORT, so
// the coordinator's CheckDropPrivilege (see target.go) can verify the caller
// owns the target tables it names before DropTables runs as the admin pool.
func TestMigrationDDL_PhaseImportSetsCallerRole(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"c": {Id: 1, Name: "c", Dsn: "host=x"}}}
	backend := newTestBackend(fake)

	stmts, err := parser.ParseSQL("ALTER MIGRATION m PHASE IMPORT")
	require.NoError(t, err)
	p := NewMigrationDDL("ALTER MIGRATION m PHASE IMPORT", stmts[0], backend)

	execErr := p.StreamExecute(context.Background(), nil, privilegedTestConnAs("app"), nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, execErr)
	require.NotNil(t, fake.direction)
	assert.Equal(t, "app", fake.direction.GetCallerRole())
}

func TestMigrationDDL_LifecycleAndDrop(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"c": {Id: 1, Name: "c", Dsn: "host=x"}}}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "ALTER MIGRATION m PHASE IMPORT")
	require.NoError(t, err)
	require.NotNil(t, fake.direction)
	assert.Equal(t, "m", fake.direction.GetRef().GetName())
	assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, fake.direction.GetDirection())

	_, err = runSQL(t, backend, "ALTER MIGRATION m PHASE EXPORT WHEN (lag_bytes < 8388608) WITH (wait_timeout = '30s')")
	require.NoError(t, err)
	assert.Equal(t, "m", fake.direction.GetRef().GetName())
	assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, fake.direction.GetDirection())
	assert.Equal(t, uint64(8388608), fake.direction.GetMaxLagBytes())
	assert.Equal(t, int64(30), fake.direction.GetWaitTimeoutSeconds())

	_, err = runSQL(t, backend, "ALTER MIGRATION m PHASE IMPORT")
	require.NoError(t, err)
	assert.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, fake.direction.GetDirection())

	// A not-yet-backed WHEN combination is rejected at the gateway, not sent.
	_, err = runSQL(t, backend, "ALTER MIGRATION m PHASE EXPORT WHEN (total_relations >= 10)")
	require.Error(t, err)
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))

	_, err = runSQL(t, backend, "DROP MIGRATION m FORCE")
	require.NoError(t, err)
	require.NotNil(t, fake.drop)
	assert.True(t, fake.drop.GetForce())

	_, err = runSQL(t, backend, "DROP MIGRATION m WHEN (lag_bytes = 0) WITH (wait_timeout = 30)")
	require.NoError(t, err)
	assert.True(t, fake.drop.GetWait())
	assert.Equal(t, int64(30), fake.drop.GetWaitTimeoutSeconds())

	// A not-yet-backed WHEN value (non-zero lag_bytes) is rejected at the
	// gateway, not sent.
	_, err = runSQL(t, backend, "DROP MIGRATION m WHEN (lag_bytes = 100)")
	require.Error(t, err)
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))
}

func TestMigrationDDL_StatMigrationSelect(t *testing.T) {
	fake := &fakeMigrator{migrations: []*migratorpb.GetMigrationResponse{
		{
			Migration: &migratorpb.MigrationRecord{
				Name: "orders_move", Id: 1, ConnectionName: "src",
				Target: &clustermetadatapb.ShardKey{Database: "appdb", Shard: "0", TableGroup: "default"},
			},
			Status: &migratorpb.MigrationStatus{
				Id:              1,
				Phase:           migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING,
				ActiveDirection: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT,
				CaughtUp:        true,
				LagBytes:        4096,
				LagSeconds:      1.5,
			},
		},
	}}
	backend := newTestBackend(fake)

	res, err := runSQL(t, backend, "SELECT * FROM multigres.stat_migration WHERE migration_name = 'orders_move'")
	require.NoError(t, err)
	assert.Equal(t, "orders_move", fake.get.GetRef().GetName())
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Fields, 11)
	assert.Equal(t, "migration_target", res.Fields[3].Name)
	assert.Equal(t, "lag_bytes", res.Fields[8].Name)
	assert.Equal(t, "lag_seconds", res.Fields[9].Name)
	vals := res.Rows[0].Values
	assert.Equal(t, "1", string(vals[0]), "migration_id")
	assert.Equal(t, "orders_move", string(vals[1]), "migration_name")
	assert.Equal(t, "src", string(vals[2]), "connection_name")
	assert.Equal(t, "(appdb,default,0)", string(vals[3]), "migration_target composite")
	assert.Equal(t, "IMPORTING", string(vals[4]), "migration_phase (proto enum prefix stripped)")
	assert.Equal(t, "IMPORT", string(vals[5]), "active_direction (proto enum prefix stripped)")
	assert.Equal(t, "4096", string(vals[8]))
	assert.Equal(t, "1.500", string(vals[9]))

	// A single-column projection with an id filter.
	res, err = runSQL(t, backend, "SELECT migration_name, migration_phase FROM multigres.stat_migration WHERE migration_id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), fake.get.GetRef().GetId())
	require.Len(t, res.Fields, 2)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, []string{"orders_move", "IMPORTING"}, []string{string(res.Rows[0].Values[0]), string(res.Rows[0].Values[1])})

	// Every migration, no filter.
	res, err = runSQL(t, backend, "SELECT migration_id FROM multigres.stat_migration")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "1", string(res.Rows[0].Values[0]))

	// Outside the recognized shape (a join, here simulated via an unsupported
	// WHERE) falls through to the "unsupported query" error rather than being
	// misanswered — the planner is what would actually route this elsewhere in
	// production; this unit test only exercises the primitive directly.
	_, err = runSQL(t, backend, "SELECT * FROM multigres.stat_migration WHERE migration_name = 'orders_move' AND migration_id = 1")
	assert.ErrorContains(t, err, "unsupported query against multigres.stat_migration")
}

// TestMigrationDDL_StringRedactsConnectionPassword is the regression test for
// the finding this guards against: the executor logs plan.String() on every
// plan (including at DEBUG level and on execution errors — see
// executor.go), and CREATE CONNECTION carries the source password as a
// SQL literal, so a naive String() would write reusable source credentials to
// gateway logs.
func TestMigrationDDL_StringRedactsConnectionPassword(t *testing.T) {
	t.Run("CREATE CONNECTION is redacted", func(t *testing.T) {
		stmts, err := parser.ParseSQL("CREATE CONNECTION src OPTIONS (host 'h', password 'supersecret')")
		require.NoError(t, err)
		p := NewMigrationDDL(stmts[0].SqlString(), stmts[0], nil)
		s := p.String()
		assert.NotContains(t, s, "supersecret")
		assert.Contains(t, s, "src", "the connection name is still useful to keep for debugging")
	})
	t.Run("other migration statements are logged in full", func(t *testing.T) {
		stmts, err := parser.ParseSQL("CREATE MIGRATION m CONNECTION src FOR ALL TABLES")
		require.NoError(t, err)
		p := NewMigrationDDL(stmts[0].SqlString(), stmts[0], nil)
		assert.Contains(t, p.String(), "CREATE MIGRATION")
	})
}

func TestMigrationDDL_UnconfiguredBackend(t *testing.T) {
	stmts, err := parser.ParseSQL("CREATE CONNECTION x OPTIONS (host 'h')")
	require.NoError(t, err)
	p := NewMigrationDDL("sql", stmts[0], nil)
	err = p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")
}
