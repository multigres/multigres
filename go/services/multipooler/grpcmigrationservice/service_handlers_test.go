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

package grpcmigrationservice

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// fakeCoordinatorProvider implements migrationCoordinatorProvider directly,
// letting a test supply either a real *migration.Coordinator (built against a
// mock query service — see newTestService) or a fixed error, without needing a
// real *manager.MultipoolerManager or a Postgres connection.
type fakeCoordinatorProvider struct {
	coord *migration.Coordinator
	err   error
}

func (f *fakeCoordinatorProvider) MigrationCoordinatorIfPrimary(context.Context) (*migration.Coordinator, error) {
	return f.coord, f.err
}

// newTestService builds a migrationService backed by a real *migration.Coordinator
// (the same orchestration code the real service uses), itself backed by a mock
// query service — so these tests exercise the real request/response mapping and
// error translation (toGRPC, migToProto, ...) without a real Postgres or
// multipooler manager.
func newTestService(t *testing.T) (*migrationService, *mock.QueryService) {
	t.Helper()
	qs := mock.NewQueryService()
	coord := migration.NewCoordinator(qs, slog.New(slog.DiscardHandler),
		func(string) (string, error) { return "host=target dbname=app", nil },
		func(context.Context) error { return nil },
		func(context.Context) error { return nil })
	return &migrationService{manager: &fakeCoordinatorProvider{coord: coord}}, qs
}

// migrationRow builds one row in selectMigrationSQL's column order (see
// migration.store_test.go's selectRow, which this mirrors — that helper is
// unexported in a different package, so this is a deliberate duplicate, not a
// shared one). Phase CREATED means liveStatus's SubscriptionStatus/lag probe
// never runs (see Coordinator.liveStatus), so no additional query needs to be
// mocked for GetMigration/ListMigrations to work against this row.
func migrationRow(id int64, name string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(strconv.FormatInt(id, 10)), // migration_id
		sqltypes.Value("CREATED"),                 // migration_phase
		sqltypes.Value(name),                      // migration_name
		sqltypes.Value("42"),                      // connection_id
		sqltypes.Value("d"),                       // (migration_target).database
		sqltypes.Value("0"),                       // (migration_target).shard
		sqltypes.Value("default"),                 // (migration_target).table_group
		sqltypes.Value("0"),                       // sequence_margin
		sqltypes.Value("true"),                    // copy_data
		sqltypes.Value("false"),                   // skip_schema_copy
		sqltypes.Value("IMPORT"),                  // direction
		sqltypes.Value(""),                        // last_error
		sqltypes.Value(""),                        // reverse_link_error
		sqltypes.Value("2026-01-01T00:00:00Z"),    // created_at
		nil,                                       // streaming_since
		sqltypes.Value(`["public.orders"]`),       // tables
	}}
}

// selectMigrationPattern matches Store.loadCacheLocked's one-shot full-table
// read (see selectMigrationSQL): GetMigration/ListMigrations/GetMigrationJournal
// all populate the same in-memory cache from this single query.
const selectMigrationPattern = `(?s)SELECT m\.migration_id.*FROM multigres\.migration m`

func TestGetMigration(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(selectMigrationPattern, &sqltypes.Result{Rows: []*sqltypes.Row{migrationRow(1, "nightly")}})

		resp, err := svc.GetMigration(context.Background(), &migratorpb.GetMigrationRequest{
			Ref: &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: 1}},
		})
		require.NoError(t, err)
		assert.Equal(t, "nightly", resp.GetMigration().GetName())
		assert.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_CREATED, resp.GetStatus().GetPhase())
	})

	t.Run("not found maps to NotFound", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(selectMigrationPattern, mock.MakeQueryResult(nil, nil))

		_, err := svc.GetMigration(context.Background(), &migratorpb.GetMigrationRequest{
			Ref: &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: 99}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestListMigrations(t *testing.T) {
	svc, qs := newTestService(t)
	qs.AddQueryPattern(selectMigrationPattern, &sqltypes.Result{Rows: []*sqltypes.Row{migrationRow(1, "a"), migrationRow(2, "b")}})

	resp, err := svc.ListMigrations(context.Background(), &migratorpb.ListMigrationsRequest{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2}, resp.GetIds())
}

func TestGetMigrationJournal(t *testing.T) {
	svc, qs := newTestService(t)
	qs.AddQueryPattern(selectMigrationPattern, &sqltypes.Result{Rows: []*sqltypes.Row{migrationRow(1, "nightly")}})
	qs.AddQueryPattern(`FROM multigres\.migration_journal`, mock.MakeQueryResult(
		[]string{"seq", "migration_id", "migration_name", "event", "phase", "direction", "from_lsn", "to_lsn", "last_error", "detail", "created_at"},
		[][]any{
			{int64(1), int64(1), "nightly", "CREATE", "CREATED", "IMPORT", "", "", "", "", "2026-01-01T00:00:00Z"},
		},
	))

	resp, err := svc.GetMigrationJournal(context.Background(), &migratorpb.GetMigrationJournalRequest{Id: 1})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, "nightly", resp.GetEntries()[0].GetMigrationName())
}

func TestCreateConnection(t *testing.T) {
	svc, qs := newTestService(t)
	qs.AddQueryPattern(`INSERT INTO multigres\.migration_connection`, mock.MakeQueryResult(nil, nil))

	resp, err := svc.CreateConnection(context.Background(), &migratorpb.CreateConnectionRequest{
		Connection: &migratorpb.Connection{Name: "src", Dsn: "host=h dbname=app"},
	})
	require.NoError(t, err)
	assert.Equal(t, "src", resp.GetConnection().GetName())
}

func TestGetConnection(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(`FROM multigres\.migration_connection WHERE connection_id`, mock.MakeQueryResult(
			[]string{"connection_id", "name", "dsn", "created_at"},
			[][]any{{int64(1), "src", "host=h dbname=app", "2026-01-01T00:00:00Z"}},
		))

		resp, err := svc.GetConnection(context.Background(), &migratorpb.GetConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: 1}},
		})
		require.NoError(t, err)
		assert.Equal(t, "src", resp.GetConnection().GetName())
	})

	t.Run("not found maps to NotFound", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(`FROM multigres\.migration_connection WHERE connection_id`, mock.MakeQueryResult(nil, nil))

		_, err := svc.GetConnection(context.Background(), &migratorpb.GetConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: 99}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestListConnections(t *testing.T) {
	svc, qs := newTestService(t)
	qs.AddQueryPattern(`SELECT connection_id, name, dsn, created_at\s*FROM multigres\.migration_connection$`, mock.MakeQueryResult(
		[]string{"connection_id", "name", "dsn", "created_at"},
		[][]any{
			{int64(1), "a", "host=h1", "2026-01-01T00:00:00Z"},
			{int64(2), "b", "host=h2", "2026-01-01T00:00:00Z"},
		},
	))

	resp, err := svc.ListConnections(context.Background(), &migratorpb.ListConnectionsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetConnections(), 2)
}

func TestDropConnection(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(`FROM multigres\.migration_connection WHERE connection_id`, mock.MakeQueryResult(
			[]string{"connection_id", "name", "dsn", "created_at"},
			[][]any{{int64(1), "src", "host=h", "2026-01-01T00:00:00Z"}},
		))
		qs.AddQueryPattern(`DELETE FROM multigres\.migration_connection`, mock.MakeQueryResult(nil, nil))

		_, err := svc.DropConnection(context.Background(), &migratorpb.DropConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: 1}},
		})
		require.NoError(t, err)
	})

	t.Run("missing without IfExists maps to NotFound", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(`FROM multigres\.migration_connection WHERE connection_id`, mock.MakeQueryResult(nil, nil))

		_, err := svc.DropConnection(context.Background(), &migratorpb.DropConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: 99}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("missing with IfExists is a no-op success", func(t *testing.T) {
		svc, qs := newTestService(t)
		qs.AddQueryPattern(`FROM multigres\.migration_connection WHERE connection_id`, mock.MakeQueryResult(nil, nil))

		_, err := svc.DropConnection(context.Background(), &migratorpb.DropConnectionRequest{
			Ref:      &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: 99}},
			IfExists: true,
		})
		require.NoError(t, err)
	})
}

// TestMigrationService_PrimaryGateError covers the one thing every handler in
// this file does identically: when the pooler is not (yet) the shard primary,
// MigrationCoordinatorIfPrimary fails and the handler must return that error
// (through toGRPC) without touching anything else. Table-driven so adding a
// eleventh handler later without covering this path here fails loudly (the new
// case has to be added for the table to reflect it).
func TestMigrationService_PrimaryGateError(t *testing.T) {
	wantErr := errors.New("not primary")
	svc := &migrationService{manager: &fakeCoordinatorProvider{err: wantErr}}
	ctx := context.Background()

	cases := map[string]func() error{
		"CreateMigration": func() error {
			_, err := svc.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{})
			return err
		},
		"GetMigration": func() error {
			_, err := svc.GetMigration(ctx, &migratorpb.GetMigrationRequest{})
			return err
		},
		"ListMigrations": func() error {
			_, err := svc.ListMigrations(ctx, &migratorpb.ListMigrationsRequest{})
			return err
		},
		"GetMigrationJournal": func() error {
			_, err := svc.GetMigrationJournal(ctx, &migratorpb.GetMigrationJournalRequest{})
			return err
		},
		"DropMigration": func() error {
			_, err := svc.DropMigration(ctx, &migratorpb.DropMigrationRequest{})
			return err
		},
		"SetMigrationDirection": func() error {
			_, err := svc.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{})
			return err
		},
		"CreateConnection": func() error {
			_, err := svc.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{})
			return err
		},
		"GetConnection": func() error {
			_, err := svc.GetConnection(ctx, &migratorpb.GetConnectionRequest{})
			return err
		},
		"ListConnections": func() error {
			_, err := svc.ListConnections(ctx, &migratorpb.ListConnectionsRequest{})
			return err
		},
		"DropConnection": func() error {
			_, err := svc.DropConnection(ctx, &migratorpb.DropConnectionRequest{})
			return err
		},
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not primary")
		})
	}
}
