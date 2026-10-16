// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestPlan_StatMigrationSelectRouting covers the planner's T_SelectStmt gate:
// a recognized pseudo-view query against multigres.stat_migration must route
// to the migration DDL primitive (answered over RPC, bypassing the serving
// gate), while anything outside that shape — or a gateway with no migration
// backend configured at all — must route as an ordinary query instead. A
// wrongly-intercepted ordinary query would silently misbehave, so this is the
// one safety property most worth a dedicated test (see
// recognizeStatMigrationSelect in the engine package for the exhaustive shape
// coverage).
func TestPlan_StatMigrationSelectRouting(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	t.Run("recognized shape routes to the migration primitive", func(t *testing.T) {
		p := NewPlanner("default", logger, nil)
		p.SetMigrationBackend(&engine.MigrationBackend{
			Client: func() migratorpb.MigratorClient { return nil },
		})
		sql := "SELECT migration_name FROM multigres.stat_migration WHERE migration_id = 1"
		plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
		require.NoError(t, err)
		_, isMigrationDDL := plan.Primitive.(*engine.MigrationDDL)
		require.True(t, isMigrationDDL, "expected the migration DDL primitive, got %T", plan.Primitive)
	})

	t.Run("a join against stat_migration falls through to ordinary routing", func(t *testing.T) {
		p := NewPlanner("default", logger, nil)
		p.SetMigrationBackend(&engine.MigrationBackend{
			Client: func() migratorpb.MigratorClient { return nil },
		})
		sql := "SELECT a.migration_name FROM multigres.stat_migration a JOIN other b ON a.migration_id = b.id"
		plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
		require.NoError(t, err)
		_, isRoute := plan.Primitive.(*engine.Route)
		require.True(t, isRoute, "expected a plain Route, got %T", plan.Primitive)
	})

	t.Run("an ordinary select is unaffected", func(t *testing.T) {
		p := NewPlanner("default", logger, nil)
		p.SetMigrationBackend(&engine.MigrationBackend{
			Client: func() migratorpb.MigratorClient { return nil },
		})
		sql := "SELECT 1"
		plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
		require.NoError(t, err)
		_, isRoute := plan.Primitive.(*engine.Route)
		require.True(t, isRoute, "expected a plain Route, got %T", plan.Primitive)
	})

	t.Run("no migration backend configured: even a recognized shape is not intercepted", func(t *testing.T) {
		p := NewPlanner("default", logger, nil)
		sql := "SELECT migration_name FROM multigres.stat_migration"
		plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
		require.NoError(t, err)
		_, isRoute := plan.Primitive.(*engine.Route)
		require.True(t, isRoute, "a gateway with no migrator configured must route this as an ordinary query, got %T", plan.Primitive)
	})
}
