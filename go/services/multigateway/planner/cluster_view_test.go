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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func TestPlanClusterViews(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	t.Run("a SELECT on a view plans a ClusterViewRoute", func(t *testing.T) {
		for _, sql := range []string{
			"SELECT * FROM multigres.multipoolers",
			"SELECT count(*) FROM multigres.multigateways g WHERE g.is_self",
			"EXPLAIN SELECT p.name FROM multigres.multipoolers p JOIN multigres.multigateways g ON g.cell = p.cell",
		} {
			plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.NoError(t, err, sql)
			assert.IsType(t, &engine.ClusterViewRoute{}, plan.Primitive, sql)
			assert.Equal(t, engine.PlanTypeClusterViewRoute, plan.Type, sql)
		}
	})

	t.Run("unqualified or foreign names route to the backend unchanged", func(t *testing.T) {
		for _, sql := range []string{
			"SELECT * FROM multipoolers",
			"SELECT * FROM multigres.backend_vpid",
		} {
			plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.NoError(t, err, sql)
			assert.IsType(t, &engine.Route{}, plan.Primitive, sql)
		}
	})

	t.Run("writes to a view are rejected", func(t *testing.T) {
		for _, sql := range []string{
			"INSERT INTO multigres.multipoolers (name) VALUES ('x')",
			"UPDATE multigres.multigateways SET name = 'x'",
			"DELETE FROM multigres.multipoolers",
			"TRUNCATE multigres.multipoolers",
		} {
			_, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.Error(t, err, sql)
			assert.Contains(t, err.Error(), "read-only view", sql)
		}
	})

	t.Run("shapes the fold cannot serve are rejected, not forwarded unfolded", func(t *testing.T) {
		for sql, want := range map[string]string{
			"CREATE VIEW v AS SELECT * FROM multigres.multipoolers":               "view cannot be defined",
			"CREATE MATERIALIZED VIEW mv AS SELECT * FROM multigres.multipoolers": "materialized view cannot be defined",
			"CREATE TEMP VIEW tv AS SELECT * FROM multigres.multipoolers":         "view cannot be defined",
			"DECLARE c CURSOR WITH HOLD FOR SELECT * FROM multigres.multipoolers": "cannot be combined",
			"CREATE TEMP TABLE snap AS SELECT * FROM multigres.multipoolers":      "cannot be combined",
			"SELECT * INTO TEMP snap FROM multigres.multipoolers":                 "cannot be combined",
			"COPY (SELECT name FROM multigres.multipoolers) TO '/tmp/out.csv'":    "cannot be combined",
			"COPY (SELECT name FROM multigres.multipoolers) TO STDOUT":            "cannot be combined",
		} {
			_, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.Error(t, err, sql)
			assert.Contains(t, err.Error(), want, sql)
		}
	})

	t.Run("reads that materialize a snapshot or read through a cursor are allowed", func(t *testing.T) {
		for _, sql := range []string{
			"CREATE TABLE snap AS SELECT * FROM multigres.multipoolers",
			"DECLARE c CURSOR FOR SELECT * FROM multigres.multipoolers",
			"UPDATE t SET x = 1 FROM multigres.multipoolers p WHERE p.name = t.n",
			"MERGE INTO t USING multigres.multipoolers p ON t.n = p.name WHEN MATCHED THEN DELETE",
		} {
			plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.NoError(t, err, sql)
			assert.IsType(t, &engine.ClusterViewRoute{}, plan.Primitive, sql)
		}
	})
}
