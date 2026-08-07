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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
)

// TestTempObjectCreationReserves asserts that every statement creating a
// session-scoped temp object plans as TempTableRoute (which reserves a
// backend connection), and that the non-temp variants stay plain Routes.
//
// Regression coverage for two bugs found via pg_regress (rangefuncs,
// groupingsets, limit, sequence):
//   - the grammar built ViewStmt literals without BaseNode.Tag, so
//     NodeTag() returned T_Invalid and the planner's T_ViewStmt case never
//     fired — temp views landed on unreserved pool conns and vanished on
//     connection recycle;
//   - CREATE TEMP SEQUENCE neither carried RelPersistence from the grammar
//     nor had a planner case.
func TestTempObjectCreationReserves(t *testing.T) {
	s := newTestSetup(t)

	tests := []struct {
		sql      string
		wantTemp bool
	}{
		{"CREATE TEMP TABLE tt (i int)", true},
		{"CREATE TEMP TABLE tt AS SELECT 1", true},
		{"SELECT 1 INTO TEMP tt", true},
		{"CREATE TEMP VIEW vv AS SELECT 1 AS n", true},
		{"CREATE OR REPLACE TEMP VIEW vv AS SELECT 1 AS n", true},
		{"CREATE TEMP RECURSIVE VIEW rv(n) AS SELECT 1", true},
		{"CREATE TEMP VIEW gs(a,b) AS VALUES (1,2),(3,4)", true},
		{"CREATE TEMP SEQUENCE sq", true},
		{"CREATE TEMPORARY SEQUENCE IF NOT EXISTS sq2", true},
		// The TEMP keyword with an explicit pg_temp qualification is the one
		// supported spelling of schema-qualified temp creation.
		{"CREATE TEMP TABLE pg_temp.qt (i int)", true},
		// current_schema() is an ordinary read: with pg_temp barred from
		// search_path (see checkRestrictedGUCChange) it can never instantiate
		// a temp namespace, so it must not cost a reserved connection.
		{"SELECT current_schema()", false},
		{"CREATE TABLE pt (i int)", false},
		{"CREATE VIEW pv AS SELECT 1", false},
		{"CREATE OR REPLACE VIEW pv AS SELECT 1", false},
		{"CREATE SEQUENCE ps", false},
		{"CREATE SEQUENCE IF NOT EXISTS ps2", false},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			asts, err := parser.ParseSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, asts, 1)
			plan, err := s.p.Plan(tc.sql, asts[0], s.conn.Conn, PlanOptions{})
			require.NoError(t, err)
			isTemp := plan.ExecInfo.TempTable
			require.Equal(t, tc.wantTemp, isTemp,
				"plan primitive = %s", plan.Primitive.String())
		})
	}

	// The extended query protocol must reserve for the same statements: a
	// temp object created via Parse/Bind/Execute on a pooled backend would
	// vanish on connection recycle exactly like the simple-protocol case.
	// The portal path sets ExecInfo.TempTable for temp creations and uses a plain
	// Route (which reissues the portal) for the non-temp variants.
	for _, tc := range tests {
		t.Run("portal/"+tc.sql, func(t *testing.T) {
			plan, err := planPortal(t, s.p, s.conn.Conn, tc.sql)
			require.NoError(t, err)
			if !tc.wantTemp {
				isTemp := plan.ExecInfo.TempTable
				require.False(t, isTemp, "plan primitive = %s", plan.Primitive.String())
				return
			}
			require.NotNil(t, plan, "temp creation must plan locally, not plain portal execute")
			isTemp := plan.ExecInfo.TempTable
			require.True(t, isTemp, "plan primitive = %s", plan.Primitive.String())
		})
	}
}

// TestTempSchemaQualifiedCreateRejected asserts that creating an object in the
// temp namespace via schema qualification (without the TEMP keyword) is
// rejected at plan time: PostgreSQL would make it a genuine temp object during
// parse analysis, invisible to the planner's keyword-based temp detection, so
// it would land untracked on an arbitrary pooled backend.
func TestTempSchemaQualifiedCreateRejected(t *testing.T) {
	s := newTestSetup(t)

	tests := []struct {
		sql     string
		wantErr bool
	}{
		{"CREATE TABLE pg_temp.t (i int)", true},
		{"CREATE TABLE pg_temp_3.t (i int)", true},
		{"CREATE TABLE pg_temp.t AS SELECT 1", true},
		{"SELECT 1 INTO pg_temp.t", true},
		{"CREATE VIEW pg_temp.v AS SELECT 1", true},
		{"CREATE SEQUENCE pg_temp.s", true},
		{"EXPLAIN CREATE TABLE pg_temp.t AS SELECT 1", true},
		{"CREATE FUNCTION pg_temp.f() RETURNS int AS 'SELECT 1' LANGUAGE sql", true},
		{"CREATE DOMAIN pg_temp.d AS text CHECK (value <> '')", true},
		{"CREATE TYPE pg_temp.ct AS (a int)", true},
		{"CREATE TYPE pg_temp.e AS ENUM ('x')", true},
		{"CREATE TYPE pg_temp.r AS RANGE (SUBTYPE = int4)", true},
		{"CREATE TABLE public.t (i int)", false},
		{"CREATE FUNCTION public.f() RETURNS int AS 'SELECT 1' LANGUAGE sql", false},
		{"CREATE DOMAIN d AS text", false},
		{"CREATE TYPE ct AS (a int)", false},
		{"CREATE TABLE mypg_temp.t (i int)", false},
		{"SELECT * FROM pg_temp.t", false},
		{"DROP TABLE pg_temp.t", false},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			asts, err := parser.ParseSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, asts, 1)
			_, err = s.p.Plan(tc.sql, asts[0], s.conn.Conn, PlanOptions{})
			if tc.wantErr {
				require.ErrorContains(t, err, "pg_temp")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
