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

package planner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func requireFeatureNotSupported(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "want a PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	assert.Contains(t, diag.Message, contains)
}

// ALTER TABLEGROUP parses but is not implemented: the gateway rejects it with
// feature_not_supported instead of forwarding it to postgres, where it would
// fail with a syntax error. CREATE and DROP pass analysis and are planned as
// gateway primitives.
func TestAnalyzeStatementTableGroupDDL(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLEGROUP commerce RENAME TO shop",
		"ALTER TABLEGROUP commerce RESHARD ('0-inf') INTO ('0-80', '80-inf')",
	} {
		for _, unsafeConn := range []bool{false, true} {
			t.Run(sql, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), unsafeConn, false)
				requireFeatureNotSupported(t, err, "ALTER TABLEGROUP is not supported")
			})
		}
	}
	for _, sql := range []string{
		"CREATE TABLEGROUP commerce",
		"CREATE TABLEGROUP IF NOT EXISTS commerce SHARDS ('0-80', '80-inf')",
		"CREATE TABLEGROUP srctg CONNECTION ext",
		"DROP TABLEGROUP commerce",
		"DROP TABLEGROUP IF EXISTS commerce",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false, false)
			require.NoError(t, err)
		})
	}

	// Function and DO bodies are analyzed on a separate path; tablegroup DDL
	// nested there is rejected for good, before postgres sees the body.
	for _, sql := range []string{
		"CREATE FUNCTION f() RETURNS void LANGUAGE sql AS 'ALTER TABLEGROUP g RESHARD (''0-inf'') INTO (''0-80'', ''80-inf'')'",
		"CREATE FUNCTION f() RETURNS void LANGUAGE sql BEGIN ATOMIC SELECT 1; DROP TABLEGROUP g; END",
		"CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN ALTER TABLEGROUP g RENAME TO h; END $$",
		"DO $$ BEGIN CREATE TABLEGROUP g; END $$",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false, false)
			requireFeatureNotSupported(t, err, "inside a function body")
		})
	}

	// The new keywords stay ordinary identifiers in plain postgres DDL.
	for _, sql := range []string{
		"CREATE TABLE t (tablegroup text, shards int, reshard int)",
		"ALTER TABLE t DROP shards",
		"DROP TABLE tablegroup",
		"CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT shards FROM tablegroup; END",
		"DO $$ BEGIN PERFORM reshard FROM tablegroup; END $$",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false, false)
			require.NoError(t, err)
		})
	}
}

// nopTableGroupAdmin satisfies engine.TableGroupAdmin for planning tests,
// which never execute the plan.
type nopTableGroupAdmin struct{}

func (nopTableGroupAdmin) CreateTableGroup(context.Context, string, *multipoolermanagerdatapb.CreateTableGroupRequest) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	return nil, nil
}

func (nopTableGroupAdmin) DropTableGroup(context.Context, string, *multipoolermanagerdatapb.DropTableGroupRequest) (*multipoolermanagerdatapb.DropTableGroupResponse, error) {
	return nil, nil
}

func (nopTableGroupAdmin) GetOperation(context.Context, string, string) (*multipoolermanagerdatapb.Operation, error) {
	return nil, nil
}

func planTableGroupSQL(t *testing.T, admin engine.TableGroupAdmin, sql string) (*engine.Plan, error) {
	t.Helper()
	p := NewPlanner("default", slog.New(slog.DiscardHandler), nil)
	if admin != nil {
		p.SetTableGroupAdmin(admin)
	}
	return p.Plan(sql, parseOne(t, sql), server.NewTestConn(&bytes.Buffer{}).Conn, PlanOptions{})
}

func TestPlanTableGroupDDL(t *testing.T) {
	for sql, want := range map[string]string{
		"CREATE TABLEGROUP commerce":                                         "TableGroupDDL(CREATE TABLEGROUP commerce shards=[] if_not_exists=false)",
		"CREATE TABLEGROUP IF NOT EXISTS commerce SHARDS ('80-inf', '0-80')": "TableGroupDDL(CREATE TABLEGROUP commerce shards=[80-inf 0-80] if_not_exists=true)",
		"CREATE TABLEGROUP srctg CONNECTION ext":                             "TableGroupDDL(CREATE TABLEGROUP srctg connection=ext if_not_exists=false)",
		"CREATE TABLEGROUP IF NOT EXISTS srctg CONNECTION ext":               "TableGroupDDL(CREATE TABLEGROUP srctg connection=ext if_not_exists=true)",
		"DROP TABLEGROUP commerce":                                           "TableGroupDDL(DROP TABLEGROUP commerce if_exists=false)",
		"DROP TABLEGROUP IF EXISTS commerce RESTRICT":                        "TableGroupDDL(DROP TABLEGROUP commerce if_exists=true)",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := planTableGroupSQL(t, nopTableGroupAdmin{}, sql)
			require.NoError(t, err)
			require.IsType(t, &engine.TableGroupDDL{}, plan.Primitive)
			assert.Equal(t, want, plan.Primitive.String())
			assert.Equal(t, engine.PlanTypeTableGroupDDL, plan.Type)
		})
	}

	t.Run("disabled without an admin", func(t *testing.T) {
		for _, sql := range []string{"CREATE TABLEGROUP commerce", "DROP TABLEGROUP IF EXISTS commerce"} {
			_, err := planTableGroupSQL(t, nil, sql)
			requireFeatureNotSupported(t, err, "tablegroup DDL is not enabled on this cluster")
		}
	})
	t.Run("CASCADE is refused", func(t *testing.T) {
		_, err := planTableGroupSQL(t, nopTableGroupAdmin{}, "DROP TABLEGROUP commerce CASCADE")
		requireFeatureNotSupported(t, err, "CASCADE is not supported")
	})
	t.Run("other DROP statements still route to postgres", func(t *testing.T) {
		plan, err := planTableGroupSQL(t, nopTableGroupAdmin{}, "DROP TABLE tablegroup")
		require.NoError(t, err)
		assert.IsType(t, &engine.Route{}, plan.Primitive)
	})
}
