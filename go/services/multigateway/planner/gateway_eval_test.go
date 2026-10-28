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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestIsTargetListOnlySelect(t *testing.T) {
	yes := []string{
		"SELECT 1",
		"SELECT 1, 'a' AS b, now()",
		"SELECT $1 + $2",
		"select 1 /* comment */",
		"SELECT ARRAY[1]",
		"SELECT ALL 1", // ALL is the default and leaves no clause behind
	}
	no := []string{
		"SELECT",
		"SELECT 1 FROM t",
		"SELECT 1 WHERE true",
		"SELECT 1 GROUP BY 1",
		"SELECT 1 HAVING true",
		"SELECT 1 WINDOW w AS ()",
		"SELECT 1 ORDER BY 1",
		"SELECT 1 LIMIT 1",
		"SELECT 1 OFFSET 0",
		"SELECT 1 FOR UPDATE",
		"SELECT DISTINCT 1",
		"SELECT 1 INTO t",
		"SELECT 1 UNION SELECT 2",
		"WITH c AS (SELECT 1) SELECT 1",
		"VALUES (1)",
		"INSERT INTO t VALUES (1)",
		"SET x = 1",
	}
	for _, sql := range yes {
		assert.True(t, IsTargetListOnlySelect(parseOne(t, sql)), sql)
	}
	for _, sql := range no {
		assert.False(t, IsTargetListOnlySelect(parseOne(t, sql)), sql)
	}
}

func newEvalPlanner() *Planner {
	return NewPlanner("default", slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)), nil)
}

// TestPlan_GatewayEvalAdmission pins what the gateway answers itself and
// what it still sends to a backend, so a regression can never turn a routed
// statement into a local answer by accident, and vice versa.
func TestPlan_GatewayEvalAdmission(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		local  bool
		reason string
	}{
		{name: "constant", sql: "SELECT 1", local: true},
		{name: "arithmetic and aliases", sql: "SELECT 1 + 2 AS three, abs(-3), true AND false", local: true},
		{name: "cast via io", sql: "SELECT 1::int8::int2, CAST(2 AS bigint)", local: true},
		{name: "conditionals", sql: "SELECT CASE WHEN 1 > 2 THEN 1 ELSE 2 END, COALESCE(NULL::int4, 1), NULLIF(1, 2)", local: true},
		{name: "null tests", sql: "SELECT 1 IS NULL, true IS NOT TRUE, 1 IS DISTINCT FROM 2", local: true},
		{name: "typed null", sql: "SELECT NULL::int4", local: true},
		{name: "evaluation error stays local", sql: "SELECT 1 / 0", local: true},
		{name: "overflow stays local", sql: "SELECT 2147483647 + 1", local: true},
		{name: "quoted and folded aliases", sql: `SELECT 1 AS "Mixed Case", 2 AS Folded, 3 AS "?column?"`, local: true},
		{name: "widest allowed target list", sql: "SELECT " + strings.TrimSuffix(strings.Repeat("1,", maxTupleAttributeNumber), ","), local: true},

		{name: "with FROM", sql: "SELECT 1 FROM t", reason: "needs a relation"},
		{name: "column reference", sql: "SELECT x", reason: "needs a relation"},
		{name: "star without FROM", sql: "SELECT *", reason: "the backend reports 42601"},
		{name: "qualified star without FROM", sql: "SELECT t.*", reason: "needs a relation"},
		{name: "unknown-type literal", sql: "SELECT 'x'", reason: "textin is not ported"},
		{name: "bare NULL", sql: "SELECT NULL", reason: "resolves to text"},
		{name: "stable function", sql: "SELECT now()", reason: "not immutable"},
		{name: "volatile function", sql: "SELECT random()", reason: "not immutable"},
		{name: "backend identity", sql: "SELECT pg_backend_pid()", reason: "not immutable: the readiness probes rely on this"},
		{name: "unported immutable function", sql: "SELECT pi()", reason: "not ported"},
		{name: "analysis error", sql: "SELECT 1 + true", reason: "the backend reports errors"},
		{name: "unknown function", sql: "SELECT nosuch(1)", reason: "could be a user function"},
		{name: "array", sql: "SELECT ARRAY[1, 2]", reason: "arrays are not evaluable yet"},
		{name: "scalar array op", sql: "SELECT 1 = ANY (ARRAY[1, 2])", reason: "arrays are not evaluable yet"},
		{name: "greatest", sql: "SELECT GREATEST(1, 2)", reason: "not evaluable yet"},
		{name: "parameter in simple protocol", sql: "SELECT $1", reason: "there is no parameter"},
		{name: "advisory lock", sql: "SELECT pg_advisory_lock(1)", reason: "pins a backend"},
		{name: "set_config", sql: "SELECT set_config('a.b', 'c', false)", reason: "tracked session state"},
		{name: "current_setting", sql: "SELECT current_setting('statement_timeout')", reason: "gateway-managed read"},
		{name: "field indirection", sql: "SELECT (1).f", reason: "indirection"},
		{name: "empty target list", sql: "SELECT", reason: "no columns"},
		{name: "invalid literal for the type", sql: "SELECT 'abc'::int4", reason: "the backend reports 22P02"},
		{name: "too wide a target list", sql: "SELECT " + strings.TrimSuffix(strings.Repeat("1,", maxTupleAttributeNumber+1), ","), reason: "the backend reports 54011"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newEvalPlanner()
			conn := server.NewTestConn(&bytes.Buffer{}).Conn
			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn, PlanOptions{})
			require.NoError(t, err)
			if tt.local {
				require.IsType(t, &engine.GatewayEval{}, plan.Primitive, "expected a local answer")
				assert.Equal(t, engine.PlanTypeGatewayEval, plan.Type)
				assert.Empty(t, plan.TablesUsed)
				if tt.name == "quoted and folded aliases" {
					desc, err := plan.Primitive.(*engine.GatewayEval).Description(nil)
					require.NoError(t, err)
					assert.Equal(t, []string{"Mixed Case", "folded", "?column?"},
						[]string{desc.Fields[0].Name, desc.Fields[1].Name, desc.Fields[2].Name})
				}
				return
			}
			_, local := plan.Primitive.(*engine.GatewayEval)
			assert.False(t, local, "expected routing: %s", tt.reason)
			assert.NotEqual(t, engine.PlanTypeGatewayEval, plan.Type)
		})
	}
}

// Session state the plan must respect: a transaction block (snapshot and
// aborted-transaction semantics belong to the backend) and an unsafe
// connection (which asked for its pinned backend) both route.
func TestPlan_GatewayEvalSessionState(t *testing.T) {
	p := newEvalPlanner()

	inTxn := server.NewTestConn(&bytes.Buffer{})
	inTxn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	plan, err := p.Plan("SELECT 1", parseOne(t, "SELECT 1"), inTxn.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.IsType(t, &engine.Route{}, plan.Primitive, "inside a transaction block")

	unsafe := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection())
	plan, err = p.Plan("SELECT 1", parseOne(t, "SELECT 1"), unsafe.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.IsType(t, &engine.Route{}, plan.Primitive, "unsafe connection")

	reserved := server.NewTestConn(&bytes.Buffer{})
	st := handler.NewMultigatewayConnectionState()
	st.SetReservedConnection(protoutil.NewTarget("", "default", constants.DefaultShard, query.Mode_MODE_UNSPECIFIED),
		&query.ReservedState{ReservedConnectionId: 7})
	reserved.Conn.SetConnectionState(st)
	plan, err = p.Plan("SELECT 1", parseOne(t, "SELECT 1"), reserved.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.IsType(t, &engine.Route{}, plan.Primitive, "reserved backend: its loss must surface on the next statement")

	st.ClearAllReservedConnections()
	plan, err = p.Plan("SELECT 1", parseOne(t, "SELECT 1"), reserved.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.IsType(t, &engine.GatewayEval{}, plan.Primitive, "released")
}

// Extended protocol: parameters are typed from the client's Parse
// declarations, deduced when declared 0, exactly as the backend would.
func TestPlan_GatewayEvalParameters(t *testing.T) {
	int4, int8, text := uint32(pgoid.INT4OID), uint32(pgoid.INT8OID), uint32(pgoid.TEXTOID)
	tests := []struct {
		name       string
		sql        string
		declared   []uint32
		wantParams []pgoid.Oid // nil: routed
	}{
		{name: "declared", sql: "SELECT $1 + 1", declared: []uint32{int8}, wantParams: []pgoid.Oid{pgoid.INT8OID}},
		{name: "deduced", sql: "SELECT $1 + 1", declared: []uint32{0}, wantParams: []pgoid.Oid{pgoid.INT4OID}},
		{name: "undeclared", sql: "SELECT $1 + 1", declared: nil, wantParams: []pgoid.Oid{pgoid.INT4OID}},
		{name: "deduced through a cast", sql: "SELECT $1::int4 + $2", declared: []uint32{0, 0}, wantParams: []pgoid.Oid{pgoid.INT4OID, pgoid.INT4OID}},
		{name: "unreferenced declared parameter", sql: "SELECT 1", declared: []uint32{int4}, wantParams: []pgoid.Oid{pgoid.INT4OID}},
		{name: "undetermined type", sql: "SELECT $1", declared: []uint32{0}},
		{name: "undetermined unreferenced", sql: "SELECT 1", declared: []uint32{0}},
		{name: "unported parameter type", sql: "SELECT $1 IS NULL", declared: []uint32{text}},
		{name: "unported deduced type", sql: "SELECT $1::text IS NULL", declared: []uint32{0}},
		{name: "inconsistent deduction", sql: "SELECT $1 + 1, $1 AND true", declared: []uint32{0}},
		{name: "declared type the catalog lacks", sql: "SELECT $1 IS NULL", declared: []uint32{99999}},
		{name: "unreferenced type the catalog lacks", sql: "SELECT 1", declared: []uint32{99999}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newEvalPlanner()
			conn := server.NewTestConn(&bytes.Buffer{}).Conn
			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn, PlanOptions{IsPortal: true, ParamTypes: tt.declared})
			require.NoError(t, err)
			if tt.wantParams == nil {
				assert.IsType(t, &engine.Route{}, plan.Primitive)
				return
			}
			ge, ok := plan.Primitive.(*engine.GatewayEval)
			require.True(t, ok, "expected a local answer, got %T", plan.Primitive)
			desc, err := ge.Description(nil)
			require.NoError(t, err)
			got := make([]pgoid.Oid, len(desc.Parameters))
			for i, p := range desc.Parameters {
				got[i] = pgoid.Oid(p.DataTypeOid)
			}
			assert.Equal(t, tt.wantParams, got)
		})
	}
}

// GatewayEvalFor is what Describe asks; it must agree with Plan on every
// statement so the client's RowDescription matches what Execute sends.
func TestGatewayEvalFor(t *testing.T) {
	p := newEvalPlanner()
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	ge := p.GatewayEvalFor("SELECT $1::int4 + 1 AS n", parseOne(t, "SELECT $1::int4 + 1 AS n"), conn, []uint32{0})
	require.NotNil(t, ge)
	desc, err := ge.Description(nil)
	require.NoError(t, err)
	require.Len(t, desc.Fields, 1)
	assert.Equal(t, "n", desc.Fields[0].Name)
	assert.Equal(t, uint32(pgoid.INT4OID), desc.Fields[0].DataTypeOid)

	for _, sql := range []string{
		"SELECT now()",
		"SELECT set_config('a.b', 'c', false)",
		"SELECT pg_read_file('/etc/passwd')", // rejected by analysis: left to Execute
		"SELECT 1 FROM t",
		"SET x = 1",
	} {
		assert.Nil(t, p.GatewayEvalFor(sql, parseOne(t, sql), conn, nil), sql)
	}

	inTxn := server.NewTestConn(&bytes.Buffer{})
	inTxn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	assert.Nil(t, p.GatewayEvalFor("SELECT 1", parseOne(t, "SELECT 1"), inTxn.Conn, nil), "inside a transaction block")
}

// The primitive reports its own plan type for spans and query logs.
func TestPrimitiveName_GatewayEval(t *testing.T) {
	assert.Equal(t, engine.PlanTypeGatewayEval, primitiveName(engine.NewGatewayEval("SELECT 1", nil, nil)))
}

// A CoerceViaIO cast runs two I/O functions that never appear as calls in
// the tree; int/bool I/O is immutable, so the cast is admitted. (No such
// cast exists between the ported types, so the node is built by hand.)
func TestImmutable_CoerceViaIO(t *testing.T) {
	arg := ast.NewConst(pgoid.INT4OID, datum.Int32GetDatum(1), false)
	assert.True(t, immutable(ast.NewCoerceViaIO(arg, pgoid.INT8OID, ast.COERCE_EXPLICIT_CAST)))
	assert.True(t, immutable(arg))
}
