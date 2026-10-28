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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	"github.com/multigres/multigres/go/common/pgeval/exec"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// gatewayEvalFor builds the primitive the planner would for sql, with the
// parameter types a Parse declared (0 = deduce). The planner's admission is
// tested in its own package; here the input is known to be evaluable.
func gatewayEvalFor(t *testing.T, sql string, paramTypes []uint32) *GatewayEval {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	ss := stmts[0].(*ast.SelectStmt)
	ps := analyze.NewParseState()
	declared := make([]pgoid.Oid, len(paramTypes))
	for i, p := range paramTypes {
		declared[i] = pgoid.Oid(p)
	}
	ps.SetVariableParameters(declared)
	var columns []EvalColumn
	for _, item := range ss.TargetList.Items {
		rt := item.(*ast.ResTarget)
		name := rt.Name
		if name == "" {
			name = analyze.FigureColname(rt.Val)
		}
		tree, err := ps.TransformExpr(rt.Val)
		require.NoError(t, err)
		state, err := exec.Prepare(tree)
		require.NoError(t, err)
		columns = append(columns, EvalColumn{Name: name, Type: analyze.ExprType(tree), Typmod: analyze.ExprTypmod(tree), State: state})
	}
	return NewGatewayEval(sql, columns, ps.ParamTypes())
}

func evalPortal(t *testing.T, sql string, paramTypes []uint32, params [][]byte, paramFormats, resultFormats []int16) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("stmt", sql, paramTypes))
	require.NoError(t, err)
	return preparedstatement.NewPortalInfo(psi, protoutil.NewPortal("", "stmt", params, paramFormats, resultFormats))
}

func runPortal(t *testing.T, g *GatewayEval, pi *preparedstatement.PortalInfo, includeDescribe bool) (*sqltypes.Result, error) {
	t.Helper()
	var results []*sqltypes.Result
	err := g.PortalStreamExecute(context.Background(), nil, nil, nil, pi, 0, includeDescribe, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error {
			results = append(results, r)
			return nil
		})
	if err != nil {
		require.Empty(t, results, "an error must not follow a result")
		return nil, err
	}
	require.Len(t, results, 1)
	return results[0], nil
}

func values(row *sqltypes.Row) [][]byte {
	out := make([][]byte, len(row.Values))
	for i, v := range row.Values {
		out[i] = []byte(v)
	}
	return out
}

func sqlstate(t *testing.T, err error, code, message string) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, mterrors.ExtractSQLSTATE(err))
	assert.Contains(t, err.Error(), message)
}

func TestGatewayEval_StreamExecute(t *testing.T) {
	g := gatewayEvalFor(t, "SELECT 1 + 2 AS three, true, NULL::int4, -2147483648::int8 * 2", nil)

	results := collectResults(t, func(cb func(context.Context, *sqltypes.Result) error) error {
		return g.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{}, cb)
	})
	require.Len(t, results, 1)
	res := results[0]

	assert.Equal(t, "SELECT 1", res.CommandTag)
	require.Len(t, res.Fields, 4)
	for i, want := range []struct {
		name string
		oid  uint32
		size int32
	}{
		{"three", uint32(pgoid.INT4OID), 4},
		{"?column?", uint32(pgoid.BOOLOID), 1},
		{"int4", uint32(pgoid.INT4OID), 4},
		{"?column?", uint32(pgoid.INT8OID), 8},
	} {
		f := res.Fields[i]
		assert.Equal(t, want.name, f.Name)
		assert.Equal(t, want.oid, f.DataTypeOid)
		assert.Equal(t, want.size, f.DataTypeSize)
		assert.Equal(t, int32(-1), f.TypeModifier)
		assert.Equal(t, int32(0), f.Format, "simple protocol is text")
		assert.Zero(t, f.TableOid)
	}
	require.Len(t, res.Rows, 1)
	assert.Equal(t, [][]byte{[]byte("3"), []byte("t"), nil, []byte("-4294967296")}, values(res.Rows[0]))
}

// An evaluation error is the PostgreSQL diagnostic the engine raised, with
// no result preceding it.
func TestGatewayEval_StreamExecuteError(t *testing.T) {
	g := gatewayEvalFor(t, "SELECT 1, 1 / 0", nil)
	err := g.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error {
			t.Fatal("no result expected before the error")
			return nil
		})
	sqlstate(t, err, mterrors.PgSSDivisionByZero, "division by zero")
}

func TestGatewayEval_PortalStreamExecute(t *testing.T) {
	const sql = "SELECT $1::int4 + $2 AS sum, $2 > 3"
	g := gatewayEvalFor(t, sql, []uint32{0, 0})
	int4 := uint32(pgoid.INT4OID)
	be4 := func(b ...byte) []byte { return b }

	t.Run("text in, text out", func(t *testing.T) {
		res, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), []byte("4")}, nil, nil), false)
		require.NoError(t, err)
		assert.Nil(t, res.Fields, "RowDescription came from Describe")
		assert.Equal(t, "SELECT 1", res.CommandTag)
		assert.Equal(t, [][]byte{[]byte("7"), []byte("t")}, values(res.Rows[0]))
	})

	t.Run("binary in, binary out", func(t *testing.T) {
		pi := evalPortal(t, sql, []uint32{int4, int4}, [][]byte{be4(0, 0, 0, 3), be4(0, 0, 0, 4)}, []int16{1}, []int16{1})
		res, err := runPortal(t, g, pi, true)
		require.NoError(t, err)
		require.Len(t, res.Fields, 2)
		assert.Equal(t, int32(1), res.Fields[0].Format)
		assert.Equal(t, int32(1), res.Fields[1].Format)
		assert.Equal(t, [][]byte{be4(0, 0, 0, 7), {1}}, values(res.Rows[0]))
	})

	t.Run("per-parameter and per-column formats", func(t *testing.T) {
		pi := evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), be4(0, 0, 0, 4)}, []int16{0, 1}, []int16{1, 0})
		res, err := runPortal(t, g, pi, true)
		require.NoError(t, err)
		assert.Equal(t, int32(1), res.Fields[0].Format)
		assert.Equal(t, int32(0), res.Fields[1].Format)
		assert.Equal(t, [][]byte{be4(0, 0, 0, 7), []byte("t")}, values(res.Rows[0]))
	})

	t.Run("null parameter", func(t *testing.T) {
		res, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), nil}, nil, nil), false)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{nil, nil}, values(res.Rows[0]))
	})

	t.Run("parameter count mismatch", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3")}, nil, nil), false)
		sqlstate(t, err, mterrors.PgSSProtocolViolation, `bind message supplies 1 parameters, but prepared statement "stmt" requires 2`)
	})

	t.Run("parameter format count mismatch", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), []byte("4")}, []int16{0, 0, 0}, nil), false)
		sqlstate(t, err, mterrors.PgSSProtocolViolation, "bind message has 3 parameter formats but 2 parameters")
	})

	t.Run("result format count mismatch", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), []byte("4")}, nil, []int16{0, 0, 0}), false)
		sqlstate(t, err, mterrors.PgSSProtocolViolation, "bind message has 3 result formats but query has 2 columns")
	})

	t.Run("invalid text parameter", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("abc"), []byte("4")}, nil, nil), false)
		sqlstate(t, err, mterrors.PgSSInvalidTextRepresentation, `invalid input syntax for type integer: "abc"`)
	})

	t.Run("binary parameter with trailing bytes", func(t *testing.T) {
		pi := evalPortal(t, sql, []uint32{int4, int4}, [][]byte{be4(0, 0, 0, 3, 0), be4(0, 0, 0, 4)}, []int16{1}, nil)
		_, err := runPortal(t, g, pi, false)
		sqlstate(t, err, mterrors.PgSSInvalidBinaryRepresentation, "incorrect binary data format in bind parameter 1")
	})

	t.Run("binary parameter too short", func(t *testing.T) {
		pi := evalPortal(t, sql, []uint32{int4, int4}, [][]byte{be4(0, 3), be4(0, 0, 0, 4)}, []int16{1}, nil)
		_, err := runPortal(t, g, pi, false)
		sqlstate(t, err, mterrors.PgSSProtocolViolation, "insufficient data left in message")
	})

	// pg_client_to_server verifies a text parameter's bytes before the input
	// function runs; the input functions themselves stop at a NUL.
	t.Run("text parameter with embedded NUL", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("1\x00junk"), []byte("4")}, nil, nil), false)
		sqlstate(t, err, mterrors.PgSSCharacterNotInRepertoire, `invalid byte sequence for encoding "UTF8": 0x00`)
	})

	t.Run("text parameter with invalid UTF-8", func(t *testing.T) {
		for _, tc := range []struct {
			in   []byte
			want string
		}{
			{[]byte{0xff}, "0xff"},                       // not a lead byte
			{[]byte{0xc3, 0x28}, "0xc3 0x28"},            // bad continuation byte
			{[]byte{'4', 0xe2, 0x82}, "0xe2 0x82"},       // value ends inside the character
			{[]byte{0xed, 0xa0, 0x80}, "0xed 0xa0 0x80"}, // UTF-16 surrogate
		} {
			_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{tc.in, []byte("4")}, nil, nil), false)
			sqlstate(t, err, mterrors.PgSSCharacterNotInRepertoire, `invalid byte sequence for encoding "UTF8": `+tc.want)
		}
	})

	t.Run("valid non-ASCII text reaches the input function", func(t *testing.T) {
		_, err := runPortal(t, g, evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("é"), []byte("4")}, nil, nil), false)
		sqlstate(t, err, mterrors.PgSSInvalidTextRepresentation, `invalid input syntax for type integer: "é"`)
	})

	t.Run("evaluation error", func(t *testing.T) {
		g := gatewayEvalFor(t, "SELECT 1 / $1", []uint32{int4})
		_, err := runPortal(t, g, evalPortal(t, "SELECT 1 / $1", []uint32{int4}, [][]byte{[]byte("0")}, nil, nil), false)
		sqlstate(t, err, mterrors.PgSSDivisionByZero, "division by zero")
	})
}

// ValidateBind is what Bind runs for a portal the gateway will evaluate: the
// checks exec_bind_message makes, and nothing of Execute's.
func TestGatewayEval_ValidateBind(t *testing.T) {
	const sql = "SELECT $1::int4 + $2 AS sum, $2 > 3"
	g := gatewayEvalFor(t, sql, []uint32{0, 0})
	int4 := uint32(pgoid.INT4OID)

	require.NoError(t, g.ValidateBind(evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), []byte("4")}, nil, nil), "client"))

	err := g.ValidateBind(evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("abc"), []byte("4")}, nil, nil), "client")
	sqlstate(t, err, mterrors.PgSSInvalidTextRepresentation, `invalid input syntax for type integer: "abc"`)

	err = g.ValidateBind(evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("1\x00"), []byte("4")}, nil, nil), "client")
	sqlstate(t, err, mterrors.PgSSCharacterNotInRepertoire, `invalid byte sequence for encoding "UTF8": 0x00`)

	err = g.ValidateBind(evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3")}, nil, nil), "client")
	sqlstate(t, err, mterrors.PgSSProtocolViolation, `bind message supplies 1 parameters, but prepared statement "client" requires 2`)

	err = g.ValidateBind(evalPortal(t, sql, []uint32{int4, int4}, [][]byte{[]byte("3"), []byte("4")}, nil, []int16{0, 0, 0}), "client")
	sqlstate(t, err, mterrors.PgSSProtocolViolation, "bind message has 3 result formats but query has 2 columns")

	div := gatewayEvalFor(t, "SELECT 1 / $1", []uint32{int4})
	require.NoError(t, div.ValidateBind(evalPortal(t, "SELECT 1 / $1", []uint32{int4}, [][]byte{[]byte("0")}, nil, nil), "client"),
		"evaluation errors belong to Execute")
}

func TestGatewayEval_Description(t *testing.T) {
	const sql = "SELECT $1::int4 + $2 AS sum, $2 > 3"
	g := gatewayEvalFor(t, sql, []uint32{0, 0})

	t.Run("statement", func(t *testing.T) {
		desc, err := g.Description(nil)
		require.NoError(t, err)
		assert.True(t, desc.HasFields)
		require.Len(t, desc.Parameters, 2)
		assert.Equal(t, uint32(pgoid.INT4OID), desc.Parameters[0].DataTypeOid)
		assert.Equal(t, uint32(pgoid.INT4OID), desc.Parameters[1].DataTypeOid, "deduced from the operator")
		require.Len(t, desc.Fields, 2)
		assert.Equal(t, "sum", desc.Fields[0].Name)
		assert.Equal(t, uint32(pgoid.INT4OID), desc.Fields[0].DataTypeOid)
		assert.Equal(t, "?column?", desc.Fields[1].Name)
		assert.Equal(t, uint32(pgoid.BOOLOID), desc.Fields[1].DataTypeOid)
		assert.Equal(t, int32(0), desc.Fields[0].Format)
	})

	t.Run("portal reports its result formats", func(t *testing.T) {
		pi := evalPortal(t, sql, nil, [][]byte{[]byte("3"), []byte("4")}, nil, []int16{0, 1})
		desc, err := g.Description(pi.Portal)
		require.NoError(t, err)
		assert.Equal(t, int32(0), desc.Fields[0].Format)
		assert.Equal(t, int32(1), desc.Fields[1].Format)
	})

	t.Run("portal with mismatched result formats", func(t *testing.T) {
		pi := evalPortal(t, sql, nil, [][]byte{[]byte("3"), []byte("4")}, nil, []int16{0, 1, 0})
		_, err := g.Description(pi.Portal)
		sqlstate(t, err, mterrors.PgSSProtocolViolation, "bind message has 3 result formats but query has 2 columns")
	})
}

func TestGatewayEval_Accessors(t *testing.T) {
	g := gatewayEvalFor(t, "SELECT 1", nil)
	assert.Empty(t, g.GetTableGroup())
	assert.Empty(t, g.GetQuery())
	assert.Equal(t, "GatewayEval(SELECT 1)", g.String())
}
