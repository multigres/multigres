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
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/exec"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/funcs"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// GatewayEval answers a SELECT whose target list is nothing but constant
// expressions (`SELECT 1`, `SELECT $1::int4 + 2`) in the gateway, with the
// PostgreSQL engine port in go/common/pgeval, so the query never reaches a
// backend. The planner admits a statement only when every column analyzes,
// prepares and is immutable; anything else is routed as before (see
// planner.IsTargetListOnlySelect and the admission in planner/gateway_eval.go).
//
// The primitive holds prepared expression state, which is not safe for
// concurrent use, so its plan is never cached: the planner builds one per
// execution.
//
// What a backend would have done for the same statement, and what this path
// does instead:
//   - Backend checkout. The statement succeeds with no pooler or PostgreSQL
//     reachable, and a session whose startup parameters would be rejected at
//     checkout does not learn it here; the error surfaces on the first routed
//     statement. A driver's `SELECT 1` validation query now validates the
//     gateway, not the backend.
//   - Name resolution. Functions, operators and types resolve against the
//     compiled-in pg_catalog. PostgreSQL searches pg_catalog first unless
//     search_path lists it explicitly after a user schema; only such a
//     session can shadow a builtin, and the shadow is not seen here.
//   - Portal lifecycle. Every Execute of the portal returns the row again,
//     where PostgreSQL reports an exhausted portal with zero rows, and
//     maxRows is ignored (one row, never PortalSuspended).
//   - Parser settings. Literals are parsed with the default
//     standard_conforming_strings and backslash_quote. A literal whose
//     escapes parse differently under a session's changed setting is
//     different here; one that becomes invalid input declines at analysis
//     and routes.
//   - Observability. The backend's log and pg_stat_statements never see the
//     statement; the gateway's query log records plan type GatewayEval.
//
// Unchanged: a transaction block or a reserved backend (advisory lock, temp
// table) routes (the planner declines), so aborted state, snapshots, and
// `SELECT 1` as the probe that discovers a lost reserved backend behave as
// before. The extended protocol's implicit transaction between two Sync
// messages was never modelled by the gateway; each Execute autocommits
// whether routed or evaluated here.
type GatewayEval struct {
	sql        string
	columns    []EvalColumn
	paramTypes []pgoid.Oid
}

// EvalColumn is one result column: its name, type and prepared expression.
type EvalColumn struct {
	Name   string
	Type   pgoid.Oid
	Typmod int32
	State  *exec.ExprState
}

// NewGatewayEval creates the primitive. paramTypes are the statement's
// parameter types after analysis deduced the undeclared ones, one per
// parameter the client's Parse declared or the statement referenced.
func NewGatewayEval(sql string, columns []EvalColumn, paramTypes []pgoid.Oid) *GatewayEval {
	return &GatewayEval{sql: sql, columns: columns, paramTypes: paramTypes}
}

// Description answers Describe. portal is nil for Describe('S'), whose
// RowDescription is always text; for Describe('P') the portal's result
// formats are reported, as the backend would.
func (g *GatewayEval) Description(portal *query.Portal) (*query.StatementDescription, error) {
	var formats []int16
	if portal != nil {
		var err error
		if formats, err = g.resultFormats(portal); err != nil {
			return nil, err
		}
	}
	params := make([]*query.ParameterDescription, len(g.paramTypes))
	for i, typ := range g.paramTypes {
		params[i] = &query.ParameterDescription{DataTypeOid: uint32(typ)}
	}
	return &query.StatementDescription{
		Parameters: params,
		Fields:     g.fields(formats),
		HasFields:  true,
	}, nil
}

// fields builds the RowDescription; formats is nil for all-text.
func (g *GatewayEval) fields(formats []int16) []*query.Field {
	fields := make([]*query.Field, len(g.columns))
	for i, col := range g.columns {
		typ := pgcatalog.TypeByOid(col.Type)
		f := &query.Field{
			Name:         col.Name,
			Type:         typ.Name,
			DataTypeOid:  uint32(col.Type),
			DataTypeSize: int32(typ.Len),
			TypeModifier: col.Typmod,
		}
		if formats != nil {
			f.Format = int32(formats[i])
		}
		fields[i] = f
	}
	return fields
}

// StreamExecute evaluates the columns with no parameters (simple protocol).
func (g *GatewayEval) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	row, err := g.evaluate(nil, nil)
	if err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{
		Fields:     g.fields(nil),
		Rows:       []*sqltypes.Row{row},
		CommandTag: "SELECT 1",
	})
}

// PortalStreamExecute decodes the portal's Bind values with the parameter
// types' input or receive functions, evaluates, and encodes each column in
// the portal's result format. Fields ride along only when a Describe('P')
// was folded into the Execute; otherwise the separate Describe sent them.
func (g *GatewayEval) PortalStreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	includeDescribe bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	formats, err := g.resultFormats(portalInfo.Portal)
	if err != nil {
		return err
	}
	params, err := g.decodeParams(portalInfo)
	if err != nil {
		return err
	}
	row, err := g.evaluate(params, formats)
	if err != nil {
		return err
	}
	result := &sqltypes.Result{
		Rows:       []*sqltypes.Row{row},
		CommandTag: "SELECT 1",
	}
	if includeDescribe {
		result.Fields = g.fields(formats)
	}
	return callback(ctx, result)
}

// evaluate runs every column and renders it in its format (text when
// formats is nil). An evaluation error such as division by zero is returned
// as the PostgreSQL diagnostic the engine raised.
func (g *GatewayEval) evaluate(params []exec.ExternParam, formats []int16) (*sqltypes.Row, error) {
	values := make([][]byte, len(g.columns))
	for i, col := range g.columns {
		v, err := col.State.Eval(params)
		if err != nil {
			return nil, err
		}
		if v.IsNull {
			continue
		}
		typ := pgcatalog.TypeByOid(col.Type)
		proc := typ.Output
		if formats != nil && formats[i] == 1 {
			proc = typ.Send
		}
		flinfo, err := fmgr.FmgrInfoFor(proc)
		if err != nil {
			return nil, err
		}
		err = pgerror.Recover(func() {
			values[i] = datum.DatumGetBytes(fmgr.FunctionCall1Coll(flinfo, pgoid.InvalidOid, v.Value))
		})
		if err != nil {
			return nil, err
		}
	}
	return sqltypes.MakeRow(values), nil
}

// resultFormats resolves the Bind message's result-column format codes the
// way exec_bind_message does (postgres.c): none means all text, one applies
// to every column, otherwise there must be exactly one per column.
func (g *GatewayEval) resultFormats(portal *query.Portal) ([]int16, error) {
	formats := make([]int16, len(g.columns))
	switch n := len(portal.ResultFormats); {
	case n == 0:
	case n == 1:
		for i := range formats {
			formats[i] = int16(portal.ResultFormats[0])
		}
	case n == len(g.columns):
		for i := range formats {
			formats[i] = int16(portal.ResultFormats[i])
		}
	default:
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
			fmt.Sprintf("bind message has %d result formats but query has %d columns", n, len(g.columns)), "")
	}
	return formats, nil
}

// decodeParams turns the portal's Bind values into engine parameters -
// exec_bind_message (postgres.c). A text value goes through the type's input
// function, a binary one through its receive function, which must consume
// the value exactly.
func (g *GatewayEval) decodeParams(portalInfo *preparedstatement.PortalInfo) ([]exec.ExternParam, error) {
	portal := portalInfo.Portal
	values := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	if len(values) != len(g.paramTypes) {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
			fmt.Sprintf("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
				len(values), portalInfo.PreparedStatementInfo.GetName(), len(g.paramTypes)), "")
	}
	if n := len(portal.ParamFormats); n > 1 && n != len(values) {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
			fmt.Sprintf("bind message has %d parameter formats but %d parameters", n, len(values)), "")
	}
	params := make([]exec.ExternParam, len(values))
	for i, raw := range values {
		typ := pgcatalog.TypeByOid(g.paramTypes[i])
		params[i] = exec.ExternParam{Type: typ.Oid, IsNull: raw == nil}
		if raw == nil {
			continue
		}
		format := int32(0)
		switch len(portal.ParamFormats) {
		case 0:
		case 1:
			format = portal.ParamFormats[0]
		default:
			format = portal.ParamFormats[i]
		}
		// getTypeIOParam: the element type for an array type, else the type.
		ioparam := typ.Oid
		if typ.Elem != pgoid.InvalidOid {
			ioparam = typ.Elem
		}
		var err error
		if format == 1 {
			flinfo, ferr := fmgr.FmgrInfoFor(typ.Receive)
			if ferr != nil {
				return nil, ferr
			}
			buf := funcs.NewBinaryInput(raw)
			err = pgerror.Recover(func() {
				params[i].Value = fmgr.FunctionCall3Coll(flinfo, pgoid.InvalidOid,
					buf.Datum(), datum.ObjectIdGetDatum(ioparam), datum.Int32GetDatum(-1))
			})
			if err == nil && buf.Remaining() != 0 {
				err = mterrors.NewPgError("ERROR", mterrors.PgSSInvalidBinaryRepresentation,
					fmt.Sprintf("incorrect binary data format in bind parameter %d", i+1), "")
			}
		} else {
			flinfo, ferr := fmgr.FmgrInfoFor(typ.Input)
			if ferr != nil {
				return nil, ferr
			}
			err = pgerror.Recover(func() {
				params[i].Value = fmgr.FunctionCall3Coll(flinfo, pgoid.InvalidOid,
					datum.BytesGetDatum(raw), datum.ObjectIdGetDatum(ioparam), datum.Int32GetDatum(-1))
			})
		}
		if err != nil {
			return nil, err
		}
	}
	return params, nil
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (g *GatewayEval) GetTableGroup() string {
	return ""
}

// GetQuery returns empty string as this primitive doesn't execute a query.
func (g *GatewayEval) GetQuery() string {
	return ""
}

// String returns a description for logging/debugging.
func (g *GatewayEval) String() string {
	return fmt.Sprintf("GatewayEval(%s)", g.sql)
}

// Ensure GatewayEval implements Primitive interface.
var _ Primitive = (*GatewayEval)(nil)
