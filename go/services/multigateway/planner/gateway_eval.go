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
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	"github.com/multigres/multigres/go/common/pgeval/exec"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// IsTargetListOnlySelect reports whether stmt is a SELECT with nothing but a
// target list: no FROM, WHERE, GROUP BY, HAVING, WINDOW, ORDER BY, LIMIT,
// locking, WITH, DISTINCT, INTO, VALUES or set operation. Only such a
// statement can be answered by the gateway's expression engine (see
// GatewayEval), and because whether it can depends on its literals, the
// executor keeps these off the normalized plan cache.
func IsTargetListOnlySelect(stmt ast.Stmt) bool {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return false
	}
	empty := func(l *ast.NodeList) bool { return l == nil || l.Len() == 0 }
	return ss.Op == ast.SETOP_NONE && ss.Larg == nil && ss.Rarg == nil &&
		!empty(ss.TargetList) &&
		ss.DistinctClause == nil && ss.IntoClause == nil &&
		empty(ss.FromClause) && ss.WhereClause == nil &&
		empty(ss.GroupClause) && ss.HavingClause == nil && empty(ss.WindowClause) &&
		empty(ss.ValuesLists) && empty(ss.SortClause) &&
		ss.LimitOffset == nil && ss.LimitCount == nil &&
		empty(ss.LockingClause) && ss.WithClause == nil
}

// SessionAllowsGatewayEval reports whether a session's state permits the
// gateway to answer a statement without a backend: it is idle (no
// transaction block), not an unsafe connection, and holds no reserved
// backend. The executor uses the same predicate to keep such statements off
// the plan cache, so a plan the planner could answer locally is never cached
// and a cached plan was never built for a session that could (see
// IsTargetListOnlySelect).
func SessionAllowsGatewayEval(conn *server.Conn) bool {
	if conn == nil {
		return true
	}
	if conn.TxnStatus() != protocol.TxnStatusIdle || conn.UnsafeConnection() {
		return false
	}
	if st, ok := conn.GetConnectionState().(*handler.MultigatewayConnectionState); ok && st != nil && st.HasReservedConnection() {
		return false
	}
	return true
}

// GatewayEvalFor is the Describe entry point: it returns the primitive that
// Execute would run for stmt (the client's declared parameter types in hand),
// so Describe can report the same parameters and columns, or nil when Execute
// would route the statement and Describe must be forwarded too. It runs the
// same statement analysis Plan does, declining on any error so a rejection
// surfaces at Execute exactly as before.
func (p *Planner) GatewayEvalFor(sql string, stmt ast.Stmt, conn *server.Conn, paramTypes []uint32) *engine.GatewayEval {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok || !IsTargetListOnlySelect(ss) {
		return nil
	}
	analysis, err := analyzeStatement(stmt, conn != nil && conn.UnsafeConnection(), p.admitsFailoverSlots())
	if err != nil || len(analysis.SetConfigs) > 0 || analysis.DynamicSetConfig {
		return nil
	}
	opts := PlanOptions{IsPortal: true, ParamTypes: paramTypes}
	opts.applyAnalysis(analysis)
	return p.gatewayEval(sql, ss, conn, opts)
}

// gatewayEval decides whether ss can be answered in the gateway and builds
// the primitive if so; nil means route it to PostgreSQL. Admission is
// conservative and never depends on parameter values:
//
//   - the statement is a target-list-only SELECT (IsTargetListOnlySelect)
//     with no routing signal (advisory lock, replication slot, setseed,
//     gateway-managed current_setting);
//   - the session allows it (SessionAllowsGatewayEval): not in a transaction
//     block, so the gateway need not mirror snapshot or aborted-transaction
//     state; not an unsafe connection, which asked for its pinned backend;
//     and holding no reserved backend, whose loss the next statement must
//     surface;
//   - every column analyzes against the builtin catalog with the client's
//     parameter declarations, prepares in the evaluator, calls only
//     immutable functions, and has a type whose text and binary I/O is
//     ported, as does every parameter;
//   - an unknown-type column (a bare string literal) is declined: PostgreSQL
//     resolves it to text through textin, which is not ported.
//
// Any analysis or preparation error declines rather than reporting it: a
// user-defined object on search_path could make PostgreSQL accept what the
// builtin catalog rejects, so the backend keeps the last word on errors.
func (p *Planner) gatewayEval(sql string, ss *ast.SelectStmt, conn *server.Conn, opts PlanOptions) *engine.GatewayEval {
	if !IsTargetListOnlySelect(ss) || opts.routesSpecially() {
		return nil
	}
	// PostgreSQL rejects a wider target list with 54011 "target lists can have
	// at most 1664 entries" (MaxTupleAttributeNumber); let it.
	if ss.TargetList.Len() > maxTupleAttributeNumber {
		return nil
	}
	if !SessionAllowsGatewayEval(conn) {
		return nil
	}

	// The simple protocol has no parameters: a $n is "there is no parameter",
	// which declines below and lets PostgreSQL report it. The extended
	// protocol deduces undeclared (0) types as exec_parse_message does.
	ps := analyze.NewParseState()
	if opts.IsPortal {
		declared := make([]pgoid.Oid, len(opts.ParamTypes))
		for i, t := range opts.ParamTypes {
			declared[i] = pgoid.Oid(t)
		}
		ps.SetVariableParameters(declared)
	}

	type target struct {
		name string
		tree ast.Node
	}
	targets := make([]target, 0, ss.TargetList.Len())
	for _, item := range ss.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok || rt.Indirection != nil {
			return nil
		}
		// The name comes from the raw tree, and analysis consumes its input,
		// so figure it first and analyze a clone: the statement may be a
		// prepared statement's shared AST.
		name := rt.Name
		if name == "" {
			name = analyze.FigureColname(rt.Val)
		}
		tree, err := ps.TransformExpr(ast.CloneNode(rt.Val))
		if err != nil {
			return nil
		}
		targets = append(targets, target{name, tree})
	}
	for _, t := range targets {
		if err := ps.CheckVariableParameters(t.tree); err != nil {
			return nil
		}
	}
	paramTypes := ps.ParamTypes()
	for _, typ := range paramTypes {
		// An undeduced parameter is exec_parse_message's "could not determine
		// data type of parameter $n".
		if typ == pgoid.InvalidOid || typ == pgoid.UNKNOWNOID || !ioPorted(typ) {
			return nil
		}
	}

	columns := make([]engine.EvalColumn, 0, len(targets))
	for _, t := range targets {
		typ := analyze.ExprType(t.tree)
		if typ == pgoid.UNKNOWNOID || !ioPorted(typ) || !immutable(t.tree) {
			return nil
		}
		state, err := exec.Prepare(t.tree)
		if err != nil {
			return nil
		}
		columns = append(columns, engine.EvalColumn{
			Name:   t.name,
			Type:   typ,
			Typmod: analyze.ExprTypmod(t.tree),
			State:  state,
		})
	}
	return engine.NewGatewayEval(sql, columns, paramTypes)
}

// maxTupleAttributeNumber is the most columns a result may have -
// access/htup_details.h (MaxTupleAttributeNumber).
const maxTupleAttributeNumber = 1664

// routesSpecially reports whether analysis attached a signal that needs a
// backend: a reservation to pin or a gateway-managed read to rewrite.
func (o PlanOptions) routesSpecially() bool {
	return o.PinForAdvisoryLock || o.RecheckForAdvisoryLock || o.PinForLogicalReplicationSlot ||
		o.PinForSetSeed || o.RewriteCurrentSetting
}

// ioPorted reports whether a type's text and binary input and output
// functions are all available in the engine, so a parameter of the type can
// be decoded and a column of it rendered in either format the client picks.
func ioPorted(typ pgoid.Oid) bool {
	t := pgcatalog.TypeByOid(typ)
	return t != nil && registered(t.Input) && registered(t.Output) && registered(t.Receive) && registered(t.Send)
}

func registered(proc pgoid.Oid) bool {
	pr := pgcatalog.ProcByOid(proc)
	return pr != nil && fmgr.IsBuiltinRegistered(pr.Src)
}

// immutable reports whether every function an analyzed expression calls is
// immutable, so the gateway's answer is the one any backend would give. A
// stable or volatile builtin (now(), random(), current_setting()) must run on
// a backend even once it is ported.
func immutable(tree ast.Node) bool {
	ok := true
	check := func(funcid pgoid.Oid) bool {
		proc := pgcatalog.ProcByOid(funcid)
		if proc == nil || proc.Volatile != pgcatalog.VolatilityImmutable {
			ok = false
		}
		return ok
	}
	analyze.Walk(tree, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.FuncExpr:
			return check(e.Funcid)
		case *ast.OpExpr:
			return check(e.Opfuncid)
		case *ast.DistinctExpr:
			return check(e.Opfuncid)
		case *ast.NullIfExpr:
			return check(e.Opfuncid)
		case *ast.ScalarArrayOpExpr:
			return check(e.Opfuncid)
		case *ast.CoerceViaIO:
			// The cast runs the source type's output and the result type's
			// input function.
			return check(pgcatalog.TypeByOid(analyze.ExprType(e.Arg)).Output) &&
				check(pgcatalog.TypeByOid(e.Resulttype).Input)
		}
		return true
	})
	return ok
}
