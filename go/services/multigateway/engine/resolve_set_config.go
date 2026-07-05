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
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ResolveTrackSetConfig handles a SELECT whose target list is entirely
// set_config(...) calls where at least one argument can't be resolved at plan
// time — the shape pg_dump uses on PG17+:
//
//	SELECT set_config(name, 'view, foreign-table', false)
//	FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'
//
// A session-scoped (is_local=false) set_config must be tracked in
// SessionSettings so it survives pool rotation, which needs the concrete GUC
// name — but here the name is a column reference resolved per row. Rather than
// reject it (or pin the connection), this primitive runs in three steps:
//
//  1. Resolve: execute the "unroll" projection — the original SELECT with each
//     set_config(a, b, c) target replaced by its three arguments a, b, c — once,
//     reading the rows internally. This is a pure read that yields the concrete
//     (name, value, is_local) tuple per row. It runs through ResolveRoute, an
//     ordinary Route (or an AdvisoryLockRoute when a set_config argument
//     acquires/releases a session advisory lock), so backend pinning and
//     bindVar reconstruction reuse the existing routing machinery.
//  2. Apply: synthesize a set_config(...) query from the *captured literals* and
//     run it, forwarding PostgreSQL's authoritative result to the client. Using
//     the captured literals — not a re-run of the original dynamic query — is
//     essential: the original could resolve to different rows/values the second
//     time (concurrent catalog change, volatile FROM).
//  3. Track: record the session-scoped (is_local=false) tuples in
//     SessionSettings for pool-rotation replay. is_local=true tuples were
//     applied by step 2 but are transaction-scoped and deliberately not tracked.
//
// Zero resolved rows (e.g. a WHERE that matches nothing — a GUC absent on this
// server version) means nothing is set and the client gets an empty result,
// matching stock PostgreSQL.
type ResolveTrackSetConfig struct {
	// TableGroup is the target tablegroup for the apply query.
	TableGroup string

	// Shard is the target shard (empty for unsharded).
	Shard string

	// Query is the original SQL string, kept for GetQuery/debug output.
	Query string

	// ResolveRoute runs the unroll projection. It is a Route, or an
	// AdvisoryLockRoute wrapping one when the projection acquires/releases a
	// session-level advisory lock — so opts/advisory pinning and $N bindVar
	// reconstruction flow through the same primitives as ordinary queries. It is
	// executed with a capturing callback: the resolve reads the rows, the client
	// never sees them.
	ResolveRoute Primitive

	// unrollAST is the projection's AST. ResolveRoute already holds it for
	// execution; we keep a reference only to enumerate $N parameters on the
	// extended-protocol path, where the portal's Bind values must be decoded
	// before ResolveRoute can reconstruct the SQL.
	unrollAST ast.Stmt

	// Aliases holds the per-call output-column alias in target-list order
	// (ResTarget.Name, "" when the call had no AS). len(Aliases) is the number
	// of set_config calls; the unroll result has three columns per call.
	Aliases []string
}

// NewResolveTrackSetConfig creates a new ResolveTrackSetConfig primitive.
// resolveRoute runs the unroll projection (built by the planner via
// routePrimitive so advisory-lock pinning is folded in); unrollAST is the same
// projection AST, used to enumerate parameters on the portal path.
func NewResolveTrackSetConfig(tableGroup, shard, sql string, resolveRoute Primitive, unrollAST ast.Stmt, aliases []string) *ResolveTrackSetConfig {
	return &ResolveTrackSetConfig{
		TableGroup:   tableGroup,
		Shard:        shard,
		Query:        sql,
		ResolveRoute: resolveRoute,
		unrollAST:    unrollAST,
		Aliases:      aliases,
	}
}

// StreamExecute runs the resolve/apply/track flow on the simple-query-protocol
// path, where non-set_config literals arrive as bindVars from the normalizer.
func (s *ResolveTrackSetConfig) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return s.execute(ctx, exec, conn, state, bindVars, info, callback)
}

// PortalStreamExecute runs the same flow on the extended-protocol path. The
// unroll's $N placeholders are the user's prepared-statement parameters, so we
// decode them from the portal's Bind values into literals; ResolveRoute then
// reconstructs the projection SQL from them, exactly as on the simple path.
func (s *ResolveTrackSetConfig) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	bindVars, err := s.bindVarsFromPortal(portalInfo)
	if err != nil {
		return err
	}
	return s.execute(ctx, exec, conn, state, bindVars, info, callback)
}

// execute drives resolve → apply → track. bindVars resolves any $N ParamRefs
// in the unroll projection (empty when there are none).
func (s *ResolveTrackSetConfig) execute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// info (the plan's reservation directives, e.g. an advisory-lock pin) rides
	// on the resolve projection — that's the query that actually runs the user's
	// pg_advisory_lock call. The separate set_config apply below is auxiliary, so
	// it carries the zero value.
	rows, err := s.resolve(ctx, exec, conn, state, bindVars, info)
	if err != nil {
		return err
	}

	// Nothing matched: no GUC is set and the client sees an empty result with
	// the original's columns, exactly as stock PostgreSQL would respond.
	if len(rows) == 0 {
		return callback(ctx, s.emptyResult())
	}

	// Apply every resolved tuple with literals (is_local := true — see
	// buildApplySQL) and forward PostgreSQL's authoritative result to the client.
	applySQL, err := s.buildApplySQL(rows)
	if err != nil {
		return err
	}
	if err := exec.StreamExecute(ctx, conn, s.TableGroup, s.Shard, applySQL, nil, state, PlanExecInfo{}, callback); err != nil {
		return err
	}

	// Track the session-scoped settings only after a successful apply, so we
	// never record a setting PostgreSQL rejected.
	s.track(state, rows)
	return nil
}

// resolve runs the unroll projection through ResolveRoute and accumulates its
// rows. The result is consumed internally via the capturing callback (never
// forwarded to the client). Running through ResolveRoute means any
// advisory-lock pinning and $N bindVar reconstruction happen in the existing
// Route/AdvisoryLockRoute primitives. Each row must have three columns per
// set_config call.
func (s *ResolveTrackSetConfig) resolve(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
) ([]*sqltypes.Row, error) {
	var rows []*sqltypes.Row
	err := s.ResolveRoute.StreamExecute(ctx, exec, conn, state, bindVars, info,
		func(_ context.Context, res *sqltypes.Result) error {
			rows = append(rows, res.Rows...)
			return nil
		})
	if err != nil {
		return nil, err
	}
	want := len(s.Aliases) * 3
	for _, r := range rows {
		if len(r.Values) != want {
			// The unroll projects exactly three columns per set_config call, so a
			// mismatch means the plan and the executed projection disagree — a bug.
			return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
				"internal error resolving set_config (please report this as a bug)",
				fmt.Sprintf("expected %d columns in the projection result, got %d", want, len(r.Values)))
		}
	}
	return rows, nil
}

// buildApplySQL synthesizes a query that re-runs each set_config call with the
// captured literal arguments, reproducing the original's columns (and aliases)
// and one row per resolved row. Rows are combined with UNION ALL, which takes
// its column names from the first leg — so aliases are emitted on the first row
// only.
//
// Every call is applied with is_local := true regardless of the captured
// is_local, because the multipooler is the sole authority on session GUC state
// (see also ValidateSetting). A session-scoped (is_local=false) set_config that
// persisted on the pooled backend would leak across clients when the backend is
// reused — the multipooler doesn't track raw set_config mutations. So the apply
// only needs to *return* the authoritative value (set_config returns the value
// it set even under GUC_ACTION_LOCAL, which reverts at statement end), while
// persistence for session-scoped settings is delivered by track() →
// SessionSettings replay. A genuinely is_local=true call is correct as-is, and
// a tracked is_local=false call is re-applied on every subsequent query by the
// replay path.
func (s *ResolveTrackSetConfig) buildApplySQL(rows []*sqltypes.Row) (string, error) {
	numCalls := len(s.Aliases)
	legs := make([]string, 0, len(rows))
	for ri, row := range rows {
		targets := make([]string, 0, numCalls)
		for ci := range numCalls {
			name := row.Values[ci*3]
			value := row.Values[ci*3+1]
			call := fmt.Sprintf("set_config(%s, %s, true)",
				name.SQLLiteral(), value.SQLLiteral())
			if ri == 0 && s.Aliases[ci] != "" {
				call += " AS " + ast.QuoteIdentifier(s.Aliases[ci])
			}
			targets = append(targets, call)
		}
		legs = append(legs, "SELECT "+strings.Join(targets, ", "))
	}
	return strings.Join(legs, " UNION ALL "), nil
}

// track records the session-scoped (is_local=false) resolved tuples in
// SessionSettings. A NULL value resets the GUC to its default
// (set_config(name, NULL, false) semantics); a NULL name is skipped (the apply
// query would already have raised PostgreSQL's error).
//
// It uses the same helper as ApplySessionState for role/session authorization
// coupling: SET SESSION AUTHORIZATION clears the active role, and role value
// "none" means RESET ROLE. Building synthetic primitives per tuple would be
// more code for the same tracking behavior.
func (s *ResolveTrackSetConfig) track(state *handler.MultigatewayConnectionState, rows []*sqltypes.Row) {
	numCalls := len(s.Aliases)
	for _, row := range rows {
		for ci := range numCalls {
			name := row.Values[ci*3]
			value := row.Values[ci*3+1]
			isLocal := row.Values[ci*3+2]
			if name.IsNull() || isLocal.IsTrue() {
				continue
			}
			if value.IsNull() {
				resetTrackedSessionVariable(state, string(name))
				continue
			}
			applyTrackedSessionVariable(state, string(name), string(value))
		}
	}
}

// emptyResult builds a zero-row result carrying the original's columns (text),
// used when the unroll projection matched no rows.
func (s *ResolveTrackSetConfig) emptyResult() *sqltypes.Result {
	fields := make([]*query.Field, len(s.Aliases))
	for i, alias := range s.Aliases {
		name := alias
		if name == "" {
			name = "set_config"
		}
		fields[i] = &query.Field{Name: name, Type: "text", DataTypeOid: uint32(ast.TEXTOID)}
	}
	return &sqltypes.Result{
		Fields:     fields,
		CommandTag: "SELECT 0",
	}
}

// bindVarsFromPortal decodes the portal's Bind values for every $N referenced
// in the unroll projection, returning them positionally (index 0 is $1) so
// ResolveRoute can substitute them. Returns nil when the projection has no
// parameters.
func (s *ResolveTrackSetConfig) bindVarsFromPortal(portalInfo *preparedstatement.PortalInfo) ([]*ast.A_Const, error) {
	maxNum := 0
	var refs []*ast.ParamRef
	ast.Rewrite(s.unrollAST, func(cursor *ast.Cursor) bool {
		if pr, ok := cursor.Node().(*ast.ParamRef); ok {
			refs = append(refs, pr)
			if pr.Number > maxNum {
				maxNum = pr.Number
			}
		}
		return true
	}, nil)
	if maxNum == 0 {
		return nil, nil
	}
	bindVars := make([]*ast.A_Const, maxNum)
	for _, pr := range refs {
		text, err := preparedstatement.DecodeBindAsText(portalInfo, pr, "set_config resolve projection parameter")
		if err != nil {
			return nil, err
		}
		bindVars[pr.Number-1] = ast.NewA_Const(ast.NewString(text), 0)
	}
	return bindVars, nil
}

// GetTableGroup returns the target tablegroup.
func (s *ResolveTrackSetConfig) GetTableGroup() string {
	return s.TableGroup
}

// GetQuery returns the original SQL string.
func (s *ResolveTrackSetConfig) GetQuery() string {
	return s.Query
}

// String returns a description for debugging.
func (s *ResolveTrackSetConfig) String() string {
	return fmt.Sprintf("ResolveTrackSetConfig(%s)", s.Query)
}

// Ensure ResolveTrackSetConfig implements Primitive.
var _ Primitive = (*ResolveTrackSetConfig)(nil)
