// Copyright 2025 Supabase, Inc.
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
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ApplySessionState records a SET/RESET in the gateway's local session-settings
// tracker. The pool propagates the tracked settings to a backend on the next
// query via ApplySettings, so the change survives pool rotation.
//
// This primitive does NOT validate against PostgreSQL — it only records state.
// Validation is the planner's job: a real `SET var = value` is planned as
// Sequence[ValidateSetting, ApplySessionState] (see planner.planVariableSetStmt).
// The ValidateSetting step runs set_config(..., is_local := true) on a backend so
// an invalid name/value errors at SET time (but leaves no backend state behind),
// and this primitive runs only if that succeeded — recording the setting and
// emitting CommandComplete("SET"). A silent variant of this primitive is also
// used for `SELECT set_config(...)` (see planner.planSelectStmt), where a
// trailing Route owns the client response.
//
// RESET/RESET ALL run through this primitive directly (no backend round-trip):
// the variable is removed from SessionSettings, and on the next query the merged
// settings (SessionSettings overlaid on StartupParams) fall back to the
// startup/default value.
type ApplySessionState struct {
	// VariableStmt is the SET/RESET statement from the AST.
	VariableStmt *ast.VariableSetStmt

	// Query is the original SQL string.
	Query string

	// SilentTracking, when true, updates SessionSettings but does NOT invoke
	// the callback. Used inside a Sequence where a sibling primitive (like
	// Route) owns the client-facing result — if both called back, the client
	// would see a stray CommandComplete before the real row data. This is
	// the shape a `SELECT set_config(...)` plan takes: silent tracking step
	// first, then a Route that sends the query to PG and streams the result.
	SilentTracking bool

	// BindRefs, when non-nil, marks this primitive as a deferred-resolution
	// set_config(): name, value, and/or is_local are resolved at execute
	// time before SessionSettings is updated. Nil-valued fields use the
	// literal already in VariableStmt.Name / Args[0] / VariableStmt.IsLocal.
	//
	// Two resolution sources, one per protocol path:
	//   - PortalStreamExecute decodes the portal's wire-protocol Bind values
	//     (extended-protocol `SELECT set_config($1, ...)`).
	//   - StreamExecute resolves against the normalizer-extracted bindVars
	//     (simple protocol: ast.Normalize parameterizes the value of a
	//     set_config(..., true) call, so the cached plan for a gateway-managed
	//     variable carries a ValueParam that must be re-resolved from each
	//     execution's literals — never baked into the plan).
	//
	// Set automatically by NewApplySessionStateFromBind.
	BindRefs *BoundSetConfigRefs
}

// BoundSetConfigRefs carries the ParamRef-to-portal-bind mapping for a
// set_config(...) call whose name, value, or is_local was supplied as a
// wire-protocol bound parameter. Each *ast.ParamRef field is the parser-
// produced ParamRef for the corresponding set_config slot, or nil when
// that slot was already a literal in the AST.
//
// is_local literals don't need to be carried here: they are baked into the
// synthetic VariableStmt at plan time, so IsLocalParam == nil means "use
// VariableStmt.IsLocal" — false for a literal-false call, true only for a
// gateway-managed set_config(..., true), which the planner tracks as a
// transaction-local override (an ordinary literal-true call produces no
// setConfigCall at all). Both execute paths fall back to that field.
type BoundSetConfigRefs struct {
	NameParam    *ast.ParamRef
	ValueParam   *ast.ParamRef
	IsLocalParam *ast.ParamRef
}

// NewApplySessionState creates a new ApplySessionState primitive.
func NewApplySessionState(sql string, stmt *ast.VariableSetStmt) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt: stmt,
		Query:        sql,
	}
}

// NewApplySessionStateSilent creates an ApplySessionState that updates the
// tracker without emitting anything to the client. Intended for use inside a
// Sequence where a Route primitive owns the client-facing response — see
// planner.planSelectStmt for the `SELECT set_config(...), * FROM t` case.
func NewApplySessionStateSilent(sql string, stmt *ast.VariableSetStmt) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt:   stmt,
		Query:          sql,
		SilentTracking: true,
	}
}

// NewApplySessionStateFromBind creates a silent ApplySessionState whose
// name, value, or is_local will be resolved from the portal's Bind values
// at execute time. The synthetic VariableStmt holds bind-placeholder
// strings (e.g. "__bind_$1__") in the slots that refs marks as bound; the
// placeholders are overwritten at execute time and exist only to keep
// SqlString-based debug output structurally valid.
//
// Always silent: a bound set_config is only emitted as part of the
// Sequence[ApplySessionState, Route] plan, where the trailing Route owns
// the client-facing CommandComplete.
func NewApplySessionStateFromBind(sql string, stmt *ast.VariableSetStmt, refs *BoundSetConfigRefs) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt:   stmt,
		Query:          sql,
		SilentTracking: true,
		BindRefs:       refs,
	}
}

// PortalStreamExecute handles SET/RESET on the extended-protocol path.
//
// Two modes:
//   - BindRefs == nil: the primitive carries literal arguments only, so the
//     portal's binds are irrelevant; delegate to StreamExecute. This is the
//     simple/literal SET/RESET shape and the
//     `SELECT set_config('lit', 'lit', false)` Sequence step.
//   - BindRefs != nil: name/value/is_local must be decoded from the
//     portal's wire-protocol Bind values before SessionSettings is updated.
//     Reached only via the `SELECT set_config($1, ...)` Sequence step.
func (s *ApplySessionState) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if s.BindRefs != nil {
		return s.executeSetWithBinds(ctx, conn, state, portalInfo, callback)
	}
	return s.StreamExecute(ctx, exec, conn, state, nil, callback)
}

// setConfigParamResolver is the small protocol-specific layer for resolving
// bound set_config(...) arguments. The executeSetWithResolvedParams helper owns
// the shared is_local -> name -> GMV guard -> value -> applyTracked flow, while
// each caller supplies how ParamRefs are decoded for its protocol path.
type setConfigParamResolver struct {
	resolveBool func(ref *ast.ParamRef, what string) (bool, error)
	resolveText func(ref *ast.ParamRef, what string) (string, error)
}

// executeSetWithBinds resolves name/value/is_local from the portal's binds
// and conditionally updates SessionSettings.
//
// Errors (NULL bind, unsupported OID, out-of-range ParamRef) propagate up
// through the Sequence, which aborts before the Route fires.
func (s *ApplySessionState) executeSetWithBinds(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return s.executeSetWithResolvedParams(ctx, conn, state, setConfigParamResolver{
		resolveBool: func(ref *ast.ParamRef, what string) (bool, error) {
			return preparedstatement.DecodeBindAsBool(portalInfo, ref, what)
		},
		resolveText: func(ref *ast.ParamRef, what string) (string, error) {
			return preparedstatement.DecodeBindAsText(portalInfo, ref, what)
		},
	}, callback)
}

// executeSetWithNormalizedBinds is StreamExecute's counterpart to
// executeSetWithBinds: it resolves the bound set_config slots from the
// normalizer-extracted bindVars instead of a portal's wire-protocol Bind
// values. Reached on the simple-protocol cacheable path, where ast.Normalize
// parameterizes the value of a gateway-managed set_config(..., true) call —
// the plan is cached by its normalized SQL, so every execution must
// re-resolve the value from that execution's literals.
func (s *ApplySessionState) executeSetWithNormalizedBinds(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return s.executeSetWithResolvedParams(ctx, conn, state, setConfigParamResolver{
		resolveBool: func(ref *ast.ParamRef, what string) (bool, error) {
			c, err := normalizedBindConst(bindVars, ref, what)
			if err != nil {
				return false, err
			}
			b, ok := c.Val.(*ast.Boolean)
			if !ok {
				return false, mterrors.NewFeatureNotSupported(fmt.Sprintf(
					"%s ($%d) must be a boolean literal", what, ref.Number))
			}
			return b.BoolVal, nil
		},
		resolveText: func(ref *ast.ParamRef, what string) (string, error) {
			c, err := normalizedBindConst(bindVars, ref, what)
			if err != nil {
				return "", err
			}
			return extractConstValue(c), nil
		},
	}, callback)
}

// executeSetWithResolvedParams owns the protocol-independent set_config bind
// flow: resolve is_local first because it decides whether tracking happens at
// all, resolve name next so the gateway-managed guard can run, and resolve the
// value only when a tracker write will actually fire.
func (s *ApplySessionState) executeSetWithResolvedParams(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	resolver setConfigParamResolver,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// is_local resolves from the bound value when present; otherwise it falls
	// back to the literal baked into the synthetic VariableStmt (true only for a
	// gateway-managed set_config(..., true) — see planner.syntheticSetStmt).
	isLocal := s.VariableStmt.IsLocal
	if s.BindRefs.IsLocalParam != nil {
		b, err := resolver.resolveBool(s.BindRefs.IsLocalParam, "set_config is_local argument")
		if err != nil {
			return err
		}
		isLocal = b
	}

	name := s.VariableStmt.Name
	if s.BindRefs.NameParam != nil {
		v, err := resolver.resolveText(s.BindRefs.NameParam, "set_config name argument")
		if err != nil {
			return err
		}
		name = v
	}

	// A transaction-scoped set_config of an ordinary (non-gateway-managed)
	// variable is owned by PostgreSQL via the trailing Route — the gateway
	// tracks nothing and can skip resolving the value. Gateway-managed
	// variables fall through so the transaction-local override is applied to
	// gateway state.
	if isLocal && !handler.IsGatewayManagedVariable(name) {
		return nil
	}

	value := extractVariableValue(s.VariableStmt.Args)
	if s.BindRefs.ValueParam != nil {
		v, err := resolver.resolveText(s.BindRefs.ValueParam, "set_config value argument")
		if err != nil {
			return err
		}
		value = v
	}

	return s.applyTracked(ctx, conn, state, name, value, isLocal, callback)
}

// normalizedBindConst returns the normalizer-extracted literal that ref
// points at. ParamRef numbers are 1-based positions into bindVars (the
// normalizer mints them from a single counter; ast.ReconstructSQL uses the
// same mapping for the Route's SQL reconstruction). An out-of-range
// reference means the statement carried a user-typed $N the normalizer
// never extracted a literal for — invalid in the simple protocol, so fail
// here, before any gateway state is written (the trailing Route would
// reject it anyway with "there is no parameter $N").
func normalizedBindConst(bindVars []*ast.A_Const, ref *ast.ParamRef, what string) (*ast.A_Const, error) {
	idx := ref.Number - 1 // ParamRef is 1-based
	if idx < 0 || idx >= len(bindVars) {
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf(
			"%s references parameter $%d but the statement carries %d normalized literal(s)",
			what, ref.Number, len(bindVars)))
	}
	c := bindVars[idx]
	if c == nil || c.Isnull {
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("%s ($%d) cannot be NULL", what, ref.Number))
	}
	return c, nil
}

// StreamExecute handles the SET/RESET command.
//
// bindVars are the literals the normalizer extracted on the simple-protocol
// cacheable path. A BindRefs primitive must resolve its bound slots from
// them: the cached plan was minted from the normalized AST, so its synthetic
// VariableStmt carries `__bind_$N__` placeholders — applying those (or any
// first-seen literal) would corrupt gateway state on every cache hit that
// carries different literals.
func (s *ApplySessionState) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_SET_VALUE:
		if s.BindRefs != nil {
			return s.executeSetWithNormalizedBinds(ctx, conn, state, bindVars, callback)
		}
		return s.executeSet(ctx, conn, state, callback)
	case ast.VAR_RESET, ast.VAR_RESET_ALL:
		return s.executeReset(ctx, state, callback)
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("SET/RESET kind %d is not supported", s.VariableStmt.Kind))
	}
}

// executeSet handles SET commands: update local state and return a synthetic
// response. The value is NOT validated against PostgreSQL — see the
// ApplySessionState doc comment.
//
// Two modes:
//   - SilentTracking: update state, no callback (a sibling primitive in a
//     Sequence will respond — used for SELECT set_config(...) plans).
//   - default: update state and emit CommandComplete "SET" (real SET stmt).
func (s *ApplySessionState) executeSet(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	value := extractVariableValue(s.VariableStmt.Args)
	return s.applyTracked(ctx, conn, state, s.VariableStmt.Name, value, s.VariableStmt.IsLocal, callback)
}

// applyTracked records a tracked SET / set_config into the right place:
// gateway-managed variables (e.g. statement_timeout) go to gateway-local
// state, everything else to SessionSettings. Routing GMVs away from
// SessionSettings keeps SHOW consistent and prevents the value from being
// replayed to a backend on pool rotation.
//
// A real `SET <gmv> = ...` is planned as GatewaySessionState, not this
// primitive, so the gateway-managed branch here is reached only via
// `SELECT set_config('<gmv>', ...)`.
//
// SET LOCAL of a gateway-managed variable issued outside a transaction is a
// no-op in PostgreSQL (the change is scoped to the implicit single-statement
// transaction). We mirror that and skip the gateway override, otherwise it
// would leak for the connection's lifetime — no COMMIT/ROLLBACK fires to clear
// it. This matches GatewaySessionState's SET LOCAL guard.
func (s *ApplySessionState) applyTracked(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	name, value string,
	isLocal bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	skipLeakyLocal := isLocal && !conn.IsInTransaction() && handler.IsGatewayManagedVariable(name)
	if !skipLeakyLocal {
		if handled, err := state.ApplyGatewayManagedVariable(name, value, isLocal); err != nil {
			return err
		} else if !handled {
			state.SetSessionVariable(name, value)
		}
	}

	if s.SilentTracking {
		return nil
	}

	return callback(ctx, &sqltypes.Result{
		CommandTag: "SET",
	})
}

// executeReset handles RESET/RESET ALL: update state, return synthetic response.
//
// Two modes (mirrors executeSet):
//   - SilentTracking: update state, no callback. No current planner path
//     produces a silent RESET, but gating defensively prevents a future
//     caller from emitting a stray CommandComplete("RESET") into the
//     protocol stream ahead of a sibling primitive's real response.
//   - default: update state and emit CommandComplete "RESET".
func (s *ApplySessionState) executeReset(
	ctx context.Context,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_RESET:
		// RESET variable
		state.ResetSessionVariable(s.VariableStmt.Name)

	case ast.VAR_RESET_ALL:
		state.ResetAllSessionVariables()
		// Also reset gateway-managed variables that live outside SessionSettings.
		state.ResetGatewayManagedVariables()
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("RESET kind %d is not supported", s.VariableStmt.Kind))
	}

	if s.SilentTracking {
		return nil
	}

	// Return synthetic CommandComplete
	return callback(ctx, &sqltypes.Result{
		CommandTag: "RESET",
	})
}

// GetTableGroup returns empty string — SET/RESET are local-only and don't target a tablegroup.
func (s *ApplySessionState) GetTableGroup() string {
	return ""
}

// GetQuery returns the original SQL string.
func (s *ApplySessionState) GetQuery() string {
	return s.Query
}

// String returns a string representation for debugging.
func (s *ApplySessionState) String() string {
	return fmt.Sprintf("ApplySessionState(%s)", s.VariableStmt.SqlString())
}

// extractVariableValue converts AST NodeList arguments to a string value.
func extractVariableValue(args *ast.NodeList) string {
	if args == nil || args.Len() == 0 {
		return ""
	}

	var values []string
	for _, arg := range args.Items {
		switch v := arg.(type) {
		case *ast.A_Const:
			values = append(values, extractConstValue(v))
		case *ast.String:
			values = append(values, v.SVal)
		case *ast.Integer:
			values = append(values, strconv.Itoa(v.IVal))
		default:
			values = append(values, arg.SqlString())
		}
	}

	return strings.Join(values, ", ")
}

// extractConstValue extracts string value from A_Const node.
func extractConstValue(aConst *ast.A_Const) string {
	if aConst == nil || aConst.Val == nil {
		return ""
	}

	switch val := aConst.Val.(type) {
	case *ast.String:
		return val.SVal
	case *ast.Integer:
		return strconv.Itoa(val.IVal)
	case *ast.Float:
		return val.FVal
	default:
		return aConst.SqlString()
	}
}

// Ensure ApplySessionState implements Primitive interface.
var _ Primitive = (*ApplySessionState)(nil)
