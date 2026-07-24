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
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// Sequence executes multiple primitives in order.
// If any primitive fails, execution stops and returns the error.
// This enables composable execution plans (similar to PostgreSQL's Append node).
type Sequence struct {
	Primitives []Primitive
}

// NewSequence creates a new Sequence primitive.
func NewSequence(primitives []Primitive) *Sequence {
	return &Sequence{Primitives: primitives}
}

type silentTrackingAction struct {
	apply func()

	// previewPostSessionSettings mutates/returns a copy of the backend session
	// settings that should be recorded if the routed statement succeeds. It is
	// deliberately separate from apply: Sequence runs these previews before the
	// Route so the multipooler can receive post-query bookkeeping, but it runs
	// apply only after the Route succeeds so gateway state cannot drift from
	// PostgreSQL.
	previewPostSessionSettings func(map[string]string) map[string]string
}

type preparedSilentTrackingActions struct {
	actions                     map[int]silentTrackingAction
	hasPostQuerySessionSettings bool
	postQuerySessionSettings    map[string]string
}

type silentTrackingPreparer interface {
	prepareStreamSilentTrackingAction(*server.Conn, *handler.MultigatewayConnectionState, []*ast.A_Const) (silentTrackingAction, bool, error)
	preparePortalSilentTrackingAction(*server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo) (silentTrackingAction, bool, error)
}

func (s *Sequence) prepareStreamSilentTrackingActions(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
) (preparedSilentTrackingActions, error) {
	prepared := preparedSilentTrackingActions{actions: make(map[int]silentTrackingAction)}
	for i, p := range s.Primitives {
		preparer, ok := p.(silentTrackingPreparer)
		if !ok {
			continue
		}
		action, handled, err := preparer.prepareStreamSilentTrackingAction(conn, state, bindVars)
		if err != nil {
			return preparedSilentTrackingActions{}, fmt.Errorf("primitive %d (%s) failed: %w", i, p.String(), err)
		}
		if handled {
			prepared.actions[i] = action
			if action.previewPostSessionSettings != nil {
				if !prepared.hasPostQuerySessionSettings {
					prepared.postQuerySessionSettings = state.GetSessionSettings()
					prepared.hasPostQuerySessionSettings = true
				}
				prepared.postQuerySessionSettings = action.previewPostSessionSettings(prepared.postQuerySessionSettings)
			}
		}
	}
	return prepared, nil
}

func (s *Sequence) preparePortalSilentTrackingActions(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
) (preparedSilentTrackingActions, error) {
	prepared := preparedSilentTrackingActions{actions: make(map[int]silentTrackingAction)}
	for i, p := range s.Primitives {
		preparer, ok := p.(silentTrackingPreparer)
		if !ok {
			continue
		}
		action, handled, err := preparer.preparePortalSilentTrackingAction(conn, state, portalInfo)
		if err != nil {
			return preparedSilentTrackingActions{}, fmt.Errorf("primitive %d (%s) failed: %w", i, p.String(), err)
		}
		if handled {
			prepared.actions[i] = action
			if action.previewPostSessionSettings != nil {
				if !prepared.hasPostQuerySessionSettings {
					prepared.postQuerySessionSettings = state.GetSessionSettings()
					prepared.hasPostQuerySessionSettings = true
				}
				prepared.postQuerySessionSettings = action.previewPostSessionSettings(prepared.postQuerySessionSettings)
			}
		}
	}
	return prepared, nil
}

// StreamExecute executes each primitive in order, stopping on first error.
//
// bindVars are forwarded to every child. The current Sequence shape used by
// planSelectStmt is [Route, silent ApplySessionState...]: the Route uses
// bindVars + its NormalizedAST to reconstruct the original literals before
// sending the query to the backend; all-literal silent steps ignore bindVars
// (their VariableSetStmt is fully synthesized at plan time from the literal
// set_config args), and BindRefs steps resolve their bound set_config slots
// from bindVars (the normalizer parameterizes the value of a gateway-managed
// set_config(..., true) — see ApplySessionState.executeSetWithNormalizedBinds).
// Dropping bindVars here breaks both: the tracker would record a `__bind_$N__`
// placeholder, and the normalized `$N` placeholders would reach PG unbound and
// fail with "there is no parameter $N".
func (s *Sequence) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	prepared, err := s.prepareStreamSilentTrackingActions(conn, state, bindVars)
	if err != nil {
		return err
	}

	// exchange is created once and shared (by pointer) with every child, so an
	// earlier primitive can hand runtime data to a later sibling. It is scoped to
	// this execution — never the cached plan.
	exchange := &SequenceExchange{}

	// info is forwarded to every child; only the routing child (the leading
	// Route in planSelectStmt's Sequence) forwards it onward to IExecute. The
	// auxiliary children (silent ApplySessionState / ResolveTrackSetConfig
	// set_config steps) ignore it and
	// issue their own backend calls with the zero value, so the plan's
	// reservation directives apply exactly once, on the query that warrants them.
	postQueryInfoAttached := false
	for i, p := range s.Primitives {
		if action, ok := prepared.actions[i]; ok {
			if action.apply != nil {
				action.apply()
			}
			continue
		}
		childInfo := info
		childInfo.Exchange = exchange
		if prepared.hasPostQuerySessionSettings && !postQueryInfoAttached {
			childInfo.HasPostQuerySessionSettings = true
			childInfo.PostQuerySessionSettings = prepared.postQuerySessionSettings
			postQueryInfoAttached = true
		}
		if err := p.StreamExecute(ctx, exec, conn, state, bindVars, childInfo, callback); err != nil {
			return fmt.Errorf("primitive %d (%s) failed: %w", i, p.String(), err)
		}
	}
	return nil
}

// PortalStreamExecute runs each child's PortalStreamExecute in order. The
// dispatch lives on each child — the Route reissues the portal to the backend,
// while silent ApplySessionState steps that compose with it use portalInfo only
// to resolve bound set_config slots and update tracker state after success.
//
// Stops on the first error and reports which child failed.
func (s *Sequence) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	prepared, err := s.preparePortalSilentTrackingActions(conn, state, portalInfo)
	if err != nil {
		return err
	}

	// exchange is created once and shared (by pointer) with every child — see the
	// StreamExecute counterpart.
	exchange := &SequenceExchange{}

	postQueryInfoAttached := false
	for i, p := range s.Primitives {
		if action, ok := prepared.actions[i]; ok {
			if action.apply != nil {
				action.apply()
			}
			continue
		}
		childInfo := info
		childInfo.Exchange = exchange
		if prepared.hasPostQuerySessionSettings && !postQueryInfoAttached {
			childInfo.HasPostQuerySessionSettings = true
			childInfo.PostQuerySessionSettings = prepared.postQuerySessionSettings
			postQueryInfoAttached = true
		}
		if err := p.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, childInfo, callback); err != nil {
			return fmt.Errorf("primitive %d (%s) failed: %w", i, p.String(), err)
		}
	}
	return nil
}

// GetTableGroup returns the tablegroup from the first primitive that has one.
func (s *Sequence) GetTableGroup() string {
	// Return tablegroup from first primitive that has one
	for _, p := range s.Primitives {
		if tg := p.GetTableGroup(); tg != "" {
			return tg
		}
	}
	return ""
}

// GetQuery returns the query from the first primitive that has one.
func (s *Sequence) GetQuery() string {
	// Return query from first primitive that has one
	for _, p := range s.Primitives {
		if q := p.GetQuery(); q != "" {
			return q
		}
	}
	return ""
}

// String returns a string representation of the sequence for debugging.
func (s *Sequence) String() string {
	parts := make([]string, len(s.Primitives))
	for i, p := range s.Primitives {
		parts[i] = p.String()
	}
	return fmt.Sprintf("Sequence[%s]", strings.Join(parts, ", "))
}

// Ensure Sequence implements Primitive interface.
var _ Primitive = (*Sequence)(nil)
