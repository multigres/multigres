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

// TestPlan_DeclareCursorWithHold dispatches DECLARE … WITH HOLD through
// HoldCursorRoute so the backend gets pinned with ReasonPortal.
func TestPlan_DeclareCursorWithHold(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "DECLARE c CURSOR WITH HOLD FOR SELECT 1"
	plan, err := p.Plan(sql, parseOne(t, sql), conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	route, ok := plan.Primitive.(*engine.HoldCursorRoute)
	require.True(t, ok, "expected HoldCursorRoute, got %T", plan.Primitive)
	assert.Equal(t, "c", route.CursorName)
	assert.Equal(t, sql, route.Query)
	assert.Equal(t, engine.PlanTypeHoldCursorRoute, plan.Type,
		"plan.Type must be set so observability spans + query logs surface it correctly")
}

// TestPlan_DeclareCursorWithoutHold falls through to planDefault so the
// transaction-scoped cursor is handled by the existing Route primitive.
func TestPlan_DeclareCursorWithoutHold(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "DECLARE c CURSOR FOR SELECT 1"
	plan, err := p.Plan(sql, parseOne(t, sql), conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	_, isHold := plan.Primitive.(*engine.HoldCursorRoute)
	assert.False(t, isHold, "non-HOLD DECLARE must not dispatch to HoldCursorRoute")
	_, isRoute := plan.Primitive.(*engine.Route)
	assert.True(t, isRoute, "non-HOLD DECLARE must fall through to planDefault → Route")
}

// TestPlan_ClosePortal covers both CLOSE <name> and CLOSE ALL — they must
// reach CloseCursorRoute so the multipooler-side pin set is drained.
func TestPlan_ClosePortal(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		closeAll bool
		cursor   string
	}{
		{name: "CLOSE named", sql: "CLOSE c1", closeAll: false, cursor: "c1"},
		{name: "CLOSE ALL", sql: "CLOSE ALL", closeAll: true, cursor: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			route, ok := plan.Primitive.(*engine.CloseCursorRoute)
			require.True(t, ok, "expected CloseCursorRoute, got %T", plan.Primitive)
			assert.Equal(t, tt.closeAll, route.CloseAll)
			assert.Equal(t, tt.cursor, route.CursorName)
			assert.Equal(t, engine.PlanTypeCloseCursorRoute, plan.Type)
		})
	}
}

// TestPlan_DiscardAll routes DISCARD ALL through DiscardAllPrimitive, which
// resets the gateway's session state and releases any reserved connection
// without forwarding the statement to a shared pooled backend.
func TestPlan_DiscardAll(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "DISCARD ALL"
	plan, err := p.Plan(sql, parseOne(t, sql), conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	prim, ok := plan.Primitive.(*engine.DiscardAllPrimitive)
	require.True(t, ok, "DISCARD ALL must route through DiscardAllPrimitive, got %T", plan.Primitive)
	assert.Equal(t, "DISCARD ALL", prim.Query)
	assert.Equal(t, engine.PlanTypeDiscardAll, plan.Type)
}

// TestPlan_DiscardPlansSequences keeps the existing planDefault behavior for
// DISCARD variants that don't touch cursors. Guards against accidentally
// over-broadening the DISCARD-ALL dispatch.
func TestPlan_DiscardPlansSequences(t *testing.T) {
	for _, sql := range []string{"DISCARD PLANS", "DISCARD SEQUENCES"} {
		t.Run(sql, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(sql, parseOne(t, sql), conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			_, isClose := plan.Primitive.(*engine.CloseCursorRoute)
			assert.False(t, isClose, "%s must not route through CloseCursorRoute", sql)
		})
	}
}
