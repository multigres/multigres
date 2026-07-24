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

// TestPlan_SessionAdvisoryLockRouting verifies how statements that touch
// session-level advisory locks are routed. Both acquires and releases set the
// plan's ExecInfo.RecheckAdvisoryLocks (so the multipooler re-probes pg_locks
// afterward), but only acquires set ExecInfo.AdvisoryLock, the pin intent that
// reserves the backend. Transaction-level locks and unrelated queries carry no
// advisory ExecInfo.
func TestPlan_SessionAdvisoryLockRouting(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantAdvisory bool // ExecInfo.RecheckAdvisoryLocks (acquire or release)
		wantPin      bool // ExecInfo.AdvisoryLock — reserves the backend (acquire only)
	}{
		{name: "exclusive", sql: "SELECT pg_advisory_lock(101)", wantAdvisory: true, wantPin: true},
		{name: "shared", sql: "SELECT pg_advisory_lock_shared(101)", wantAdvisory: true, wantPin: true},
		{name: "try", sql: "SELECT pg_try_advisory_lock(101)", wantAdvisory: true, wantPin: true},
		{name: "try_shared", sql: "SELECT pg_try_advisory_lock_shared(101)", wantAdvisory: true, wantPin: true},
		{name: "two_key", sql: "SELECT pg_advisory_lock(7, 9)", wantAdvisory: true, wantPin: true},
		{name: "schema_qualified", sql: "SELECT pg_catalog.pg_advisory_lock(101)", wantAdvisory: true, wantPin: true},
		{name: "inside_larger_select", sql: "SELECT pg_advisory_lock(101), 42", wantAdvisory: true, wantPin: true},

		// Releases route through AdvisoryLockRoute for the recheck, but do not pin.
		{name: "unlock", sql: "SELECT pg_advisory_unlock(101)", wantAdvisory: true, wantPin: false},
		{name: "unlock_shared", sql: "SELECT pg_advisory_unlock_shared(101)", wantAdvisory: true, wantPin: false},
		{name: "unlock_all", sql: "SELECT pg_advisory_unlock_all()", wantAdvisory: true, wantPin: false},

		// Transaction-level locks are out of scope — released at transaction end.
		{name: "xact_lock", sql: "SELECT pg_advisory_xact_lock(101)", wantAdvisory: false, wantPin: false},
		{name: "xact_unlock", sql: "SELECT pg_advisory_xact_lock_shared(101)", wantAdvisory: false, wantPin: false},

		// Unrelated queries are unaffected.
		{name: "plain_select", sql: "SELECT 1", wantAdvisory: false, wantPin: false},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn, PlanOptions{})
			require.NoError(t, err)
			require.NotNil(t, plan)

			// Routing is a plain Route; advisory intent rides on plan.ExecInfo.
			_, isRoute := plan.Primitive.(*engine.Route)
			require.True(t, isRoute, "expected a plain Route, got %T", plan.Primitive)
			assert.Equal(t, tt.wantAdvisory, plan.ExecInfo.RecheckAdvisoryLocks, "recheck intent for %q", tt.sql)
			assert.Equal(t, tt.wantPin, plan.ExecInfo.AdvisoryLock, "pin intent for %q", tt.sql)
			if tt.wantAdvisory {
				assert.Equal(t, engine.PlanTypeAdvisoryLockRoute, plan.Type,
					"plan.Type must reflect advisory routing for observability")
			} else {
				assert.Equal(t, engine.PlanTypeRoute, plan.Type)
			}
		})
	}
}

// TestPlan_AdvisoryLockComposesWithSetConfig verifies that a statement which
// both acquires a session-level advisory lock and tracks a set_config keeps
// both behaviors: the plan is a Sequence with a leading Route that pins the
// backend and trailing set_config ApplySessionState primitives.
func TestPlan_AdvisoryLockComposesWithSetConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "SELECT set_config('search_path', 'myschema', false), pg_advisory_lock(101)"
	plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.NotEmpty(t, seq.Primitives)

	// The set_config tracking must still be planned.
	var hasApplySessionState bool
	for _, prim := range seq.Primitives {
		if _, isApply := prim.(*engine.ApplySessionState); isApply {
			hasApplySessionState = true
		}
	}
	assert.True(t, hasApplySessionState, "set_config must still be tracked via ApplySessionState")

	// The leading primitive is a plain Route; the advisory pin now rides on the
	// plan's ExecInfo, which the Sequence forwards to that Route at exec time.
	first := seq.Primitives[0]
	_, isRoute := first.(*engine.Route)
	assert.True(t, isRoute, "leading primitive must be a Route, got %T", first)
	assert.True(t, plan.ExecInfo.AdvisoryLock, "plan must carry the advisory-lock pin intent")
	assert.True(t, plan.ExecInfo.RecheckAdvisoryLocks, "plan must request the advisory recheck")
}
