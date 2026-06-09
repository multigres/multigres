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

// TestPlan_SessionAdvisoryLockPins verifies that statements acquiring a
// session-level advisory lock are dispatched through AdvisoryLockRoute so the
// backend is pinned, while transaction-level locks and unlock calls are not.
func TestPlan_SessionAdvisoryLockPins(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantPinned bool
	}{
		{name: "exclusive", sql: "SELECT pg_advisory_lock(101)", wantPinned: true},
		{name: "shared", sql: "SELECT pg_advisory_lock_shared(101)", wantPinned: true},
		{name: "try", sql: "SELECT pg_try_advisory_lock(101)", wantPinned: true},
		{name: "try_shared", sql: "SELECT pg_try_advisory_lock_shared(101)", wantPinned: true},
		{name: "two_key", sql: "SELECT pg_advisory_lock(7, 9)", wantPinned: true},
		{name: "schema_qualified", sql: "SELECT pg_catalog.pg_advisory_lock(101)", wantPinned: true},
		{name: "inside_larger_select", sql: "SELECT pg_advisory_lock(101), 42", wantPinned: true},

		// Transaction-level locks are out of scope — released at transaction end.
		{name: "xact_lock", sql: "SELECT pg_advisory_xact_lock(101)", wantPinned: false},
		{name: "xact_shared", sql: "SELECT pg_advisory_xact_lock_shared(101)", wantPinned: false},

		// Unlock calls do not acquire anything, so they must not pin.
		{name: "unlock", sql: "SELECT pg_advisory_unlock(101)", wantPinned: false},
		{name: "unlock_all", sql: "SELECT pg_advisory_unlock_all()", wantPinned: false},

		// Unrelated queries are unaffected.
		{name: "plain_select", sql: "SELECT 1", wantPinned: false},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			_, isAdvisory := plan.Primitive.(*engine.AdvisoryLockRoute)
			if tt.wantPinned {
				assert.True(t, isAdvisory, "expected AdvisoryLockRoute, got %T", plan.Primitive)
				assert.Equal(t, engine.PlanTypeAdvisoryLockRoute, plan.Type,
					"plan.Type must be set for observability")
			} else {
				assert.False(t, isAdvisory, "did not expect AdvisoryLockRoute for %q", tt.sql)
			}
		})
	}
}

// TestPlan_AdvisoryLockComposesWithSetConfig verifies that a statement which
// both acquires a session-level advisory lock and tracks a set_config keeps
// both behaviors: the plan is a Sequence with the set_config ApplySessionState
// primitives AND a trailing AdvisoryLockRoute that pins the backend.
func TestPlan_AdvisoryLockComposesWithSetConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "SELECT set_config('search_path', 'myschema', false), pg_advisory_lock(101)"
	plan, err := p.Plan(sql, parseOne(t, sql), conn)
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

	// The trailing route must be an AdvisoryLockRoute so the backend is pinned.
	last := seq.Primitives[len(seq.Primitives)-1]
	_, isAdvisory := last.(*engine.AdvisoryLockRoute)
	assert.True(t, isAdvisory, "trailing route must be AdvisoryLockRoute, got %T", last)
}
