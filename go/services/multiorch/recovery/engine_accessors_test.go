// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// TestEngine_GetDetectedProblems_DelegatesToTracker covers the thin
// GetDetectedProblems/GetProblemStates accessors, which have no test of
// their own elsewhere (only the tracker they delegate to is tested directly).
func TestEngine_GetDetectedProblems_DelegatesToTracker(t *testing.T) {
	engine := &Engine{problems: newProblemTracker()}
	p := testProblem("ReplicaLagging", "FixReplication")

	engine.problems.reconcile([]types.Problem{p})

	active := engine.GetDetectedProblems()
	require.Len(t, active, 1)
	require.Equal(t, p.Code, active[0].Code)

	states := engine.GetProblemStates()
	require.Len(t, states, 1)
	require.Equal(t, p.Code, states[0].LastKnown.Code)
}

// TestEngine_GetActionAttemptHistory_DelegatesToTracker covers the thin
// GetActionAttemptHistory accessor.
func TestEngine_GetActionAttemptHistory_DelegatesToTracker(t *testing.T) {
	engine := &Engine{problems: newProblemTracker()}
	p := testProblem("LeaderUnhealthy", "AppointLeader")

	_, ok := engine.GetActionAttemptHistory("AppointLeader", p.EntityID())
	require.False(t, ok, "no attempt recorded yet")

	engine.problems.recordAttemptStart(p, time.Now())
	hist, ok := engine.GetActionAttemptHistory("AppointLeader", p.EntityID())
	require.True(t, ok)
	require.Equal(t, 1, hist.TotalAttempts)
}

// TestEngine_NextEligibleAttempt_DelegatesToReadyToExecute covers the thin
// NextEligibleAttempt accessor for a non-failover problem (no grace period),
// which must report ready immediately.
func TestEngine_NextEligibleAttempt_DelegatesToReadyToExecute(t *testing.T) {
	engine := &Engine{
		recoveryGracePeriodTracker: NewRecoveryGracePeriodTracker(t.Context(), nil),
	}
	p := testProblem("ReplicaLagging", "FixReplication")

	readyAt, ready := engine.NextEligibleAttempt(p)
	require.True(t, ready, "an action with no grace period must be immediately ready")
	require.True(t, readyAt.IsZero())
}
