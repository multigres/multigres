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

package switcher

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// fakeToggleable is a test double recording Open/Close call counts alongside
// its current state, so tests can assert both "is it open now" and "how many
// times did we flap it."
type fakeToggleable struct {
	mu     sync.Mutex
	open   bool
	opens  int
	closes int
}

func (f *fakeToggleable) Open() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = true
	f.opens++
}

func (f *fakeToggleable) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = false
	f.closes++
}

func (f *fakeToggleable) IsOpen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open
}

func writableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}}
}

func notWritableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}}
}

func TestRoleSwitcher_WritableOpensPrimary(t *testing.T) {
	primary, secondary := &fakeToggleable{}, &fakeToggleable{}
	p := NewRoleSwitcher(primary, secondary)

	require.NoError(t, p.OnStateChange(t.Context(), writableState()))

	assert.True(t, primary.IsOpen())
	assert.False(t, secondary.IsOpen())
}

func TestRoleSwitcher_NotWritableOpensSecondary(t *testing.T) {
	primary, secondary := &fakeToggleable{}, &fakeToggleable{}
	p := NewRoleSwitcher(primary, secondary)

	require.NoError(t, p.OnStateChange(t.Context(), notWritableState()))

	assert.False(t, primary.IsOpen())
	assert.True(t, secondary.IsOpen())
}

// TestRoleSwitcher_IdempotentNoFlap verifies repeated OnStateChange notifications
// with the same writability don't needlessly close and reopen the running
// component. StateManager re-notifies on every fanout (including rule-only
// bumps that don't change writability), so without this the active role
// would flap on every reconciliation tick.
func TestRoleSwitcher_IdempotentNoFlap(t *testing.T) {
	primary, secondary := &fakeToggleable{}, &fakeToggleable{}
	p := NewRoleSwitcher(primary, secondary)

	require.NoError(t, p.OnStateChange(t.Context(), writableState()))
	require.NoError(t, p.OnStateChange(t.Context(), writableState()))

	assert.Equal(t, 1, primary.opens, "primary should only open once across two identical notifications")
	assert.Equal(t, 0, secondary.opens, "secondary was never active, so it should never have been opened")
}

func TestRoleSwitcher_SwitchesRolesOnTransition(t *testing.T) {
	primary, secondary := &fakeToggleable{}, &fakeToggleable{}
	p := NewRoleSwitcher(primary, secondary)

	require.NoError(t, p.OnStateChange(t.Context(), notWritableState()))
	assert.True(t, secondary.IsOpen())

	require.NoError(t, p.OnStateChange(t.Context(), writableState()))
	assert.True(t, primary.IsOpen())
	assert.False(t, secondary.IsOpen())
}

// TestRoleSwitcher_CloseStopsBothRegardlessOfActiveRole is the
// regression-shape test motivating this whole package: Close must stop both
// components regardless of which was last active. The bug this generalizes
// (heartbeat.ReplTracker hand-rolling this and leaving the reader running
// past a real shutdown) was actually one level up — GracefulShutdown never
// called Close at all — but RoleSwitcher.Close itself must still hold this
// guarantee unconditionally, for any consumer that does call it.
func TestRoleSwitcher_CloseStopsBothRegardlessOfActiveRole(t *testing.T) {
	tests := []struct {
		name      string
		lastState servingstate.State
	}{
		{"last state was writable (primary active)", writableState()},
		{"last state was not writable (secondary active)", notWritableState()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary, secondary := &fakeToggleable{}, &fakeToggleable{}
			p := NewRoleSwitcher(primary, secondary)

			require.NoError(t, p.OnStateChange(t.Context(), tt.lastState))
			p.Close()

			assert.False(t, primary.IsOpen(), "primary must be closed regardless of which role was active")
			assert.False(t, secondary.IsOpen(), "secondary must be closed regardless of which role was active")
		})
	}
}

func TestNoOp_IsAlwaysClosedAndSafeToCall(t *testing.T) {
	var n NoOp
	assert.False(t, n.IsOpen())
	assert.NotPanics(t, func() {
		n.Open()
		n.Close()
	})
}
