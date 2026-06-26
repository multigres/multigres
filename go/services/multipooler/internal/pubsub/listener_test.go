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

package pubsub

import (
	"context"
	"log/slog"
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// running reports whether the listener's background event loop is active. Start
// sets cancel; Stop (which waits for the goroutine to exit) clears it.
func running(l *Listener) bool { return l.cancel != nil }

// newTestListener builds a Listener with no pool manager. The dedicated PG
// connection is acquired lazily on the first subscribe, so a listener that is
// only started/stopped (no subscribers) never touches the pool — which lets us
// exercise the OnStateChange gating without a real postgres.
func newTestListener(t *testing.T) *Listener {
	t.Helper()
	metrics, err := NewPubSubMetrics()
	require.NoError(t, err)
	l := NewListener(nil, slog.Default(), metrics)
	t.Cleanup(l.Stop)
	return l
}

// TestListenerOnStateChangeGating verifies the LISTEN/NOTIFY listener runs only
// when this pooler is the consensus leader AND postgres is out of recovery AND
// serving. postgresPrimary is the key gate: LISTEN/NOTIFY only delivers on the
// actual postgres primary, so a leader whose postgres is still in recovery must
// not run the listener.
func TestListenerOnStateChangeGating(t *testing.T) {
	tests := []struct {
		name              string
		isConsensusLeader bool
		postgresPrimary   bool
		servingStatus     clustermetadatapb.PoolerServingStatus
		wantRunning       bool
	}{
		{
			name:              "leader, writable, serving -> listener runs",
			isConsensusLeader: true,
			postgresPrimary:   true,
			servingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
			wantRunning:       true,
		},
		{
			name:              "leader but not yet writable -> listener stays off",
			isConsensusLeader: true,
			postgresPrimary:   false,
			servingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
			wantRunning:       false,
		},
		{
			// A non-leader is never writable (Writable encodes committed
			// leadership), so the not-writable gate keeps the listener off.
			name:              "not leader, not writable -> listener stays off",
			isConsensusLeader: false,
			postgresPrimary:   false,
			servingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
			wantRunning:       false,
		},
		{
			name:              "leader and writable but draining -> listener stays off",
			isConsensusLeader: true,
			postgresPrimary:   true,
			servingStatus:     clustermetadatapb.PoolerServingStatus_DRAINING,
			wantRunning:       false,
		},
		{
			name:              "leader and writable but disabled -> listener stays off",
			isConsensusLeader: true,
			postgresPrimary:   true,
			servingStatus:     clustermetadatapb.PoolerServingStatus_DISABLED,
			wantRunning:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newTestListener(t)
			err := l.OnStateChange(context.Background(), servingstate.State{IsHighestKnownLeader: tt.isConsensusLeader, Writable: tt.postgresPrimary, ServingStatus: tt.servingStatus})
			require.NoError(t, err)
			assert.Equal(t, tt.wantRunning, running(l))
		})
	}
}

// TestListenerOnStateChangeStopsOnDemotion verifies that a running listener is
// stopped when the node loses writability (e.g. demoted from leader to standby),
// which is the failure this gating guards against.
func TestListenerOnStateChangeStopsOnDemotion(t *testing.T) {
	l := newTestListener(t)

	require.NoError(t, l.OnStateChange(context.Background(), servingstate.State{IsHighestKnownLeader: true, Writable: true, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	require.True(t, running(l), "listener should run while leader+writable+serving")

	// Postgres falls back into recovery (demoted to standby) while still nominally
	// the leader: the listener must stop.
	require.NoError(t, l.OnStateChange(context.Background(), servingstate.State{IsHighestKnownLeader: true, Writable: false, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	assert.False(t, running(l), "listener must stop once postgres is no longer writable")
}
