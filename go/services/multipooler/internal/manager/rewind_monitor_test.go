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

package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// A rewind that failed while its outcome could not be confirmed leaves the node
// down with suspectedDivergence already set. The monitor must settle the attempt
// before it starts postgres on the directory, not skip that because the flag is
// set.
func TestMonitor_UnsettledRewindAttemptWithDivergenceSet(t *testing.T) {
	setup := func(t *testing.T) (*MultipoolerManager, *mockPgctldClient) {
		t.Helper()
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
		}
		pm.pgctldClient = client
		withLock(t, pm, func(ctx context.Context) {
			_, err := pm.consensusMgr.SetSuspectedDivergence(ctx, true)
			require.NoError(t, err)
		})
		return pm, client
	}

	t.Run("discovery sees the attempt marker and the monitor settles it before any start", func(t *testing.T) {
		pm, _ := setup(t)
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttemptFor("run-1").write())

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		require.True(t, state.rewindAttemptPresent)
		assert.Equal(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(t.Context(), state))
	})

	t.Run("without an attempt marker a held start is unchanged", func(t *testing.T) {
		pm, _ := setup(t)
		require.NoError(t, pm.rewindSentinel().write())

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		require.False(t, state.rewindAttemptPresent)
		assert.NotEqual(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(t.Context(), state))
	})

	t.Run("pgctld unreachable at the failure, then back: the attempt is settled before the start", func(t *testing.T) {
		pm, client := setup(t)
		client.pgRewindResponse = &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"}
		client.pgRewindError = nil
		// The mutating call fails and pgctld cannot be asked what became of it.
		client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) {
			if client.pgRewindCalls == 2 {
				client.pgRewindError = errors.New("connection reset")
			}
		}
		client.pgRewindAbortError = status.Error(codes.Unavailable, "pgctld unreachable")
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "cannot confirm what happened to the pg_rewind run")
		requirePresent(t, pm.rewindAttempt(), true, "kept")

		// Next tick, pgctld is reachable but still cannot say: no start, nothing armed.
		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		require.Equal(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(t.Context(), state))
		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, state))
		})
		assert.False(t, client.startCalled, "postgres must not start on an unsettled directory")
		requirePresent(t, pm.rewindAttempt(), true, "still kept")

		// pgctld answers and the directory is untouched: the attempt is settled and the
		// ordinary held start is next, with divergence still suspected.
		client.pgRewindAbortError = nil
		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, state))
		})
		requirePresent(t, pm.rewindAttempt(), false, "settled")
		assert.True(t, pm.consensusMgr.SuspectedDivergence())
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined)
		state, err = pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.NotEqual(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(t.Context(), state))
	})

	t.Run("a modified directory is quarantined instead of started", func(t *testing.T) {
		pm, client := setup(t)
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttemptFor("run-1").write())
		client.pgRewindAbortOnCall = func(*pgctldpb.PgRewindAbortRequest) {
			require.NoError(t, os.WriteFile(filepath.Join(postgresDataDir(), "base/1/16384"), []byte("late write"), 0o600))
		}

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.True(t, quarantined)
		assert.False(t, client.startCalled)
	})
}
