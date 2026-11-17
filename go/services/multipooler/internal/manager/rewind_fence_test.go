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

func attemptRunID(t *testing.T, pm *MultipoolerManager) string {
	t.Helper()
	id, err := pm.rewindAttemptRunID()
	require.NoError(t, err)
	return id
}

// The pooler fences the run id through pgctld before it judges the data
// directory, because a request pgctld has not handled yet leaves no running record
// to wait for.
func TestRewindRunFence(t *testing.T) {
	overwrite := func(t *testing.T) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(postgresDataDir(), "base/1/16384"), []byte("late write"), 0o600))
	}

	t.Run("the mutating request carries the run id that the attempt marker records", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
		}
		var mutating *pgctldpb.PgRewindRequest
		var markedID string
		client.pgRewindOnCall = func(req *pgctldpb.PgRewindRequest) {
			if !req.GetDryRun() {
				mutating = req
				markedID, _ = pm.rewindAttemptRunID()
			}
		}
		pm.pgctldClient = client

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		require.NotNil(t, mutating)
		assert.NotEmpty(t, mutating.GetRunId())
		assert.Equal(t, mutating.GetRunId(), markedID, "the marker was written before the call, with the same id")
		assert.Empty(t, client.pgRewindReqs[0].GetRunId(), "the dry-run is not a run to fence")
		assert.Empty(t, client.pgRewindAbortReqs, "a successful run needs no fence")
	})

	t.Run("a failed run is fenced before the directory is judged", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, errors.New("connection reset"))
		// A request pgctld handled late: its write lands after the failure but before
		// the fence returns, so only a scan that follows the fence sees it.
		client.pgRewindAbortOnCall = func(*pgctldpb.PgRewindAbortRequest) { overwrite(t) }

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "did not complete")
		require.Len(t, client.pgRewindAbortReqs, 1)
		assert.Equal(t, client.pgRewindReqs[1].GetRunId(), client.pgRewindAbortReqs[0].GetRunId(), "the fenced id is the run's")
		requirePresent(t, pm.rewindTainted(), true, "the late write is attributed to the run")
	})

	t.Run("an untouched directory is still retryable after the fence", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, errors.New("source demoted while planning"))

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "source demoted while planning")
		require.Len(t, client.pgRewindAbortReqs, 1)
		requirePresent(t, pm.rewindTainted(), false, "")
		requirePresent(t, pm.rewindAttempt(), false, "settled")
	})

	t.Run("pgctld cannot be asked: the directory is not judged and the marker is kept", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, errors.New("connection reset"))
		client.pgRewindAbortError = status.Error(codes.Unavailable, "pgctld unreachable")
		// Even a modified directory is not judged on a guess.
		client.pgRewindAbortOnCall = func(*pgctldpb.PgRewindAbortRequest) { overwrite(t) }

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "cannot confirm what happened to the pg_rewind run")
		require.ErrorContains(t, err, "connection reset", "the original failure is kept")
		requirePresent(t, pm.rewindTainted(), false, "no verdict without an answer")
		requirePresent(t, pm.rewindAttempt(), true, "kept so the next settle can ask again")
		assert.NotEmpty(t, attemptRunID(t, pm))

		// pgctld is back: the next settle fences the same run and judges it.
		client.pgRewindAbortError = nil
		withLock(t, pm, func(ctx context.Context) {
			tainted, err := pm.settleRewindAttempt(ctx)
			require.NoError(t, err)
			assert.True(t, tainted)
		})
		require.Len(t, client.pgRewindAbortReqs, 2)
		assert.Equal(t, client.pgRewindAbortReqs[0].GetRunId(), client.pgRewindAbortReqs[1].GetRunId())
	})

	t.Run("a pgctld without the RPC cannot confirm the run stopped, so the directory is treated as modified", func(t *testing.T) {
		// An older pgctld neither fences nor writes the running record, so an
		// untouched-looking directory proves nothing: a late write could still land.
		pm, client := rewindRunSetup(t, nil, errors.New("connection reset"))
		client.pgRewindAbortError = status.Error(codes.Unimplemented, "unknown method PgRewindAbort")

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "did not complete")
		requirePresent(t, pm.rewindTainted(), true, "no running record exists to say the run stopped")
		requirePresent(t, pm.rewindAttempt(), true, "kept: the run may still be writing and can delete the verdict")
	})

	t.Run("a marker without a run id is judged without a fence", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{}
		pm.pgctldClient = client
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))
		assert.Empty(t, attemptRunID(t, pm))

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
		assert.Empty(t, client.pgRewindAbortReqs)
	})

	t.Run("settling without a pgctld client is not an error", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		pm.pgctldClient = nil
		require.NoError(t, pm.rewindAttemptFor("run-1").write())

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.False(t, tainted)
	})

	t.Run("the run id is read back from the marker", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		assert.Empty(t, attemptRunID(t, pm), "no marker")

		require.NoError(t, pm.rewindAttemptFor("run-1").write())
		assert.Equal(t, "run-1", attemptRunID(t, pm))
	})

	t.Run("a marker that cannot be read is not read as having no run id", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{}
		pm.pgctldClient = client
		// A directory where the marker goes: present, but not readable as a file.
		blockSentinel(t, pm.rewindAttempt())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))

		_, err := pm.rewindAttemptRunID()
		require.Error(t, err)

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.ErrorContains(t, err, "cannot read the run id from the rewind attempt marker")
		assert.False(t, tainted, "nothing is judged on a marker that cannot be read")
		assert.Empty(t, client.pgRewindAbortReqs)
		requirePresent(t, pm.rewindTainted(), false, "")
		requirePresent(t, pm.rewindAttempt(), true, "kept so the next settle can retry")
	})

	t.Run("the monitor fences an interrupted run before it judges it", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{}
		pm.pgctldClient = client
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttemptFor("run-1").write())
		client.pgRewindAbortOnCall = func(*pgctldpb.PgRewindAbortRequest) { overwrite(t) }

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		require.Len(t, client.pgRewindAbortReqs, 1)
		assert.Equal(t, "run-1", client.pgRewindAbortReqs[0].GetRunId())
		quarantined, _, _ := quarantineState(pm)
		assert.True(t, quarantined, "the write the fence let through is a modification")
	})

	t.Run("the monitor retries later when pgctld cannot confirm the run", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{pgRewindAbortError: status.Error(codes.Unavailable, "pgctld unreachable")}
		pm.pgctldClient = client
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttemptFor("run-1").write())

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined)
		assert.False(t, pm.consensusMgr.SuspectedDivergence(), "nothing is armed until the run is confirmed")
		requirePresent(t, pm.rewindAttempt(), true, "")
	})
}
