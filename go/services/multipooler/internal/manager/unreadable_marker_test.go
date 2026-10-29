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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/safefs"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// breakMarkerReads makes every read of the restore marker fail, the way an
// exhausted descriptor table or a refused anchor does, while the marker file
// itself stays on disk.
func breakMarkerReads(f *asyncRestoreFixture) {
	f.pm.volumeRoot = func() (*safefs.Root, error) { return nil, errors.New("anchor: EMFILE") }
}

// An unreadable marker is an unsettled restore: it ages toward
// quarantine like a readable one and never causes a delete or a start.
func TestMonitor_SidecarUnreadableMarker(t *testing.T) {
	setup := func(t *testing.T) (*asyncRestoreFixture, *fakeClock) {
		f := newAsyncRestoreFixtureWith(t, filepath.Join(t.TempDir(), "gone"))
		f.writeMarkerAndPGVersion(t)
		clock := newFakeClock()
		f.pm.nowFn = clock.now
		f.pm.unrecoverableTimeout = 5 * time.Minute
		f.pm.unrecoverableMinAttempts = 3
		breakMarkerReads(f)
		return f, clock
	}

	t.Run("the tick is not skipped and an old marker quarantines without a delete or a start", func(t *testing.T) {
		f, clock := setup(t)
		setMarkerTime(t, f, clock.now().Add(-restoreMarkerStaleAfter()-time.Minute))
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.Error(t, tick(t, f, ctx), "past the bound the unreadable marker is a failed recovery")
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.True(t, q)
		assert.True(t, f.hasPGVersion(), "never a delete")
		assert.True(t, f.hasSentinel())
		assert.Empty(t, f.mockPgctld.StartCalls, "never a start")
		assert.Zero(t, f.a.runCount())
	})

	t.Run("a young marker waits without counting", func(t *testing.T) {
		f, clock := setup(t)
		setMarkerTime(t, f, clock.now())
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.NoError(t, tick(t, f, ctx))
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.False(t, q)
		assert.True(t, f.hasPGVersion())
	})

	t.Run("no PG_VERSION converges the same way", func(t *testing.T) {
		f, clock := setup(t)
		require.NoError(t, os.Remove(filepath.Join(f.dataDir, "PG_VERSION")))
		f.mockPgctld.StatusResponse = &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_NOT_INITIALIZED}
		require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "partial"), []byte("x"), 0o644))
		setMarkerTime(t, f, clock.now().Add(-restoreMarkerStaleAfter()-time.Minute))
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.Error(t, tick(t, f, ctx))
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.True(t, q)
		assert.FileExists(t, filepath.Join(f.dataDir, "partial"), "never a delete")
		assert.Empty(t, f.mockPgctld.StartCalls)
		assert.Zero(t, f.a.runCount())
	})

	t.Run("without a listable mtime it ages from the first time it was seen unreadable", func(t *testing.T) {
		f, clock := setup(t)
		// The pooler directory cannot be listed either, so there is no mtime.
		require.NoError(t, os.RemoveAll(f.poolerDir))
		require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
		withLock(t, f.pm, func(ctx context.Context) {
			for range 3 {
				require.NoError(t, tick(t, f, ctx), "a marker first seen now is young")
				clock.advance(time.Minute)
			}
			q, _, _ := quarantineState(f.pm)
			require.False(t, q)
			clock.advance(restoreMarkerStaleAfter())
			for range 6 {
				require.Error(t, tick(t, f, ctx))
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.True(t, q, "a persistent read failure still ends in quarantine")
		assert.True(t, f.hasPGVersion())
		assert.Empty(t, f.mockPgctld.StartCalls)
	})

	t.Run("a running postgres keeps its tick and its remediation", func(t *testing.T) {
		f, _ := setup(t)
		pm := newRunningStandbyManagerForTest(t, &fakeRuleStore{pos: makeRulePosition(1)})
		pm.sidecar = f.pm.sidecar
		pm.volumeRoot = func() (*safefs.Root, error) { return nil, errors.New("anchor: EMFILE") }
		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err, "an unreadable marker must not skip the tick of a running postgres")
		assert.True(t, state.postgresRunning)
		assert.True(t, state.restoreSentinelPresent)
		assert.Equal(t, pgmode.InRecovery, state.pgMode, "the probes still ran")
	})
}

// The quarantine log names the cause.
func TestMonitor_SidecarQuarantineLogDistinguishesCause(t *testing.T) {
	run := func(t *testing.T, sock string, running bool) string {
		f := newAsyncRestoreFixtureWith(t, sock)
		if running {
			f.a.setStatus(true, false)
		}
		f.writeMarkerAndPGVersion(t)
		clock := newFakeClock()
		f.pm.nowFn = clock.now
		var logs bytes.Buffer
		f.pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
		setMarkerTime(t, f, clock.now().Add(-restoreMarkerStaleAfter()-time.Minute))
		withLock(t, f.pm, func(ctx context.Context) {
			require.Error(t, tick(t, f, ctx))
		})
		return logs.String()
	}
	down := run(t, filepath.Join(t.TempDir(), "gone"), false)
	assert.Contains(t, down, "the sidecar has been unreachable")
	assert.NotContains(t, down, "reports a restore running")

	busy := run(t, "", true)
	assert.Contains(t, busy, "the sidecar reports a restore running longer than a restore can take")
	assert.NotContains(t, busy, "unreachable")
}

// Removing the sentinel through its root syncs the pooler directory after the
// unlink, so a power loss cannot bring the marker back over a restored node.
func TestSentinelRemove_RootModeFsyncsPoolerDir(t *testing.T) {
	dir := t.TempDir()
	s := sentinel{path: filepath.Join(dir, constants.RestoreSentinelFile), name: constants.RestoreSentinelFile, contents: "x"}
	s.root = func() (*safefs.Root, error) { return safefs.OpenRoot(dir) }
	require.NoError(t, s.write())

	var synced []string
	testHookFsyncPath = func(p string) {
		_, statErr := os.Stat(s.path)
		assert.True(t, os.IsNotExist(statErr), "the sync comes after the unlink")
		synced = append(synced, p)
	}
	t.Cleanup(func() { testHookFsyncPath = nil })

	require.NoError(t, s.remove())
	assert.Equal(t, []string{dir}, synced)

	synced = nil
	require.NoError(t, s.remove(), "a missing sentinel is not an error")
	assert.Equal(t, []string{dir}, synced, "and the directory is still synced")
}
