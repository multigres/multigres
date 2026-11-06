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

package pgbackrestd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartup_RunsPgbackrestStart(t *testing.T) {
	s := newTestServer(t, nil)
	runLog := fakeServerBin(t, s, "exit 0")

	require.NoError(t, s.runStartupDuties(t.Context()))

	args, err := os.ReadFile(runLog)
	require.NoError(t, err)
	assert.Contains(t, string(args), "--stanza=multigres")
	assert.Contains(t, string(args), "--config="+s.cfg.stateSub(fileCommandConf))
	assert.Contains(t, string(args), " start\n")
}

func TestStartup_FailedStartDoesNotStopTheSidecar(t *testing.T) {
	s := newTestServer(t, nil)
	fakeServerBin(t, s, "echo boom >&2; exit 32")
	require.NoError(t, s.runStartupDuties(t.Context()))
}

func TestStartup_BoundedWhenChildHangs(t *testing.T) {
	s := newTestServer(t, nil)
	fakeServerBin(t, s, "exec sleep 300")
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	require.NoError(t, s.runStartupDuties(ctx))
	assert.Less(t, time.Since(start), 10*time.Second)
}

func TestStartup_ClearsTmp(t *testing.T) {
	s := newTestServer(t, nil)
	fakeServerBin(t, s, "exit 0")
	tmp := s.cfg.stateSub(dirTmp)
	require.NoError(t, os.MkdirAll(filepath.Join(tmp, "left", "over"), 0o700))
	victim := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(victim, "keep"), nil, 0o600))
	require.NoError(t, os.Symlink(victim, filepath.Join(tmp, "planted")))

	require.NoError(t, s.runStartupDuties(t.Context()))

	entries, err := os.ReadDir(tmp)
	require.NoError(t, err)
	assert.Empty(t, entries)
	fi, err := os.Stat(tmp)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
	_, err = os.Stat(filepath.Join(victim, "keep"))
	require.NoError(t, err, "a planted link must not be followed")
}

func TestStartup_UnreadableStateDirFails(t *testing.T) {
	s := newTestServer(t, nil)
	require.NoError(t, os.RemoveAll(s.cfg.StateDir))
	require.Error(t, s.runStartupDuties(t.Context()))
}

// instantSleep makes the retry loop spin without waiting, calling onSleep
// before each attempt.
func instantSleep(onSleep func(n int)) func(context.Context, time.Duration) bool {
	var n int
	return func(ctx context.Context, _ time.Duration) bool {
		n++
		if onSleep != nil {
			onSleep(n)
		}
		return ctx.Err() == nil
	}
}

func TestStartup_RetriesFailedStartUntilItSucceeds(t *testing.T) {
	s := newTestServer(t, nil)
	s.pg.sleep = instantSleep(nil)
	runLog := fakeServerBin(t, s, `[ $(wc -l < `+"RUNLOG"+` | tr -d ' ') -ge 3 ] || exit 32`)
	script, err := os.ReadFile(s.bin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(s.bin, []byte(strings.ReplaceAll(string(script), "RUNLOG", runLog)), 0o700))

	require.NoError(t, s.runStartupDuties(t.Context()))
	s.stopBackground() // returns once the retry has succeeded

	assert.Equal(t, 3, countRuns(t, runLog), "boot attempt plus two retries, none after success")
}

func TestStartup_RetryWaitsOutAClosedGate(t *testing.T) {
	s := newTestServer(t, nil)
	runLog := fakeServerBin(t, s, "exit 32")
	script, err := os.ReadFile(s.bin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(s.bin, []byte(strings.ReplaceAll(string(script), "exit 32", `[ $(wc -l < `+runLog+` | tr -d ' ') -ge 2 ] || exit 32`)), 0o700))

	reopen, err := s.gate.Close(t.Context(), time.Second)
	require.NoError(t, err)
	var runsWhileClosed int
	s.pg.sleep = instantSleep(func(n int) {
		if n == 20 {
			runsWhileClosed = countRuns(t, runLog)
			reopen()
		}
	})

	require.NoError(t, s.runStartupDuties(t.Context()))
	s.stopBackground()

	assert.Equal(t, 1, runsWhileClosed, "only the boot attempt ran while a restore held the gate")
	assert.Equal(t, 2, countRuns(t, runLog))
}

func TestStartup_RetryStopsWithContext(t *testing.T) {
	s := newTestServer(t, nil)
	s.pg.startRetryMin = time.Hour
	fakeServerBin(t, s, "exit 32")
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, s.runStartupDuties(ctx))

	cancel()
	done := make(chan struct{})
	go func() { s.stopBackground(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("start retry outlived its context")
	}
}
