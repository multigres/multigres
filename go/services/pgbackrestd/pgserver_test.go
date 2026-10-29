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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeServerBin installs a script as the pgbackrest binary. Each run appends
// a line to the returned log: the argv, then the child's environment.
func fakeServerBin(t *testing.T, s *Server, body string) (runLog string) {
	t.Helper()
	dir := t.TempDir()
	runLog = filepath.Join(dir, "runs")
	script := "#!/bin/sh\necho \"$@\" >> " + runLog + "\nenv > " + runLog + ".env\n" + body + "\n"
	bin := filepath.Join(dir, "pgbackrest")
	require.NoError(t, os.WriteFile(bin, []byte(script), 0o700))
	s.bin = bin
	return runLog
}

func countRuns(t *testing.T, runLog string) int {
	t.Helper()
	b, err := os.ReadFile(runLog)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return strings.Count(string(b), "\n")
}

func fastServer(s *Server) {
	s.pg.minBackoff = time.Millisecond
	s.pg.maxBackoff = 5 * time.Millisecond
	s.pg.upGrace = 20 * time.Millisecond
}

func TestPgServer_RestartsForever(t *testing.T) {
	s := newTestServer(t, nil)
	fastServer(s)
	runLog := fakeServerBin(t, s, "exit 1")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx))

	require.Eventually(t, func() bool { return s.pg.restarts.Load() > 5 }, 10*time.Second, 5*time.Millisecond)
	assert.Greater(t, countRuns(t, runLog), 5)
	assert.False(t, s.serverUp())
}

func TestPgServer_FailsClosedWithoutCerts(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.TLSCertDir = filepath.Join(t.TempDir(), "missing") })
	fastServer(s)
	runLog := fakeServerBin(t, s, "sleep 300")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx), "a missing cert dir must not stop the sidecar")

	require.Eventually(t, func() bool { return s.pg.restarts.Load() > 3 }, 10*time.Second, 5*time.Millisecond)
	assert.Zero(t, countRuns(t, runLog), "the server must never start without certificates")
	assert.False(t, s.serverUp())

	// Pausing a server that cannot start must still return.
	done := make(chan struct{})
	go func() { s.pauseServer()(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pauseServer did not return while the server cannot start")
	}
}

func TestPgServer_PauseSuppressesRestart(t *testing.T) {
	s := newTestServer(t, nil)
	fastServer(s)
	runLog := fakeServerBin(t, s, "exec sleep 300")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx))
	require.Eventually(t, func() bool { return s.serverUp() && countRuns(t, runLog) == 1 }, 10*time.Second, 5*time.Millisecond)

	resume := s.pauseServer()
	assert.False(t, s.serverUp(), "pause returns only once the child has exited")
	nested := s.pauseServer()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 1, countRuns(t, runLog), "no restart while paused")

	resume()
	resume() // idempotent
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, countRuns(t, runLog), "still paused until every pause is resumed")

	nested()
	require.Eventually(t, func() bool { return s.serverUp() && countRuns(t, runLog) == 2 }, 10*time.Second, 5*time.Millisecond)
}

func TestPgServer_PauseDuringBackoff(t *testing.T) {
	s := newTestServer(t, nil)
	s.pg.minBackoff = 30 * time.Second
	s.pg.maxBackoff = time.Minute
	runLog := fakeServerBin(t, s, "exit 1")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx))
	require.Eventually(t, func() bool { return s.pg.restarts.Load() == 1 }, 10*time.Second, 5*time.Millisecond)

	done := make(chan func())
	go func() { done <- s.pauseServer() }()
	select {
	case resume := <-done:
		resume()
	case <-time.After(5 * time.Second):
		t.Fatal("pauseServer blocked on the restart backoff")
	}
	assert.Equal(t, 1, countRuns(t, runLog))
}

func TestPgServer_StopsWithContext(t *testing.T) {
	s := newTestServer(t, nil)
	fastServer(s)
	fakeServerBin(t, s, "exec sleep 300")
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, s.startPgServer(ctx))
	require.Eventually(t, s.serverUp, 10*time.Second, 5*time.Millisecond)

	cancel()
	require.Eventually(t, func() bool { return !s.serverUp() }, 10*time.Second, 5*time.Millisecond)
	// A pause after shutdown has nothing to wait for.
	s.pauseServer()()
}

func TestPgServer_NoTokenInEnv(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "from-pid1")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/token")
	s := newTestServer(t, nil)
	fastServer(s)
	runLog := fakeServerBin(t, s, "exec sleep 300")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx))
	require.Eventually(t, func() bool { return countRuns(t, runLog) >= 1 }, 10*time.Second, 5*time.Millisecond)

	var env []byte
	require.Eventually(t, func() bool {
		var err error
		env, err = os.ReadFile(runLog + ".env")
		return err == nil && len(env) > 0
	}, 10*time.Second, 5*time.Millisecond)
	text := string(env)
	assert.NotContains(t, text, "PGBACKRESTD_CREDS_TOKEN")
	assert.NotContains(t, text, s.credsToken())
	assert.NotContains(t, text, "AWS_")
	assert.Contains(t, text, "PGBACKREST_CONFIG="+s.cfg.stateSub(dirConf+"/"+serverConfName))

	args, err := os.ReadFile(runLog)
	require.NoError(t, err)
	assert.NotContains(t, string(args), "--stanza")
	assert.NotContains(t, string(args), "--lock-path")
}

func TestPgServer_ConcurrentPauseResume(t *testing.T) {
	s := newTestServer(t, nil)
	fastServer(s)
	fakeServerBin(t, s, "exec sleep 300")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, s.startPgServer(ctx))

	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 5 {
				s.pauseServer()()
			}
		}()
	}
	for range 8 {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("pause/resume deadlocked")
		}
	}
	require.Eventually(t, s.serverUp, 10*time.Second, 5*time.Millisecond)
}

func TestPgServer_BackoffCapsAndResets(t *testing.T) {
	s := newTestServer(t, nil)
	s.pg.minBackoff = time.Second
	s.pg.maxBackoff = 4 * time.Second
	s.pg.stableRun = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var mu sync.Mutex
	var slept []time.Duration
	s.pg.sleep = func(ctx context.Context, d time.Duration) bool {
		mu.Lock()
		defer mu.Unlock()
		slept = append(slept, d)
		if len(slept) == 7 {
			cancel()
		}
		return ctx.Err() == nil
	}
	// Run 6 outlives stableRun, so the sleep after it starts from the minimum.
	runLog := fakeServerBin(t, s, "")
	script := "#!/bin/sh\necho x >> " + runLog + "\nn=$(wc -l < " + runLog + " | tr -d ' ')\n[ \"$n\" -eq 6 ] && sleep 1\nexit 1\n"
	require.NoError(t, os.WriteFile(s.bin, []byte(script), 0o700))

	require.NoError(t, s.startPgServer(ctx))
	s.stopBackground()

	mu.Lock()
	defer mu.Unlock()
	sec := time.Second
	assert.Equal(t, []time.Duration{1 * sec, 2 * sec, 4 * sec, 4 * sec, 4 * sec, 1 * sec, 2 * sec}, slept)
}

func TestRun_StopsServerChildBeforeReturning(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgbr-run")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := newTestServer(t, func(c *Config) {
		c.WALSocket = filepath.Join(dir, "wal.sock")
		c.ControlSocket = filepath.Join(dir, "control.sock")
		// creds.sock lives under the state dir, and a t.TempDir() named after
		// this test overruns macOS's 104-byte unix socket path limit.
		c.StateDir = filepath.Join(dir, "state")
	})
	fastServer(s)
	pidFile := filepath.Join(dir, "server.pid")
	fakeServerBin(t, s, `case "$*" in *server*) echo $$ > `+pidFile+`; exec sleep 300;; esac`)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	var pid int
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(b)))
		return err == nil
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, syscall.Kill(pid, 0))

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	assert.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the server child outlived Run")
}

func TestPgServer_RegistersMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	s := newTestServer(t, nil)
	s.registerPgServerMetrics()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, name := range []string{"pgbackrest.server.up", "pgbackrest.server.restart_count", "pgbackrest.server.uptime"} {
		assert.True(t, got[name], name)
	}
}
