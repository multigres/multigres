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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

func TestNeverSpawnedMarkersRetired(t *testing.T) {
	for _, supervisor := range []bool{true, false} {
		t.Run(map[bool]string{true: "supervisor", false: "child"}[supervisor], func(t *testing.T) {
			s := newTestServer(t, nil)
			if supervisor {
				s.self = filepath.Join(t.TempDir(), "missing-supervisor")
			}
			child := exec.Command(filepath.Join(t.TempDir(), "missing-child"))
			_, err := s.runOwned(t.Context(), child, io.Discard, io.Discard, KindS3Command, cmdInfo)
			require.Error(t, err)
			require.NoError(t, s.Close())
			markers, err := filepath.Glob(s.cfg.stateSub("worker-*.pending"))
			require.NoError(t, err)
			require.Empty(t, markers)
			next, err := newForTest(s.cfg)
			require.NoError(t, err)
			require.NoError(t, next.Close())
			// In contrast a record with unknown ownership must still veto restart.
			require.NoError(t, privateWrite(s.cfg.stateSub("worker-unknown.pending"), []byte("unknown")))
			_, err = newForTest(s.cfg)
			require.Error(t, err)
		})
	}
}

type enteredRawSink struct {
	io.Writer
	once    sync.Once
	entered chan struct{}
}

func (s *enteredRawSink) Write(b []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	return s.Writer.Write(b)
}

func TestRawServerBackpressureDoesNotOwnPause(t *testing.T) {
	s := newTestServer(t, nil)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	entered := make(chan struct{})
	sink := newRawOutput(&enteredRawSink{Writer: w, entered: entered})
	s.pg.rawOut, s.pg.rawErr = sink, sink
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		r.Close()
		w.Close()
		s.stopBackground()
		close(sink.queue)
		<-sink.done
	})
	// Real unread OS pipe, larger than its capacity; no simulated blocking Write.
	fakeServerBin(t, s, "while :; do printf 'raw server output raw server output raw server output\\n'; done")
	require.NoError(t, s.startPgServer(ctx))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("server never wrote")
	}
	require.Eventually(t, func() bool { return sink.dropped.Load() > 0 }, 5*time.Second, time.Millisecond)
	paused := make(chan struct{})
	go func() { s.pauseServer(); close(paused) }()
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("raw logging owns restore pause")
	}
	require.NoError(t, s.Close(), "shutdown must settle with raw sink still blocked")
	require.LessOrEqual(t, len(sink.queue), rawOutputQueue)
}

func TestRawOutputReportsLoss(t *testing.T) {
	var out bytes.Buffer
	sink := newRawOutput(&out)
	sink.dropped.Store(123)
	_, err := sink.Write([]byte("remaining log\n"))
	require.NoError(t, err)
	close(sink.queue)
	<-sink.done
	require.Contains(t, out.String(), "123 bytes dropped")
	require.Contains(t, out.String(), "remaining log")
}

func TestShutdownNotServingAndControlOnly(t *testing.T) {
	h := newControlHarness(t, nil)
	_, err := h.client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: h.s.incarnation}))
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.s.serveMu.Lock()
	h.s.serveCancel = cancel
	h.s.serveMu.Unlock()
	for range 2 {
		resp, err := h.client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: h.s.incarnation}))
		require.NoError(t, err)
		require.True(t, resp.Msg.Accepted)
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	mux := http.NewServeMux()
	h.s.registerWAL(mux)
	req, _ := http.NewRequest(http.MethodPost, "http://unix"+pgbackrestdserviceconnect.PgBackRestdShutdownProcedure, nil)
	_, pattern := mux.Handler(req)
	require.Empty(t, pattern, "shutdown is not exposed on WAL socket")
}

func TestShutdownServingInstance(t *testing.T)            { testShutdownServingInstance(t, false) }
func TestShutdownPropagatesUnsettledWorkers(t *testing.T) { testShutdownServingInstance(t, true) }
func testShutdownServingInstance(t *testing.T, unsettled bool) {
	newFakeBackrest(t, nil, false)
	dir, err := os.MkdirTemp("/tmp", "sc-stop-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := newTestServer(t, func(c *Config) {
		c.StateDir = filepath.Join(dir, "state")
		c.WALSocket = filepath.Join(dir, "wal")
		c.ControlSocket = filepath.Join(dir, "ctl")
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", s.cfg.ControlSocket)
	}}
	defer transport.CloseIdleConnections()
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: transport, Timeout: 2 * time.Second}, "http://unix")
	require.Eventually(t, func() bool { s.serveMu.Lock(); defer s.serveMu.Unlock(); return s.serveCancel != nil }, 5*time.Second, time.Millisecond)
	status, err := client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	require.Equal(t, s.incarnation, status.Msg.InstanceId)
	require.Equal(t, uint32(1), status.Msg.LifecycleProtocolVersion)
	for _, id := range []string{"", "stale-instance"} {
		_, err = client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: id}))
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		select {
		case err := <-done:
			t.Fatalf("mismatch stopped server: %v", err)
		default:
		}
	}
	if unsettled {
		failed := &ownedWorker{done: make(chan struct{}), err: errors.New("injected unproven worker")}
		close(failed.done)
		s.workersMu.Lock()
		if s.workers == nil {
			s.workers = map[*ownedWorker]struct{}{}
		}
		s.workers[failed] = struct{}{}
		s.workersMu.Unlock()
	}
	response, err := client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: s.incarnation}))
	require.NoError(t, err, "graceful HTTP shutdown must deliver response")
	require.True(t, response.Msg.Accepted)
	select {
	case err := <-done:
		if unsettled {
			require.ErrorContains(t, err, "unproven worker")
		} else {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	release, ok := s.gate.TryEnter()
	if release != nil {
		release()
	}
	require.False(t, ok)
	if unsettled {
		_, err := newForTest(s.cfg)
		require.ErrorContains(t, err, "still own")
		require.ErrorContains(t, s.Close(), "unproven worker")
		// The injected entry has no real process. Release its intentionally held
		// test-only lock after proving failed Close/Run never release it themselves.
		s.instanceLock.Close()
		if s.anchor != nil {
			s.anchor.Close()
		}
	} else {
		next, err := newForTest(s.cfg)
		require.NoError(t, err)
		require.NoError(t, next.Close())
	}
}
