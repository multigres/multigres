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

package replication

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeMetrics is a tiny TunnelMetrics that records per-direction byte totals,
// exercising the non-nil metrics path.
type fakeMetrics struct {
	down atomic.Int64
	up   atomic.Int64
}

func (f *fakeMetrics) RecordDownstream(n int) { f.down.Add(int64(n)) }
func (f *fakeMetrics) RecordUpstream(n int)   { f.up.Add(int64(n)) }

func TestTunnel_CopiesBothDirections(t *testing.T) {
	backendA, backendB := net.Pipe() // backendB stands in for postgres
	t.Cleanup(func() { _ = backendA.Close(); _ = backendB.Close() })

	sendCh := make(chan []byte, 8) // server->client (from backend)
	recvCh := make(chan []byte, 8) // client->server (to backend)

	fm := &fakeMetrics{}
	tun := NewTunnel(backendA, fm,
		func(b []byte) error { sendCh <- append([]byte(nil), b...); return nil },
		func() ([]byte, error) {
			b, ok := <-recvCh
			if !ok {
				return nil, io.EOF
			}
			return b, nil
		},
	)
	go func() { _ = tun.Run(t.Context()) }()

	// client -> backend
	recvCh <- []byte("ping")
	got := make([]byte, 4)
	if _, err := io.ReadFull(backendB, got); err != nil || string(got) != "ping" {
		t.Fatalf("backend recv = %q, %v", got, err)
	}
	// backend -> client
	if _, err := backendB.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-sendCh:
		if string(b) != "pong" {
			t.Fatalf("send = %q", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server->client byte")
	}

	// Each direction records its metric in the copy goroutine just after the
	// transfer completes, and Run() intentionally does not wait for those
	// goroutines (no WaitGroup — see Run's doc). Reading the counters once would
	// race the RecordUpstream/RecordDownstream call, so poll until both
	// directions are observed (4 bytes each).
	deadline := time.After(2 * time.Second)
	for fm.up.Load() != 4 || fm.down.Load() != 4 {
		select {
		case <-deadline:
			t.Fatalf("metrics not recorded: up=%d down=%d, want 4/4", fm.up.Load(), fm.down.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestTunnel_BackendCloseTearsDown verifies that when the postgres side of the
// backend closes, Run observes EOF on the downstream Read and returns cleanly.
func TestTunnel_BackendCloseTearsDown(t *testing.T) {
	backendA, backendB := net.Pipe()
	t.Cleanup(func() { _ = backendA.Close() })

	recvCh := make(chan []byte)
	tun := NewTunnel(backendA, nil,
		func(b []byte) error { return nil },
		func() ([]byte, error) {
			b, ok := <-recvCh
			if !ok {
				return nil, io.EOF
			}
			return b, nil
		},
	)
	done := make(chan error, 1)
	go func() { done <- tun.Run(t.Context()) }()

	// Close the postgres side; the downstream Read sees EOF and tears down.
	_ = backendB.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on clean backend close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after backend close")
	}
	close(recvCh) // release the (possibly blocked) upstream goroutine
}

// TestTunnel_RecvErrorTearsDown verifies that a non-EOF recv error propagates
// out of Run and tears down the downstream direction.
func TestTunnel_RecvErrorTearsDown(t *testing.T) {
	backendA, backendB := net.Pipe()
	t.Cleanup(func() { _ = backendA.Close(); _ = backendB.Close() })

	wantErr := errors.New("recv boom")
	tun := NewTunnel(backendA, nil,
		func(b []byte) error { return nil },
		func() ([]byte, error) { return nil, wantErr },
	)
	done := make(chan error, 1)
	go func() { done <- tun.Run(t.Context()) }()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run err = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after recv error")
	}
}

// TestTunnel_SendErrorTearsDown verifies that a failing send (the downstream
// receiver is gone) propagates out of Run and tears the tunnel down — the
// symmetric counterpart to the recv-error case.
func TestTunnel_SendErrorTearsDown(t *testing.T) {
	backendA, backendB := net.Pipe()
	t.Cleanup(func() { _ = backendA.Close(); _ = backendB.Close() })

	// recv blocks until teardown; closed in cleanup to release the goroutine.
	recvCh := make(chan []byte)
	t.Cleanup(func() { close(recvCh) })

	wantErr := errors.New("send boom")
	tun := NewTunnel(backendA, nil,
		func([]byte) error { return wantErr }, // receiver is down
		func() ([]byte, error) {
			<-recvCh
			return nil, io.EOF
		},
	)
	done := make(chan error, 1)
	go func() { done <- tun.Run(t.Context()) }()

	// Backend (postgres) produces a byte to forward downstream; the send fails.
	go func() { _, _ = backendB.Write([]byte("x")) }()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run err = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after send error")
	}
}

// errWriteBackend errors on Write and blocks Read until Close, so the upstream
// (client -> backend) copy fails on its first write.
type errWriteBackend struct {
	wErr      error
	readBlock chan struct{}
	closeOnce sync.Once
}

func (b *errWriteBackend) Read([]byte) (int, error)  { <-b.readBlock; return 0, io.EOF }
func (b *errWriteBackend) Write([]byte) (int, error) { return 0, b.wErr }
func (b *errWriteBackend) Close() error {
	b.closeOnce.Do(func() { close(b.readBlock) })
	return nil
}

// finiteReadBackend returns one byte and a nil error on each of its first max
// calls (signaling on reads before returning, so a test can observe exactly
// when a Read happens), then io.EOF.
type finiteReadBackend struct {
	reads chan struct{}
	max   int
	n     int
}

func (b *finiteReadBackend) Read(p []byte) (int, error) {
	b.reads <- struct{}{}
	b.n++
	if b.n > b.max {
		return 0, io.EOF
	}
	p[0] = 'x'
	return 1, nil
}

func (b *finiteReadBackend) Write(p []byte) (int, error) { return len(p), nil }
func (b *finiteReadBackend) Close() error                { return nil }

// TestTunnel_DownstreamBackpressure verifies the mechanism the tunnel relies on
// for backpressure: the downstream loop is strictly synchronous, so a slow
// send (e.g. a gRPC stream to a lagging client) blocks the next backend.Read
// rather than letting the tunnel buffer ahead. This is what keeps a slow
// logical-replication consumer from making the pooler or gateway accumulate an
// unbounded backlog of WAL data in memory.
func TestTunnel_DownstreamBackpressure(t *testing.T) {
	backend := &finiteReadBackend{reads: make(chan struct{}, 8), max: 2}

	gate := make(chan struct{})
	var sendCalls atomic.Int32
	send := func([]byte) error {
		sendCalls.Add(1)
		<-gate
		return nil
	}
	stopRecv := make(chan struct{})
	t.Cleanup(func() { close(stopRecv) })
	recv := func() ([]byte, error) {
		<-stopRecv
		return nil, io.EOF
	}

	tun := NewTunnel(backend, nil, send, recv)
	done := make(chan error, 1)
	go func() { done <- tun.Run(t.Context()) }()

	select {
	case <-backend.reads:
	case <-time.After(2 * time.Second):
		t.Fatal("backend.Read was never called")
	}

	// The downstream loop is now blocked inside send() waiting on gate. If the
	// tunnel buffered ahead instead of applying backpressure, a second
	// backend.Read would fire here even though send() has not returned.
	select {
	case <-backend.reads:
		t.Fatal("backend.Read fired again before the in-flight send() returned; tunnel is not backpressured")
	case <-time.After(50 * time.Millisecond):
	}
	if got := sendCalls.Load(); got != 1 {
		t.Fatalf("sendCalls = %d, want 1 (second read must wait for first send to return)", got)
	}

	// Release the in-flight send; the loop should proceed to a second read.
	close(gate)
	select {
	case <-backend.reads:
	case <-time.After(2 * time.Second):
		t.Fatal("second backend.Read never happened after send() unblocked")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after backend EOF")
	}
}

// TestTunnel_BackendWriteErrorTearsDown covers the upstream write-error path: a
// client->backend chunk whose backend Write fails ends the tunnel with that error.
func TestTunnel_BackendWriteErrorTearsDown(t *testing.T) {
	wantErr := errors.New("write boom")
	backend := &errWriteBackend{wErr: wantErr, readBlock: make(chan struct{})}

	recvCh := make(chan []byte, 1)
	recvCh <- []byte("upstream") // one chunk to push into the failing Write

	tun := NewTunnel(backend, nil,
		func([]byte) error { return nil },
		func() ([]byte, error) {
			b, ok := <-recvCh
			if !ok {
				return nil, io.EOF
			}
			return b, nil
		},
	)
	done := make(chan error, 1)
	go func() { done <- tun.Run(t.Context()) }()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run err = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after backend write error")
	}
}
