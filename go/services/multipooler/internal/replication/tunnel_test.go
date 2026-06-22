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
	"testing"
	"time"
)

func TestTunnel_CopiesBothDirections(t *testing.T) {
	backendA, backendB := net.Pipe() // backendB stands in for postgres
	t.Cleanup(func() { _ = backendA.Close(); _ = backendB.Close() })

	sendCh := make(chan []byte, 8) // server->client (from backend)
	recvCh := make(chan []byte, 8) // client->server (to backend)

	tun := NewTunnel(backendA, nil, // metrics nil = no-op
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
