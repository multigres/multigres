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

package client

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
)

func TestDetachConn_ReturnsSocketAndBufferedBytes(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	c := &Conn{ctx: ctx, cancel: cancel}
	c.resetConn(client)

	// Server writes 5 bytes; force the bufio.Reader to buffer them via Peek.
	go func() { _, _ = server.Write([]byte("hello")) }()
	if _, err := c.bufferedReader.Peek(5); err != nil {
		t.Fatalf("peek: %v", err)
	}

	raw, buffered, err := c.DetachConn()
	if err != nil {
		t.Fatalf("DetachConn: %v", err)
	}
	// DetachConn must cancel the connection's context: Close/ForceClose
	// short-circuit on the closed flag and never reach c.cancel(), so the child
	// context would otherwise leak in the pool context tree.
	select {
	case <-ctx.Done():
	default:
		t.Fatal("DetachConn must cancel the connection context")
	}
	if got := string(buffered); got != "hello" {
		t.Fatalf("buffered = %q, want %q", got, "hello")
	}
	if raw != client {
		t.Fatalf("raw conn is not the underlying socket")
	}
	// After detach, Close() must NOT close the raw socket (caller owns it now).
	if err := c.Close(); err != nil {
		t.Fatalf("Close after detach: %v", err)
	}
	// net.Pipe is synchronous, so write in a goroutine and read on the server
	// side: a closed socket would fail the write immediately, an open one
	// hands the byte through.
	go func() { _, _ = raw.Write([]byte("x")) }()
	got := make([]byte, 1)
	if _, err := io.ReadFull(server, got); err != nil || string(got) != "x" {
		t.Fatalf("raw socket should still be usable after Conn.Close: got %q, err %v", got, err)
	}
}

func TestDetachConn_ClosedConnReturnsError(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := &Conn{ctx: t.Context()}
	c.resetConn(client)
	c.closed.Store(true) // already closed: detach must refuse

	if _, _, err := c.DetachConn(); err == nil {
		t.Fatal("DetachConn on a closed connection must return an error")
	}
}

// TestDetachConn_ConcurrentForceCloseIsRaceFree reproduces the drain-vs-detach
// race haritabh flagged: a graceful drain force-closing a pooled conn
// (ForceClose) can run concurrently with the replication handler's DetachConn on
// the same *Conn. They must be mutually exclusive — no data race on c.conn, no
// nil-deref panic. Run with -race.
func TestDetachConn_ConcurrentForceCloseIsRaceFree(t *testing.T) {
	for range 100 {
		client, server := net.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		c := &Conn{ctx: ctx, cancel: cancel}
		c.resetConn(client)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _, _ = c.DetachConn() }()
		go func() { defer wg.Done(); _ = c.ForceClose() }()
		wg.Wait()

		_ = client.Close()
		_ = server.Close()
	}
}

func TestDetachConn_NoBufferedBytes(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	c := &Conn{ctx: ctx, cancel: cancel}
	c.resetConn(client)

	// No Peek beforehand, so the read buffer is empty.
	raw, buffered, err := c.DetachConn()
	if err != nil {
		t.Fatalf("DetachConn: %v", err)
	}
	if len(buffered) != 0 {
		t.Fatalf("buffered = %q, want empty", buffered)
	}
	if raw != client {
		t.Fatal("raw conn is not the underlying socket")
	}
	if ctx.Err() == nil {
		t.Fatal("DetachConn must cancel the connection context")
	}
}
