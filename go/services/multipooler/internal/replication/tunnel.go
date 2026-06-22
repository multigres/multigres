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

// Package replication implements the protocol-blind byte tunnel that bridges a
// gRPC StreamReplication and a pinned postgres replication backend. The tunnel
// never interprets the replication sub-protocol; bytes flow verbatim in both
// directions.
package replication

import (
	"context"
	"errors"
	"io"
)

const tunnelBufSize = 64 * 1024

// SendFunc ships one opaque chunk toward the client (server->client).
type SendFunc func([]byte) error

// RecvFunc returns the next opaque chunk from the client (client->server),
// or io.EOF when the client half-closes.
type RecvFunc func() ([]byte, error)

// Tunnel copies opaque bytes between a postgres backend and a pair of
// send/recv callbacks over a gRPC stream. It is transport-agnostic so it can
// be unit-tested with in-memory pipes.
type Tunnel struct {
	backend io.ReadWriteCloser
	metrics *Stream // may be nil
	send    SendFunc
	recv    RecvFunc
}

// NewTunnel builds a tunnel over the given backend and stream callbacks. A nil
// metrics value disables metrics recording.
func NewTunnel(backend io.ReadWriteCloser, m *Stream, send SendFunc, recv RecvFunc) *Tunnel {
	return &Tunnel{backend: backend, metrics: m, send: send, recv: recv}
}

// Run blocks until either direction ends (EOF/error) or ctx is cancelled, then
// tears down the tunnel. Returns the first non-EOF error, if any.
//
// Teardown has a single lever: t.backend.Close() below, which unblocks the
// downstream goroutine's in-flight backend.Read. The upstream goroutine is not
// directly interruptible — it exits when t.recv() returns, so recv must honor
// the ctx passed in here (in the handler recv is stream.Recv(), unblocked when
// that gRPC stream context is cancelled or the RPC ends). Run does not wait for
// the goroutines (no WaitGroup); errc is buffered so a goroutine reporting its
// error after Run has returned never blocks on the send.
func (t *Tunnel) Run(ctx context.Context) error {
	errc := make(chan error, 2)

	// downstream: backend (postgres) -> client
	go func() {
		buf := make([]byte, tunnelBufSize) // reused; no per-chunk alloc
		for {
			n, rerr := t.backend.Read(buf)
			if n > 0 {
				if serr := t.send(buf[:n]); serr != nil {
					errc <- serr
					return
				}
				t.metrics.recordDownstream(n)
			}
			if rerr != nil {
				errc <- ignoreEOF(rerr)
				return
			}
		}
	}()

	// upstream: client -> backend (postgres)
	go func() {
		for {
			chunk, rerr := t.recv()
			if len(chunk) > 0 {
				if _, werr := t.backend.Write(chunk); werr != nil {
					errc <- werr
					return
				}
				t.metrics.recordUpstream(len(chunk))
			}
			if rerr != nil {
				errc <- ignoreEOF(rerr)
				return
			}
		}
	}()

	var first error
	select {
	case first = <-errc:
	case <-ctx.Done():
		first = ctx.Err()
	}
	_ = t.backend.Close() // the sole teardown lever: unblocks the downstream Read
	return first
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
