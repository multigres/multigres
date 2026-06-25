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

package server

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
)

// newDetachTestListener builds a minimal *Listener with the object pools
// initialized, which is all newConn/Close/DetachConn need for buffer
// teardown. It does not open a network socket.
func newDetachTestListener(t *testing.T) *Listener {
	t.Helper()
	return &Listener{
		logger: testLogger(t),
		readersPool: &sync.Pool{
			New: func() any { return bufio.NewReaderSize(nil, connBufferSize) },
		},
		writersPool: &sync.Pool{
			New: func() any { return bufio.NewWriterSize(nil, connBufferSize) },
		},
		bufPool: bufpool.New(16*1024, 64*1024*1024),
	}
}

func TestConn_DetachConn(t *testing.T) {
	listener := newDetachTestListener(t)

	// serverEnd is owned by the Conn; clientEnd is the peer we use to feed
	// read-ahead bytes and to verify the detached socket stays alive.
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	c := newConn(serverEnd, listener, 1)
	// Lazily create the writer (like real serve() does via startWriterBuffering)
	// so DetachConn must flush and return it to the pool.
	c.startWriterBuffering()

	// Prime buffered read-ahead: the peer writes a few bytes, and we force
	// the Conn's bufferedReader to read them into its buffer via Peek.
	readAhead := []byte("hello-readahead")
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		_, _ = clientEnd.Write(readAhead)
	}()

	// Peek into the bufferedReader so the bytes are buffered (not yet consumed
	// by protocol decoding). Peek blocks until at least len(readAhead) bytes
	// are available.
	peeked, err := c.bufferedReader.Peek(len(readAhead))
	require.NoError(t, err)
	require.Equal(t, readAhead, peeked)
	<-writeDone
	require.Equal(t, len(readAhead), c.bufferedReader.Buffered(),
		"read-ahead bytes should be sitting in the bufferedReader")

	// Detach: should return the buffered bytes and the raw socket.
	raw, buffered, err := c.DetachConn()
	require.NoError(t, err)
	require.NotNil(t, raw, "raw socket must be handed off")
	assert.Equal(t, readAhead, buffered, "buffered read-ahead must be returned to the caller")
	assert.True(t, c.closed.Load(), "Conn must be marked closed after detach")

	// A second DetachConn must fail (Conn is no longer usable).
	raw2, buffered2, err := c.DetachConn()
	require.Error(t, err)
	assert.Nil(t, raw2)
	assert.Nil(t, buffered2)

	// Close() must succeed and must NOT close the hijacked socket. It also
	// must not double-return pooled buffers (would panic / corrupt the pool).
	require.NoError(t, c.Close())

	// Verify raw is still alive: a write from the peer must be readable on raw.
	payload := []byte("after-detach")
	peerWriteDone := make(chan struct{})
	go func() {
		defer close(peerWriteDone)
		_, _ = clientEnd.Write(payload)
	}()

	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, len(payload))
	n, err := raw.Read(got)
	require.NoError(t, err, "detached raw socket must still be readable after Close()")
	assert.Equal(t, payload, got[:n])
	<-peerWriteDone

	// And raw must still be writable.
	peerReadDone := make(chan struct{})
	go func() {
		defer close(peerReadDone)
		_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 4)
		_, _ = clientEnd.Read(buf)
	}()
	_ = raw.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = raw.Write([]byte("ping"))
	require.NoError(t, err, "detached raw socket must still be writable after Close()")
	<-peerReadDone

	_ = raw.Close()
}

// TestConn_DetachConn_ZeroizesSCRAMKeys verifies DetachConn applies the same
// credential hygiene as Close(): the SCRAM passthrough keys are wiped and
// nilled, even though the later Close() short-circuits on the closed flag.
func TestConn_DetachConn_ZeroizesSCRAMKeys(t *testing.T) {
	listener := newDetachTestListener(t)
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	c := newConn(serverEnd, listener, 1)
	c.scramClientKey = []byte("client-key-secret")
	c.scramServerKey = []byte("server-key-secret")

	raw, _, err := c.DetachConn()
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	assert.Nil(t, c.scramClientKey, "scramClientKey must be nilled after detach")
	assert.Nil(t, c.scramServerKey, "scramServerKey must be nilled after detach")
}

// TestConn_DetachConn_NoBuffered verifies DetachConn works when nothing has
// been read ahead: buffered must be empty and the socket still handed off.
func TestConn_DetachConn_NoBuffered(t *testing.T) {
	listener := newDetachTestListener(t)
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	c := newConn(serverEnd, listener, 1)

	raw, buffered, err := c.DetachConn()
	require.NoError(t, err)
	require.NotNil(t, raw)
	assert.Empty(t, buffered)

	require.NoError(t, c.Close())
	_ = raw.Close()
}
