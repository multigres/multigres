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
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

// socketPair returns the client and server ends of a real socket on network
// ("tcp" or "unix"), so CheckIdle can inspect a kernel receive buffer.
func socketPair(t *testing.T, network string) (clientSide, serverSide net.Conn) {
	t.Helper()
	addr := "127.0.0.1:0"
	if network == "unix" {
		// t.TempDir can exceed the unix socket path limit on macOS.
		dir, err := os.MkdirTemp("", "idlecheck")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		addr = filepath.Join(dir, "s")
	}
	ln, err := net.Listen(network, addr)
	require.NoError(t, err)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	clientSide, err = net.Dial(network, ln.Addr().String())
	require.NoError(t, err)
	serverSide = <-accepted
	require.NotNil(t, serverSide)
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	return clientSide, serverSide
}

func TestCheckIdle(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			t.Run("open and quiet", func(t *testing.T) {
				clientSide, _ := socketPair(t, network)
				c := newTestConnFromNetConn(clientSide)
				assert.NoError(t, c.CheckIdle())
			})

			t.Run("server closed", func(t *testing.T) {
				clientSide, serverSide := socketPair(t, network)
				c := newTestConnFromNetConn(clientSide)
				require.NoError(t, serverSide.Close())
				require.Eventually(t, func() bool {
					return c.CheckIdle() != nil
				}, 5*time.Second, time.Millisecond)
				assert.ErrorIs(t, c.CheckIdle(), io.EOF)
			})

			t.Run("server sent data", func(t *testing.T) {
				clientSide, serverSide := socketPair(t, network)
				c := newTestConnFromNetConn(clientSide)
				_, err := serverSide.Write([]byte("E"))
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					return c.CheckIdle() != nil
				}, 5*time.Second, time.Millisecond)
				assert.ErrorIs(t, c.CheckIdle(), mterrors.ErrUnsolicitedData)

				// The check peeks without consuming: the byte is still there.
				b, err := c.bufferedReader.ReadByte()
				require.NoError(t, err)
				assert.Equal(t, byte('E'), b)
			})
		})
	}
}

func TestCheckIdle_BufferedData(t *testing.T) {
	clientSide, serverSide := socketPair(t, "tcp")
	c := newTestConnFromNetConn(clientSide)
	_, err := serverSide.Write([]byte("EZ"))
	require.NoError(t, err)

	// Pull both bytes into the bufio buffer and consume one, leaving the
	// kernel buffer empty and one byte buffered in user space.
	_, err = c.bufferedReader.Peek(2)
	require.NoError(t, err)
	_, err = c.bufferedReader.ReadByte()
	require.NoError(t, err)

	assert.ErrorIs(t, c.CheckIdle(), mterrors.ErrUnsolicitedData)
}

func TestCheckIdle_ClosedConn(t *testing.T) {
	clientSide, _ := socketPair(t, "tcp")
	c := newTestConnFromNetConn(clientSide)
	c.closed.Store(true)
	assert.ErrorIs(t, c.CheckIdle(), net.ErrClosed)
}

// wrappedConn is a net.Conn wrapper that exposes its inner connection the way
// *tls.Conn does, without itself exposing a file descriptor.
type wrappedConn struct {
	net.Conn
	inner func() net.Conn
}

func (w *wrappedConn) NetConn() net.Conn { return w.inner() }

func TestSocketOf(t *testing.T) {
	clientSide, _ := socketPair(t, "tcp")
	tlsConn := tls.Client(clientSide, &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12})
	twoDeep := &wrappedConn{Conn: tlsConn, inner: func() net.Conn { return tlsConn }}
	self := &wrappedConn{Conn: tlsConn}
	self.inner = func() net.Conn { return self }
	toNil := &wrappedConn{Conn: tlsConn, inner: func() net.Conn { return nil }}
	pipe, _ := net.Pipe()
	t.Cleanup(func() { _ = pipe.Close() })

	for _, tc := range []struct {
		name string
		nc   net.Conn
		want bool
	}{
		{"tcp", clientSide, true},
		{"tls over tcp", tlsConn, true},
		{"wrapper over tls over tcp", twoDeep, true},
		{"wrapper returning itself", self, false},
		{"wrapper returning nil", toNil, false},
		{"no file descriptor", pipe, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := socketOf(tc.nc)
			assert.Equal(t, tc.want, ok)
		})
	}
}

func TestCheckIdle_ThroughTLS(t *testing.T) {
	clientSide, serverSide := socketPair(t, "tcp")
	// No handshake is needed: the check peeks the raw socket under the TLS layer.
	c := newTestConnFromNetConn(tls.Client(clientSide, &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12}))
	require.NoError(t, c.CheckIdle())

	_, err := serverSide.Write([]byte{0x15}) // a TLS alert record's content type
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return c.CheckIdle() != nil
	}, 5*time.Second, time.Millisecond)
	assert.ErrorIs(t, c.CheckIdle(), mterrors.ErrUnsolicitedData)
}
