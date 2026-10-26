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
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/multigres/multigres/go/common/mterrors"
)

// CheckIdle reports whether an idle connection — one with no request in
// flight — is still usable, without sending anything to the server. It
// returns nil when the socket is open and has nothing waiting to be read, and
// an error when the server has already closed it or written to it.
//
// PostgreSQL writes nothing to an idle session except when ending it: a
// backend terminated while idle (pg_terminate_backend, shutdown,
// idle_session_timeout) sends a FATAL ErrorResponse and closes the socket.
// That reply sits unread in the receive buffer, and a request sent afterwards
// reads it back as if PostgreSQL had answered the request with it. Checking
// before sending tells the two apart: a connection that fails the check never
// received the request, so the caller can replace it and send there.
//
// A nil result is not a guarantee: the server can still close the connection
// after the check, and a peer that vanished without a FIN or RST looks
// healthy. A connection whose transport cannot be inspected is reported as
// usable.
func (c *Conn) CheckIdle() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.bufferedReader.Buffered() > 0 {
		return mterrors.ErrUnsolicitedData
	}
	return peekSocket(c.conn)
}

// socketOf unwraps nc through NetConn() (as *tls.Conn provides) until it finds
// a layer that exposes its file descriptor. It reports false when no layer
// does, or when a wrapper's NetConn returns nil or itself.
func socketOf(nc net.Conn) (syscall.Conn, bool) {
	for nc != nil {
		if sc, ok := nc.(syscall.Conn); ok {
			return sc, true
		}
		w, ok := nc.(interface{ NetConn() net.Conn })
		if !ok {
			return nil, false
		}
		inner := w.NetConn()
		if inner == nc {
			return nil, false
		}
		nc = inner
	}
	return nil, false
}

// peekSocket checks the kernel receive buffer of nc's socket without blocking
// and without consuming anything. For TLS it inspects the underlying socket:
// any bytes there on an idle connection are a record the server sent
// unprompted, which is enough to tell that the connection is not idle.
func peekSocket(nc net.Conn) error {
	sc, ok := socketOf(nc)
	if !ok {
		return nil
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}

	var (
		n       int
		peekErr error
		buf     [1]byte
	)
	// Returning true from the callback reports the operation done, so Read
	// never parks waiting for the socket to become readable.
	if err := raw.Read(func(fd uintptr) bool {
		n, _, peekErr = unix.Recvfrom(int(fd), buf[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		return true
	}); err != nil {
		return err
	}

	switch {
	case errors.Is(peekErr, unix.EAGAIN), errors.Is(peekErr, unix.EWOULDBLOCK), errors.Is(peekErr, unix.EINTR):
		return nil
	case peekErr != nil:
		return fmt.Errorf("idle connection check: %w", peekErr)
	case n == 0:
		return fmt.Errorf("idle connection check: %w", io.EOF)
	default:
		return mterrors.ErrUnsolicitedData
	}
}
