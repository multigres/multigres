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

package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// cancelRequestLength is the size of a CancelRequest packet: length, request code,
// backend process ID and secret key, each four bytes.
const cancelRequestLength = 16

// SendCancelRequest asks the server to cancel the query this connection is running,
// by sending a CancelRequest over a new connection to the postmaster. The server
// handles it before authentication and before it allocates a connection slot, so it
// needs no session and works even when connection slots are exhausted. It is safe to
// call from another goroutine while a query is blocked, and returns once the server
// has closed its end, which it does after handling the request, or when ctx ends.
//
// The request goes to the same address as the connection: its unix socket when one is
// configured. Over TCP it is sent without TLS, which postgres accepts. The secret key
// is four bytes, as in protocol 3.0; protocol 3.2 allows longer keys, so this must
// change if the client moves to it, or the server will silently ignore the request.
//
// A cancel that arrives after the query finished is ignored by an idle backend, but
// can hit this connection's next query if it is reused, so callers should close the
// connection afterwards.
func (c *Conn) SendCancelRequest(ctx context.Context) error {
	netConn, err := dial(ctx, c.config)
	if err != nil {
		return fmt.Errorf("cancel request: %w", err)
	}
	defer netConn.Close()

	// Closing the connection when ctx ends unblocks the write and the read below,
	// whether ctx timed out or was cancelled.
	stop := context.AfterFunc(ctx, func() { _ = netConn.Close() })
	defer stop()

	var packet [cancelRequestLength]byte
	binary.BigEndian.PutUint32(packet[0:], cancelRequestLength)
	binary.BigEndian.PutUint32(packet[4:], protocol.CancelRequestCode)
	binary.BigEndian.PutUint32(packet[8:], c.processID)
	binary.BigEndian.PutUint32(packet[12:], c.secretKey)
	if _, err := netConn.Write(packet[:]); err != nil {
		return cancelRequestError(ctx, err)
	}

	// The server closes the connection after it has handled the request; waiting for
	// that makes sure the signal was sent before the caller moves on.
	if _, err := io.Copy(io.Discard, netConn); err != nil {
		return cancelRequestError(ctx, err)
	}
	return nil
}

// cancelRequestError reports why sending the request failed, naming ctx's error when
// it is what ended the attempt.
func cancelRequestError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("cancel request: %w", ctxErr)
	}
	return fmt.Errorf("cancel request: %w", err)
}
