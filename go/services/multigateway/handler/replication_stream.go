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

package handler

import (
	"bytes"
	"context"
	"io"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	commonrepl "github.com/multigres/multigres/go/common/replication"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// HandleReplicationStream takes over an authenticated replication=database
// connection and tunnels it byte-for-byte to the PRIMARY pooler. It implements
// the server.ReplicationHandler capability, invoked from Conn.serve() once
// startup completes — the connection never enters the SQL command loop.
//
// The flow is the mirror image of the pooler side: here the tunnel "backend"
// is the client socket, and send/recv wrap the pooler gRPC client stream.
func (h *MultigatewayHandler) HandleReplicationStream(ctx context.Context, conn *server.Conn) error {
	state := h.getConnectionState(conn)
	ctx = h.callerContext(ctx, conn, state)

	// The gRPC stream and the tunnel must outlive DetachConn, which cancels the
	// connection's context (ctx is that context). Derive a context that drops
	// that cancellation propagation but keeps request values, and own its
	// teardown via cancel(): the deferred cancel covers every return path,
	// including the open-error path before detach. The stream is opened with
	// this context so it is not torn down the moment DetachConn fires.
	tunnelCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	// Build the Init BEFORE detaching the socket: DetachConn zeroizes the
	// connection's SCRAM keys, so userAuth must read them while they are still
	// live. The executor fills in the routing Target.
	init := &multipoolerservice.StreamReplicationInit{
		Mode:     multipoolerservice.ReplicationMode_REPLICATION_MODE_DATABASE,
		User:     conn.User(),
		UserAuth: userAuthFrom(conn),
	}

	// Open the pooler stream while the client socket is still attached, so a
	// backend-open failure can be surfaced as a verbatim ErrorResponse.
	stream, err := h.executor.StreamReplication(tunnelCtx, conn, state, init)
	if err != nil {
		h.logger.WarnContext(ctx, "failed to open replication stream", "user", conn.User(), "error", err)
		if werr := conn.WriteError(err); werr != nil {
			h.logger.WarnContext(ctx, "failed to write replication open error to client", "error", werr)
		}
		return err
	}

	// Detach the authenticated client socket and carry over any read-ahead
	// bytes the protocol reader already consumed.
	raw, buffered, err := conn.DetachConn()
	if err != nil {
		return err
	}

	backend := clientBackend{
		Reader: io.MultiReader(bytes.NewReader(buffered), raw),
		Writer: raw,
		Closer: raw,
	}

	send := func(b []byte) error {
		return stream.Send(&multipoolerservice.StreamReplicationRequest{
			Msg: &multipoolerservice.StreamReplicationRequest_Data{Data: append([]byte(nil), b...)},
		})
	}
	recv := func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if e := resp.GetError(); e != nil {
			// Structured infra error from the pooler: end the tunnel. The
			// client cannot receive a PG ErrorResponse here (we are mid-stream
			// on a hijacked socket), so closing the tunnel is the contract.
			if diag := e.GetDiagnostic(); diag != nil {
				return nil, mterrors.PgDiagnosticFromProto(diag)
			}
			return nil, mterrors.New(mtrpcpb.Code_INTERNAL, "replication stream returned an error without a diagnostic")
		}
		return resp.GetData(), nil
	}

	return commonrepl.NewTunnel(backend, h.metrics.NewReplicationStream(conn.User()), send, recv).Run(tunnelCtx)
}

// clientBackend adapts the detached client socket (plus its read-ahead buffer)
// to the io.ReadWriteCloser the replication tunnel copies through.
type clientBackend struct {
	io.Reader
	io.Writer
	io.Closer
}

// userAuthFrom builds the outbound UserAuth payload from the session's captured
// SCRAM passthrough keys. Returns nil for sessions that did not authenticate
// via SCRAM (e.g. trust in tests) so the field stays absent on the wire.
//
// Keys are copied rather than referenced: DetachConn zeroizes the accessor's
// backing slice in place, and gRPC may marshal lazily, so a detached copy
// guarantees the proto carries live bytes for the RPC's lifetime. This mirrors
// the helper of the same name in the scatterconn package.
func userAuthFrom(conn *server.Conn) *query.UserAuth {
	clientKey := conn.ScramClientKey()
	serverKey := conn.ScramServerKey()
	if clientKey == nil && serverKey == nil {
		return nil
	}
	return &query.UserAuth{
		ClientKey: append([]byte(nil), clientKey...),
		ServerKey: append([]byte(nil), serverKey...),
	}
}
