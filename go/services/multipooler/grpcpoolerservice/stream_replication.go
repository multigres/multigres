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

package grpcpoolerservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	commonrepl "github.com/multigres/multigres/go/common/replication"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/services/multipooler/internal/replication"
)

// replBackend adapts the hijacked raw socket into an io.ReadWriteCloser for the
// tunnel. Reads first drain any bytes the client.Conn had buffered ahead of the
// detach, then continue from the raw socket; writes and close go straight to
// the socket.
type replBackend struct {
	io.Reader
	io.Writer
	io.Closer
}

// StreamReplication tunnels the raw PostgreSQL replication wire protocol as
// opaque bytes between the gateway and a dedicated replication=database backend.
// The pooler does not interpret the replication sub-protocol; bytes flow
// verbatim in both directions. PostgreSQL ErrorResponse rides through as opaque
// data — only infrastructure failures use the structured error channel.
func (s *poolerService) StreamReplication(stream multipoolerpb.MultipoolerService_StreamReplicationServer) error {
	ctx := stream.Context()

	// The first message must carry init: routing, caller, mode, and the
	// credentials needed to open the backend as the requesting role.
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to receive init: %v", err)
	}
	init := req.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "first StreamReplication message must be init")
	}

	// Reject replication modes that are not implemented. Only logical
	// (replication=database) is supported in v1; physical (TRUE) is reserved
	// and UNSPECIFIED is never valid.
	if init.GetMode() != multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE {
		return status.Errorf(codes.InvalidArgument,
			"unsupported replication mode %s: only REPLICATION_MODE_DATABASE is implemented",
			init.GetMode())
	}

	// A replication stream opens a fresh, dedicated, session-pinned backend, so
	// it is admitted like a new reservation (rejected during graceful drain).
	if err := s.pooler.StartRequest(init.GetTarget(), admissionKind(0, true)); err != nil {
		s.pooler.ReplicationMetrics().RecordSetupError(replication.SetupErrorAdmissionRejected)
		return mterrors.ToGRPC(err)
	}

	pm := s.pooler.PoolManager()
	if pm == nil {
		s.pooler.ReplicationMetrics().RecordSetupError(replication.SetupErrorUnavailable)
		return status.Error(codes.Unavailable, "pooler not initialized")
	}

	user := init.GetUser()
	metrics := s.pooler.ReplicationMetrics().NewStream(user)

	// Open the dedicated replication=database backend, authenticating as the
	// requesting role via SCRAM passthrough keys.
	setupStart := time.Now()
	conn, err := pm.NewLogicalReplicationConn(ctx, user,
		init.GetUserAuth().GetClientKey(), init.GetUserAuth().GetServerKey())
	if err != nil {
		s.pooler.ReplicationMetrics().RecordSetupError(replication.SetupErrorBackendOpenFailed)
		s.sendReplicationError(stream, err)
		return mterrors.ToGRPC(err)
	}
	metrics.RecordSetupLatency(time.Since(setupStart).Seconds())
	// Always release the reserved slot. Release taints the wrapper so Recycle
	// closes (rather than pools) the replication socket; the tunnel has already
	// closed the raw socket, and DetachConn left the client.Conn short-circuited
	// so this does not double-close it.
	defer conn.Release(reserved.ReleaseError, nil)

	// Hand off the authenticated socket to the protocol-blind tunnel.
	raw, buffered, err := conn.Conn().RawConn().DetachConn()
	if err != nil {
		s.pooler.ReplicationMetrics().RecordSetupError(replication.SetupErrorDetachFailed)
		s.sendReplicationError(stream, err)
		return status.Errorf(codes.Internal, "detach replication socket: %v", err)
	}
	backend := &replBackend{
		Reader: io.MultiReader(bytes.NewReader(buffered), raw),
		Writer: raw,
		Closer: raw,
	}

	return s.runReplicationTunnel(ctx, stream, backend, metrics)
}

// runReplicationTunnel sends the ready signal, then copies opaque bytes between
// the gRPC stream and the backend until either side ends. It is split from
// StreamReplication so it can be unit-tested with an in-memory backend and a
// fake stream, without a real postgres connection.
func (s *poolerService) runReplicationTunnel(
	ctx context.Context,
	stream multipoolerpb.MultipoolerService_StreamReplicationServer,
	backend io.ReadWriteCloser,
	metrics *replication.Stream,
) error {
	// Safety net: the tunnel closes the backend as its teardown lever, but a defer
	// guarantees the detached socket is released on every exit path as this
	// handler grows. Close is idempotent on the underlying net.Conn.
	defer backend.Close()

	if err := stream.Send(&multipoolerpb.StreamReplicationResponse{
		Msg: &multipoolerpb.StreamReplicationResponse_Ready{
			Ready: &multipoolerpb.StreamReplicationReady{},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send ready: %v", err)
	}

	metrics.IncActive()
	start := time.Now()

	// gRPC forbids concurrent SendMsg on one stream. The tunnel's downstream send
	// goroutine and the post-Run error send below can otherwise overlap — Run does
	// not wait for that goroutine, which may still be blocked in Send on a slow
	// client — so every Send goes through sendMu.
	var sendMu sync.Mutex
	send := func(b []byte) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(&multipoolerpb.StreamReplicationResponse{
			Msg: &multipoolerpb.StreamReplicationResponse_Data{Data: b},
		})
	}
	recv := func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			// io.EOF (client half-close) and other errors are classified by the
			// tunnel; pass them through unchanged.
			return nil, err
		}
		// init-after-init is ignored: only data carries tunnel bytes.
		return req.GetData(), nil
	}

	runErr := commonrepl.NewTunnel(backend, metrics, send, recv).Run(ctx)

	metrics.DecActive()
	metrics.RecordDuration(time.Since(start).Seconds())
	metrics.RecordTermination(terminationReason(runErr))

	if runErr != nil {
		// Infrastructure failure (not a PG ErrorResponse, which already flowed as
		// opaque data). Best-effort structured error, serialized with the tunnel's
		// sends. TryLock, not Lock: the downstream goroutine may still hold sendMu
		// (blocked in Send on a slow client), and blocking would both risk a
		// concurrent Send and deadlock — the gRPC status below conveys the failure
		// regardless.
		if sendMu.TryLock() {
			_ = stream.Send(replicationErrorResponse(runErr))
			sendMu.Unlock()
		}
		return mterrors.ToGRPC(runErr)
	}
	return nil
}

// replicationErrorResponse wraps an infrastructure error as a structured
// StreamReplicationResponse (distinct from a PG ErrorResponse, which flows as
// opaque data).
func replicationErrorResponse(err error) *multipoolerpb.StreamReplicationResponse {
	return &multipoolerpb.StreamReplicationResponse{
		Msg: &multipoolerpb.StreamReplicationResponse_Error{
			Error: &multipoolerpb.StreamReplicationError{
				Diagnostic: pgDiagnosticFromError(err),
			},
		},
	}
}

// sendReplicationError sends a structured infrastructure error on the stream.
// Best-effort: a send failure (e.g. the client already disconnected) is ignored.
// Only called before the tunnel starts (no concurrent sender exists yet); the
// post-tunnel error send is serialized via sendMu in runReplicationTunnel.
func (s *poolerService) sendReplicationError(
	stream multipoolerpb.MultipoolerService_StreamReplicationServer,
	err error,
) {
	_ = stream.Send(replicationErrorResponse(err))
}

// terminationReason classifies a tunnel exit for the terminations metric.
//
// v1 keeps this coarse: the tunnel collapses both directions into one error
// value, so we cannot always tell a client disconnect from a leader change, or
// a postgres socket error from a gRPC transport error. A clean exit or context
// cancellation is attributed to the client; anything else is a backend error.
// Finer attribution would require the tunnel to surface which direction and
// transport failed.
func terminationReason(err error) string {
	switch {
	case err == nil:
		return replication.TerminationClientDisconnect
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return replication.TerminationClientDisconnect
	default:
		return replication.TerminationBackendError
	}
}
