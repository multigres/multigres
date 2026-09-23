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

package poolergateway

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/queryrpc"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Retention policy. Fixed constants until a deployment shows it needs tuning.
const (
	maxIdleStreams    = 32
	idleStreamTimeout = 30 * time.Second
	maxStreamRequests = 1024
)

// streamPool retains only idle transport streams, never PostgreSQL connections.
// Each lease is exclusive; reservations and callers remain request fields.
type streamPool struct {
	client      pb.MultipoolerServiceClient
	ctx         context.Context
	cancel      context.CancelFunc
	m           *streamPoolMetrics
	mu          sync.Mutex
	idle        []*streamLease
	closed      bool
	unsupported atomic.Bool
}

type streamLease struct {
	stream   pb.MultipoolerService_ExecuteStreamClient
	cancel   context.CancelFunc
	timer    *time.Timer
	requests uint32
}

// streamStateConn is the connectivity view of the *grpc.ClientConn carrying the
// streams. An idle stream whose transport has died is indistinguishable from a
// healthy one until Send fails, and a failed Send is a post-submission error
// that must not be retried. Dropping idle streams as soon as the connection
// leaves READY keeps that window to the state-notification latency so the
// next operation opens a fresh stream and, if the peer is still down, takes
// the retryable pre-execution failure path instead.
type streamStateConn interface {
	GetState() connectivity.State
	WaitForStateChange(context.Context, connectivity.State) bool
}

func newStreamPool(client pb.MultipoolerServiceClient, conn streamStateConn) *streamPool {
	// The transport has an explicit Close lifecycle and must not inherit any
	// caller's values, credentials, deadline or cancellation after lease return.
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gocritic // pool-owned lifecycle, not a detached request
	p := &streamPool{client: client, ctx: ctx, cancel: cancel, m: streamMetrics()}
	if conn != nil {
		go p.watch(conn)
	}
	return p
}

func (p *streamPool) watch(conn streamStateConn) {
	for {
		s := conn.GetState()
		if s != connectivity.Ready {
			p.dropAllIdle(discardTransport)
		}
		if !conn.WaitForStateChange(p.ctx, s) {
			return
		}
	}
}

// Close cancels active and idle streams, including queries awaiting responses.
func (p *streamPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.dropAllIdleLocked(discardClosed)
	p.cancel()
}

func (p *streamPool) dropAllIdle(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropAllIdleLocked(reason)
}

func (p *streamPool) dropAllIdleLocked(reason string) {
	for _, l := range p.idle {
		// A timer whose callback already started will not find l in p.idle
		// and does nothing, so the discard is always ours.
		l.timer.Stop()
		p.dropIdle(l, reason)
	}
	p.idle = nil
}

// dropIdle cancels an idle lease. Caller holds p.mu and removes l from p.idle.
func (p *streamPool) dropIdle(l *streamLease, reason string) {
	l.cancel()
	p.m.idle.Add(p.ctx, -1)
	p.m.discards.Add(p.ctx, 1, discardAttr[reason])
}

func (p *streamPool) take() *streamLease {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.idle) > 0 {
		n := len(p.idle) - 1
		l := p.idle[n]
		p.idle[n] = nil
		p.idle = p.idle[:n]
		switch {
		case !l.timer.Stop():
			// The expiry callback is blocked on p.mu and will no longer find
			// l in p.idle, so the discard happens here.
			p.dropIdle(l, discardIdleExpired)
		case l.stream.Context().Err() != nil:
			p.dropIdle(l, discardTransport)
		default:
			p.m.idle.Add(p.ctx, -1)
			return l
		}
	}
	return nil
}

func (p *streamPool) put(l *streamLease) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Periodic retirement bounds the lifetime of enclosing RPC state. Never
	// expire a busy SQL operation: retirement happens only after completion.
	var reason string
	switch {
	case p.closed:
		reason = discardClosed
	case l.stream.Context().Err() != nil:
		reason = discardTransport
	case l.requests >= maxStreamRequests:
		reason = discardRetired
	case len(p.idle) >= maxIdleStreams:
		reason = discardIdleFull
	}
	if reason != "" {
		l.cancel()
		p.m.discards.Add(p.ctx, 1, discardAttr[reason])
		return
	}
	if l.timer != nil {
		// take stopped this timer before granting the exclusive lease. A timer
		// whose callback had started was discarded, so Reset cannot race an
		// old callback cancelling this new idle period.
		l.timer.Reset(idleStreamTimeout)
	} else {
		l.timer = time.AfterFunc(idleStreamTimeout, func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			for i, x := range p.idle {
				if x == l {
					copy(p.idle[i:], p.idle[i+1:])
					p.idle[len(p.idle)-1] = nil
					p.idle = p.idle[:len(p.idle)-1]
					p.dropIdle(l, discardIdleExpired)
					return
				}
			}
			// take already removed l and will discard it.
		})
	}
	p.idle = append(p.idle, l)
	p.m.idle.Add(p.ctx, 1)
}

// streamResponses exposes the same receive contract as the old server-streaming RPC.
// A completion frame is translated to EOF or its original gRPC status/details.
type streamResponses struct {
	lease    *streamLease
	done     bool
	stop     func() bool
	pool     *streamPool
	ctx      context.Context
	released bool
}

func (r *streamResponses) Recv() (*pb.StreamExecuteResponse, error) {
	if r.done || r.released {
		return nil, io.EOF
	}
	f, err := r.lease.stream.Recv()
	if err != nil {
		return nil, incompleteOperation(err)
	}
	if f.Ready || (f.Response == nil) == (f.Completion == nil) {
		return nil, status.Error(codes.Internal, "invalid execute stream response")
	}
	if f.Response != nil {
		return f.Response, nil
	}
	r.done = true
	if c := f.Completion; c.GetCode() != int32(codes.OK) {
		return nil, status.FromProto(c).Err()
	}
	return nil, io.EOF
}

// Release must run after the final callback, even on callback/transport error.
func (r *streamResponses) Release() {
	if r.released {
		return
	}
	r.released = true
	p := r.pool
	p.m.active.Add(p.ctx, -1)
	// stop reports false once the caller's cancellation callback has started;
	// the lease is then being cancelled concurrently and must not be reused.
	stopped := r.stop()
	switch {
	case !stopped || r.ctx.Err() != nil:
		r.lease.cancel()
		p.m.discards.Add(p.ctx, 1, discardAttr[discardCancelled])
	case !r.done:
		r.lease.cancel()
		p.m.discards.Add(p.ctx, 1, discardAttr[discardIncomplete])
	default:
		p.put(r.lease)
	}
}

// Open returns used=false only when falling back is provably safe: no SQL was
// sent. The caller must NOT retry a returned error after used=true. In
// particular, even Send returning EOF is ambiguous and cannot be replayed.
func (p *streamPool) Open(ctx context.Context, req *pb.StreamExecuteRequest) (*streamResponses, bool, error) {
	if p.unsupported.Load() {
		p.m.operations.Add(p.ctx, 1, attrLegacyUnsupported)
		return nil, false, nil
	}
	// Credentials/custom RPC metadata belong to individual calls. Preserve
	// their interceptors and validity checks through the legacy path.
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		p.m.operations.Add(p.ctx, 1, attrLegacyMetadata)
		return nil, false, nil
	}
	carrier, ok := queryrpc.Propagation(ctx)
	if !ok {
		p.m.operations.Add(p.ctx, 1, attrLegacyPropagation)
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, status.FromContextError(err).Err()
	}
	l := p.take()
	transport := attrReused
	var stop func() bool
	if l == nil {
		transport = attrNew
		streamCtx, cancel := context.WithCancel(p.ctx)
		// The caller's cancellation/deadline governs the handshake too; the
		// stream itself stays pool-owned once the lease is returned.
		stop = context.AfterFunc(ctx, cancel)
		stream, err := p.client.ExecuteStream(streamCtx)
		if err == nil {
			var ready *pb.ExecuteStreamResponse
			ready, err = stream.Recv()
			if err == nil && (!ready.Ready || ready.Response != nil || ready.Completion != nil) {
				err = status.Error(codes.Internal, "missing execute stream handshake")
			}
		}
		if err != nil {
			stop()
			cancel()
			if ctxErr := ctx.Err(); ctxErr != nil {
				// The stream was cancelled on the caller's behalf; report the
				// caller's own deadline/cancellation, not the derived cancel.
				return nil, false, status.FromContextError(ctxErr).Err()
			}
			if status.Code(err) == codes.Unimplemented {
				p.unsupported.Store(true)
				p.m.operations.Add(p.ctx, 1, attrLegacyUnsupported)
				return nil, false, nil
			}
			// No SQL has been sent, but leave classification to the caller.
			return nil, false, err
		}
		l = &streamLease{stream: stream, cancel: cancel}
	} else {
		stop = context.AfterFunc(ctx, l.cancel)
	}
	l.requests++
	p.m.operations.Add(p.ctx, 1, transport)
	p.m.active.Add(p.ctx, 1)
	r := &streamResponses{lease: l, stop: stop, pool: p, ctx: ctx}
	if err := l.stream.Send(requestFrame(ctx, req, carrier)); err != nil {
		r.Release()
		return nil, true, incompleteOperation(err)
	}
	return r, true, nil
}

func incompleteOperation(err error) error {
	// Unlike the legacy server-streaming RPC, transport EOF is NOT an SQL
	// success here. Only an explicit completion frame proves completion.
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unavailable, "execute stream ended before operation completion")
	}
	return err
}

func requestFrame(ctx context.Context, req *pb.StreamExecuteRequest, carrier map[string]string) *pb.ExecuteStreamRequest {
	f := &pb.ExecuteStreamRequest{Request: req, Propagation: carrier}
	if deadline, ok := ctx.Deadline(); ok {
		f.TimeoutNanos = max(1, int64(time.Until(deadline)))
	}
	return f
}
