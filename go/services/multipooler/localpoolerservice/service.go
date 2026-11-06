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

// Package localpoolerservice serves the pooler's query service to callers in
// the same process, without gRPC: the in-process counterpart of
// grpcpoolerservice, used by minigres's gateway.
package localpoolerservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// Server is the pooler's query service for callers in the same process. It
// serves once the pooler registers its services, as the gRPC service does, and
// Stop gives it the graceful stop the gRPC server gives its calls: no call is
// left running when the pooler closes its pools and executor.
type Server struct {
	mu sync.Mutex
	// pooler and inner (its query service: admission, then the executor) are
	// set when the pooler registers its services; nil before.
	pooler   *poolerserver.QueryPoolerServer
	inner    queryservice.QueryService
	stopped  bool
	inFlight sync.WaitGroup
	// streams ends each running long-lived stream (health subscriptions,
	// notification and replication streams). A stream lasts as long as its
	// caller wants, so Stop ends them rather than waiting.
	streams    map[uint64]context.CancelFunc
	nextStream uint64
	// loops counts the running stream loops (ServeNotifications,
	// ServeReplication): once Stop ends their streams it waits for them to
	// return and release what they hold, as a gRPC server's graceful stop
	// waits for its stream handlers.
	loops sync.WaitGroup
}

// NewServer returns a Server that refuses calls until it is registered with
// RegisterPoolerServices and the pooler registers its services.
func NewServer() *Server {
	return &Server{}
}

// RegisterPoolerServices makes server serve the pooler's query service once
// the pooler registers its services. The multipooler stops it at shutdown,
// before stopping PostgreSQL.
func RegisterPoolerServices(server *Server) {
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, server.register)
}

var _ queryservice.Pooler = (*Server)(nil)

// register makes the pooler's query service available to callers.
func (s *Server) register(p *poolerserver.QueryPoolerServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pooler = p
	s.inner = p.QueryService()
}

// Stop refuses new calls, ends long-lived streams, and waits for running calls
// and the ended streams' loops to return.
func (s *Server) Stop() {
	s.mu.Lock()
	s.stopped = true
	streams := s.streams
	s.streams = nil
	s.mu.Unlock()
	for _, cancel := range streams {
		cancel()
	}
	s.inFlight.Wait()
	s.loops.Wait()
}

// notServing is the error for a call before the pooler registers its services
// or after Stop.
func notServing() error {
	return mterrors.MarkPreExecutionUnavailable(
		mterrors.New(mtrpcpb.Code_UNAVAILABLE, "multipooler query service is not serving"))
}

// begin admits a call and returns the query service to run it on and the
// function that ends it. A call before the pooler registers its services or
// after Stop is refused as unavailable before execution, so it can be retried,
// as a gRPC call to a server that is not serving is.
func (s *Server) begin() (queryservice.QueryService, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.inner == nil {
		return nil, nil, notServing()
	}
	s.inFlight.Add(1)
	return s.inner, s.inFlight.Done, nil
}

func (s *Server) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.ExecuteQuery(ctx, target, sql, options)
}

func (s *Server) StreamExecute(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.StreamExecute(ctx, target, sql, options, reservationOptions, callback)
}

func (s *Server) PortalStreamExecute(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions, portalOptions *multipoolerpb.PortalExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
}

func (s *Server) Describe(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions) (*query.StatementDescription, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.Describe(ctx, target, preparedStatement, portal, options)
}

// Close does nothing: Stop, run at shutdown, ends the service.
func (s *Server) Close() error {
	return nil
}

func (s *Server) CopyReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return 0, nil, nil, err
	}
	defer done()
	return qs.CopyReady(ctx, target, copyQuery, options, reservationOptions)
}

func (s *Server) CopySendData(ctx context.Context, target *query.Target, data []byte, options *query.ExecuteOptions) error {
	qs, done, err := s.begin()
	if err != nil {
		return err
	}
	defer done()
	return qs.CopySendData(ctx, target, data, options)
}

func (s *Server) CopyFinalize(ctx context.Context, target *query.Target, finalData []byte, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.CopyFinalize(ctx, target, finalData, options)
}

func (s *Server) CopyAbort(ctx context.Context, target *query.Target, errorMsg string, options *query.ExecuteOptions) (*query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.CopyAbort(ctx, target, errorMsg, options)
}

func (s *Server) CopyOutReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return 0, nil, nil, nil, err
	}
	defer done()
	return qs.CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
}

func (s *Server) CopyOutStream(ctx context.Context, target *query.Target, options *query.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.CopyOutStream(ctx, target, options, onMessage)
}

func (s *Server) ConcludeTransaction(ctx context.Context, target *query.Target, options *query.ExecuteOptions, conclusion multipoolerpb.TransactionConclusion, releasePortalNames []string, releaseAllPortals bool, chain bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain, rollbackSessionSettings)
}

func (s *Server) DiscardTempTables(ctx context.Context, target *query.Target, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.DiscardTempTables(ctx, target, options)
}

func (s *Server) ReleaseReservedConnection(ctx context.Context, target *query.Target, options *query.ExecuteOptions, keepStickyReservations bool) (*query.ReservedState, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.ReleaseReservedConnection(ctx, target, options, keepStickyReservations)
}

// GetAuthCredentials looks up a role's credentials, counted like a query call.
func (s *Server) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	_, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return s.pooler.GetAuthCredentials(ctx, req)
}

// beginStream admits a long-lived stream: refused like a call when not
// serving, otherwise tracked so Stop can end it. It returns the stream's
// context, ended by ctx, by cancel, or by Stop, and the pooler to serve it.
func (s *Server) beginStream(ctx context.Context) (context.Context, context.CancelFunc, *poolerserver.QueryPoolerServer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginStreamLocked(ctx)
}

// beginLoop admits a stream served by one of the pooler's stream loops, run in
// a goroutine, like beginStream, and counts the loop so Stop waits for it. The
// goroutine calls loopDone when the loop has returned.
func (s *Server) beginLoop(ctx context.Context) (streamCtx context.Context, cancel context.CancelFunc, pooler *poolerserver.QueryPoolerServer, loopDone func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	streamCtx, cancel, pooler, err = s.beginStreamLocked(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	s.loops.Add(1)
	return streamCtx, cancel, pooler, s.loops.Done, nil
}

// beginStreamLocked is beginStream with s.mu held.
func (s *Server) beginStreamLocked(ctx context.Context) (context.Context, context.CancelFunc, *poolerserver.QueryPoolerServer, error) {
	if s.stopped || s.pooler == nil {
		return nil, nil, nil, notServing()
	}
	streamCtx, cancel := context.WithCancel(ctx)
	if s.streams == nil {
		s.streams = make(map[uint64]context.CancelFunc)
	}
	id := s.nextStream
	s.nextStream++
	s.streams[id] = cancel
	context.AfterFunc(streamCtx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.streams, id)
	})
	return streamCtx, cancel, s.pooler, nil
}

// streamEnded is the error for a stream that is over: the caller's own error
// when ctx ended, io.EOF when the pooler or Stop ended it.
func streamEnded(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return io.EOF
}

// SubscribeHealth subscribes to the pooler's health. The subscription is not
// counted as a running call: it ends when ctx ends, when the pooler ends it, or
// at Stop, whose recv then returns io.EOF, as a gRPC health stream ends at the
// server's graceful stop.
func (s *Server) SubscribeHealth(ctx context.Context) (func() (*multipoolerpb.StreamPoolerHealthResponse, error), error) {
	subCtx, cancel, pooler, err := s.beginStream(ctx)
	if err != nil {
		return nil, err
	}
	initial, updates, err := pooler.SubscribeHealth(subCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	ended := func() error {
		cancel()
		return streamEnded(ctx)
	}
	return func() (*multipoolerpb.StreamPoolerHealthResponse, error) {
		if initial != nil {
			state := initial
			initial = nil
			return state.ToProto(), nil
		}
		select {
		case state, ok := <-updates:
			if !ok {
				return nil, ended()
			}
			return state.ToProto(), nil
		case <-subCtx.Done():
			return nil, ended()
		}
	}, nil
}

// NotificationStream opens a client session's LISTEN/NOTIFY stream: the
// pooler's notification loop (ServeNotifications, as the gRPC handler runs it)
// in a goroutine, joined to the caller by a pipe. Like a health subscription
// it is not counted as a running call; it ends when ctx ends, when the loop
// ends (the pubsub listener stopped), or at Stop, which waits for the loop to
// return.
func (s *Server) NotificationStream(ctx context.Context) (queryservice.NotificationStream, error) {
	streamCtx, cancel, pooler, loopDone, err := s.beginLoop(ctx)
	if err != nil {
		return nil, err
	}
	p := newPipe[*multipoolerpb.NotificationStreamRequest, *multipoolerpb.NotificationStreamResponse](ctx, 0)
	go func() {
		defer loopDone()
		defer cancel()
		p.finish(pooler.ServeNotifications(streamCtx, p.serverRecv(streamCtx), p.serverSend(streamCtx)))
	}()
	return p, nil
}

// replicationPipeBuffer is how many messages a replication stream's pipe holds
// in each direction, so WAL streaming can pipeline a little, as a gRPC
// stream's flow-control window allows, while a slow side still holds the
// other back.
const replicationPipeBuffer = 16

// StreamReplication opens a replication tunnel: the pooler's replication code
// (ServeReplication, as the gRPC handler runs it) in a goroutine, joined to the
// caller by a pipe, after the same open handshake as over gRPC. Like a
// notification stream it is not counted as a running call; it ends when ctx
// ends, when the tunnel ends, or at Stop, which waits for the tunnel to return
// and release its backend.
func (s *Server) StreamReplication(ctx context.Context, init *multipoolerpb.StreamReplicationInit) (queryservice.ReplicationStream, error) {
	streamCtx, cancel, pooler, loopDone, err := s.beginLoop(ctx)
	if err != nil {
		return nil, err
	}
	p := newPipe[*multipoolerpb.StreamReplicationRequest, *multipoolerpb.StreamReplicationResponse](ctx, replicationPipeBuffer)
	send := p.serverSend(streamCtx)
	go func() {
		defer loopDone()
		defer cancel()
		p.finish(pooler.ServeReplication(streamCtx, p.serverRecv(streamCtx), copyingData(send)))
	}()
	if err := queryservice.OpenReplication(p, init, nil); err != nil {
		cancel()
		return nil, err
	}
	return p, nil
}

// copyingData returns send with each data message's bytes copied first. The
// replication tunnel reuses its read buffer once send returns; gRPC copies the
// bytes when it marshals, a pipe hands over the slice itself.
func copyingData(send func(*multipoolerpb.StreamReplicationResponse) error) func(*multipoolerpb.StreamReplicationResponse) error {
	return func(resp *multipoolerpb.StreamReplicationResponse) error {
		if data, ok := resp.GetMsg().(*multipoolerpb.StreamReplicationResponse_Data); ok {
			data.Data = bytes.Clone(data.Data)
		}
		return send(resp)
	}
}

// pipe is an in-process stream between a caller and one of the pooler's
// stream loops (ServeNotifications, ServeReplication), run in a goroutine: the
// caller's Send and Recv on one end, the loop's recv and send on the other.
// Like a gRPC client stream, Send and Recv return as soon as the caller's ctx
// ends, even while the loop is blocked (for example on a LISTEN PostgreSQL has
// not answered), so a caller that gives up on the stream is not held by it.
type pipe[Req, Resp any] struct {
	// ctx is the caller's context: its end ends Send and Recv, and tells the
	// caller's own end of the stream from the loop's.
	ctx   context.Context
	reqs  chan Req
	resps chan Resp
	// done is closed by finish when the loop has returned, with err its result.
	done chan struct{}
	err  error
}

// newPipe returns a pipe for a caller with ctx, holding buffer messages in each
// direction (0: each hand-over waits for the other side).
func newPipe[Req, Resp any](ctx context.Context, buffer int) *pipe[Req, Resp] {
	return &pipe[Req, Resp]{
		ctx:   ctx,
		reqs:  make(chan Req, buffer),
		resps: make(chan Resp, buffer),
		done:  make(chan struct{}),
	}
}

// serverRecv is the loop's recv: the caller's next message, or io.EOF once the
// stream's context (streamCtx) ends.
func (p *pipe[Req, Resp]) serverRecv(streamCtx context.Context) func() (Req, error) {
	return func() (Req, error) {
		select {
		case req := <-p.reqs:
			return req, nil
		case <-streamCtx.Done():
			var zero Req
			return zero, io.EOF
		}
	}
}

// serverSend is the loop's send: hands resp to the caller, or fails once the
// stream's context (streamCtx) ends.
func (p *pipe[Req, Resp]) serverSend(streamCtx context.Context) func(Resp) error {
	return func(resp Resp) error {
		select {
		case p.resps <- resp:
			return nil
		case <-streamCtx.Done():
			return streamCtx.Err()
		}
	}
}

// finish records the loop's result and ends the stream for the caller.
func (p *pipe[Req, Resp]) finish(err error) {
	p.err = err
	close(p.done)
}

// Send hands a message to the loop; io.EOF once the loop has ended, as a gRPC
// client stream's Send reports a stream the server ended. The end is checked
// first: a buffered pipe would otherwise accept a message no one will read.
func (p *pipe[Req, Resp]) Send(req Req) error {
	select {
	case <-p.done:
		return io.EOF
	default:
	}
	select {
	case p.reqs <- req:
		return nil
	case <-p.done:
		return io.EOF
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// Recv returns the loop's next message. Once the loop has ended it first hands
// out messages still buffered, then returns the loop's error, or the stream's
// end (streamEnded) when the loop ended cleanly or by cancellation.
func (p *pipe[Req, Resp]) Recv() (Resp, error) {
	select {
	case resp := <-p.resps:
		return resp, nil
	case <-p.done:
		select {
		case resp := <-p.resps:
			return resp, nil
		default:
		}
		var zero Resp
		if p.err != nil && !errors.Is(p.err, context.Canceled) {
			return zero, p.err
		}
		return zero, streamEnded(p.ctx)
	case <-p.ctx.Done():
		var zero Resp
		return zero, p.ctx.Err()
	}
}
