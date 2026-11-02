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
	"context"
	"errors"
	"io"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/servenv"
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
	// notification streams). A stream lasts as long as its caller wants, so
	// Stop ends them rather than waiting.
	streams    map[uint64]context.CancelFunc
	nextStream uint64
}

// NewServer returns a Server that refuses calls until it is registered with
// RegisterPoolerServices and the pooler registers its services.
func NewServer() *Server {
	return &Server{}
}

// RegisterPoolerServices makes server serve the pooler's query service once
// the pooler registers its services, and stops it, waiting for running calls,
// with the gRPC servers' graceful stop: before OnClose closes the pools and
// executor.
func RegisterPoolerServices(senv *servenv.ServEnv, server *Server) {
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, server.register)
	senv.OnTermSync(server.Stop)
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
// to return.
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

func (s *Server) StreamReplication(ctx context.Context, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	qs, done, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.StreamReplication(ctx, init)
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
// in a goroutine, joined to the caller by two channels. Like a health
// subscription it is not counted as a running call; it ends when ctx ends,
// when the loop ends (the pubsub listener stopped), or at Stop.
func (s *Server) NotificationStream(ctx context.Context) (queryservice.NotificationStream, error) {
	streamCtx, cancel, pooler, err := s.beginStream(ctx)
	if err != nil {
		return nil, err
	}
	p := &notificationPipe{
		ctx:   ctx,
		reqs:  make(chan *multipoolerpb.NotificationStreamRequest),
		resps: make(chan *multipoolerpb.NotificationStreamResponse),
		done:  make(chan struct{}),
	}
	go func() {
		defer cancel()
		defer close(p.done)
		p.err = pooler.ServeNotifications(streamCtx,
			func() (*multipoolerpb.NotificationStreamRequest, error) {
				select {
				case req := <-p.reqs:
					return req, nil
				case <-streamCtx.Done():
					return nil, io.EOF
				}
			},
			func(resp *multipoolerpb.NotificationStreamResponse) error {
				select {
				case p.resps <- resp:
					return nil
				case <-streamCtx.Done():
					return streamCtx.Err()
				}
			})
	}()
	return p, nil
}

// notificationPipe is the caller's end of a direct notification stream.
type notificationPipe struct {
	// ctx is the caller's context, to tell its own end from the stream's.
	ctx   context.Context
	reqs  chan *multipoolerpb.NotificationStreamRequest
	resps chan *multipoolerpb.NotificationStreamResponse
	// done is closed when the loop has returned, with err its result.
	done chan struct{}
	err  error
}

// Send hands a subscription update to the loop; io.EOF once the loop has
// ended, as a gRPC client stream's Send reports a stream the server ended.
func (p *notificationPipe) Send(req *multipoolerpb.NotificationStreamRequest) error {
	select {
	case p.reqs <- req:
		return nil
	case <-p.done:
		return io.EOF
	}
}

// Recv returns the loop's next reply. Replies are handed over unbuffered, so
// none is pending once the loop has ended; then it returns the loop's error,
// or the stream's end (streamEnded) when the loop ended cleanly or by
// cancellation.
func (p *notificationPipe) Recv() (*multipoolerpb.NotificationStreamResponse, error) {
	select {
	case resp := <-p.resps:
		return resp, nil
	case <-p.done:
		if p.err != nil && !errors.Is(p.err, context.Canceled) {
			return nil, p.err
		}
		return nil, streamEnded(p.ctx)
	}
}
