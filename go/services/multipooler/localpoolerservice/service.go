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
	// inner is the pooler's query service (admission, then the executor), set
	// when the pooler registers its services; nil before.
	inner    queryservice.QueryService
	stopped  bool
	inFlight sync.WaitGroup
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

var _ queryservice.QueryService = (*Server)(nil)

// register makes the pooler's query service available to callers.
func (s *Server) register(p *poolerserver.QueryPoolerServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inner = p.QueryService()
}

// Stop refuses new calls and waits for running ones to return.
func (s *Server) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.inFlight.Wait()
}

// begin admits a call and returns the query service to run it on and the
// function that ends it. A call before the pooler registers its services or
// after Stop is refused as unavailable before execution, so it can be retried,
// as a gRPC call to a server that is not serving is.
func (s *Server) begin() (queryservice.QueryService, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.inner == nil {
		return nil, nil, mterrors.MarkPreExecutionUnavailable(
			mterrors.New(mtrpcpb.Code_UNAVAILABLE, "multipooler query service is not serving"))
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
