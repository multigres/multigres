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

package multipooler

import (
	"context"
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

// localQueryService is the pooler's query service for callers in the same
// process (minigres's gateway), without gRPC. It becomes available when the
// pooler registers its services, as the gRPC service does, and Stop gives it
// the graceful stop the gRPC server gives its calls: no call is left running
// when the pooler closes its pools and executor.
type localQueryService struct {
	mu sync.Mutex
	// inner is the pooler's query service (admission, then the executor), set
	// when the pooler registers its services; nil before.
	inner    queryservice.QueryService
	stopped  bool
	inFlight sync.WaitGroup
}

var _ queryservice.QueryService = (*localQueryService)(nil)

// register makes the pooler's query service available to callers.
func (l *localQueryService) register(p *poolerserver.QueryPoolerServer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inner = p.QueryService()
}

// Stop refuses new calls and waits for running ones to return.
func (l *localQueryService) Stop() {
	l.mu.Lock()
	l.stopped = true
	l.mu.Unlock()
	l.inFlight.Wait()
}

// begin admits a call and returns the query service to run it on and the
// function that ends it. A call before the pooler registers its services or
// after Stop is refused as unavailable before execution (retryable), as a gRPC call to a
// server that is not serving is.
func (l *localQueryService) begin() (queryservice.QueryService, func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || l.inner == nil {
		return nil, nil, mterrors.MarkPreExecutionUnavailable(
			mterrors.New(mtrpcpb.Code_UNAVAILABLE, "multipooler query service is not serving"))
	}
	l.inFlight.Add(1)
	return l.inner, l.inFlight.Done, nil
}

func (l *localQueryService) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.ExecuteQuery(ctx, target, sql, options)
}

func (l *localQueryService) StreamExecute(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.StreamExecute(ctx, target, sql, options, reservationOptions, callback)
}

func (l *localQueryService) PortalStreamExecute(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions, portalOptions *multipoolerpb.PortalExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
}

func (l *localQueryService) Describe(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions) (*query.StatementDescription, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.Describe(ctx, target, preparedStatement, portal, options)
}

// Close does nothing: Stop, run at shutdown, ends the service.
func (l *localQueryService) Close() error {
	return nil
}

func (l *localQueryService) CopyReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return 0, nil, nil, err
	}
	defer done()
	return qs.CopyReady(ctx, target, copyQuery, options, reservationOptions)
}

func (l *localQueryService) CopySendData(ctx context.Context, target *query.Target, data []byte, options *query.ExecuteOptions) error {
	qs, done, err := l.begin()
	if err != nil {
		return err
	}
	defer done()
	return qs.CopySendData(ctx, target, data, options)
}

func (l *localQueryService) CopyFinalize(ctx context.Context, target *query.Target, finalData []byte, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.CopyFinalize(ctx, target, finalData, options)
}

func (l *localQueryService) CopyAbort(ctx context.Context, target *query.Target, errorMsg string, options *query.ExecuteOptions) (*query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.CopyAbort(ctx, target, errorMsg, options)
}

func (l *localQueryService) CopyOutReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return 0, nil, nil, nil, err
	}
	defer done()
	return qs.CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
}

func (l *localQueryService) CopyOutStream(ctx context.Context, target *query.Target, options *query.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.CopyOutStream(ctx, target, options, onMessage)
}

func (l *localQueryService) ConcludeTransaction(ctx context.Context, target *query.Target, options *query.ExecuteOptions, conclusion multipoolerpb.TransactionConclusion, releasePortalNames []string, releaseAllPortals bool, chain bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain, rollbackSessionSettings)
}

func (l *localQueryService) DiscardTempTables(ctx context.Context, target *query.Target, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, nil, err
	}
	defer done()
	return qs.DiscardTempTables(ctx, target, options)
}

func (l *localQueryService) ReleaseReservedConnection(ctx context.Context, target *query.Target, options *query.ExecuteOptions, keepStickyReservations bool) (*query.ReservedState, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.ReleaseReservedConnection(ctx, target, options, keepStickyReservations)
}

func (l *localQueryService) StreamReplication(ctx context.Context, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	qs, done, err := l.begin()
	if err != nil {
		return nil, err
	}
	defer done()
	return qs.StreamReplication(ctx, init)
}
