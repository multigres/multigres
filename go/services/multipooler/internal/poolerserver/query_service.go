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

package poolerserver

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// QueryService returns the executor behind the admission and validation every
// query-path call goes through, whatever the transport: the gRPC handler and,
// in minigres, the gateway's direct calls. Each call admits the request
// (StartRequest, with the kind for its method), validates reservation reasons
// where it carries them, then runs on the executor.
func (s *QueryPoolerServer) QueryService() queryservice.QueryService {
	return &admittedQueryService{s: s}
}

// AdmissionKind classifies a query-path request for StartRequest. A non-zero
// reservedConnID means it continues an EXISTING reserved connection; otherwise
// reserves reports whether the request will create a NEW reserved connection —
// if not, it is a single autocommit query. Each method computes reserves from
// the signal it actually carries, mirroring the executor's own reservation
// decision (reservation reasons for StreamExecute, MaxRows or reasons for
// portals, always for COPY). The graceful drain serves single queries longer
// than new reservations, so the distinction matters during shutdown.
func AdmissionKind(reservedConnID uint64, reserves bool) RequestKind {
	switch {
	case reservedConnID > 0:
		return RequestExistingReserved
	case reserves:
		return RequestNewReservation
	default:
		return RequestSingleQuery
	}
}

// PortalReserves reports whether a PortalStreamExecute will use or create a
// reserved connection, mirroring the executor's reserve decision exactly: a
// suspendable cursor (MaxRows > 0) OR a portal carrying reservation reasons.
// The reasons case is reachable on the extended-query path — a deferred BEGIN
// is folded into the first portal as ReasonTransaction while MaxRows == 0 — so
// checking MaxRows alone would mis-admit such a portal as a single query during
// a graceful drain, and the executor would then open a reserved connection.
func PortalReserves(options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) bool {
	return options.GetMaxRows() > 0 || reservationOptions.GetReasons() != 0
}

type admittedQueryService struct {
	s *QueryPoolerServer
}

var _ queryservice.QueryService = (*admittedQueryService)(nil)

// admit admits a request of kind for target and returns the executor to run
// it on.
func (a *admittedQueryService) admit(target *query.Target, kind RequestKind) (queryservice.QueryService, error) {
	if err := a.s.StartRequest(target, kind); err != nil {
		return nil, err
	}
	return a.s.Executor()
}

// validateReasons rejects unknown reservation reason bits: the gateway's
// request is the trust boundary.
func validateReasons(reservationOptions *query.ReservationOptions) error {
	if reasons := reservationOptions.GetReasons(); reasons != 0 {
		if err := protoutil.ValidateReasons(reasons); err != nil {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid reservation reasons: %v", err)
		}
	}
	return nil
}

func (a *admittedQueryService) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	exec, err := a.admit(target, AdmissionKind(options.GetReservedConnectionId(), false))
	if err != nil {
		return nil, nil, err
	}
	return exec.ExecuteQuery(ctx, target, sql, options)
}

// StreamExecute is the only statement method that can create a new
// reservation (reservation reasons with no reserved connection), so a graceful
// drain keeps serving its single queries while rejecting new transactions.
func (a *admittedQueryService) StreamExecute(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	if err := a.s.StartRequest(target, AdmissionKind(options.GetReservedConnectionId(), reservationOptions.GetReasons() != 0)); err != nil {
		return nil, err
	}
	if err := validateReasons(reservationOptions); err != nil {
		return nil, err
	}
	exec, err := a.s.Executor()
	if err != nil {
		return nil, err
	}
	return exec.StreamExecute(ctx, target, sql, options, reservationOptions, callback)
}

func (a *admittedQueryService) PortalStreamExecute(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions, portalOptions *multipoolerpb.PortalExecuteOptions, reservationOptions *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	if err := a.s.StartRequest(target, AdmissionKind(options.GetReservedConnectionId(), PortalReserves(options, reservationOptions))); err != nil {
		return nil, err
	}
	if err := validateReasons(reservationOptions); err != nil {
		return nil, err
	}
	exec, err := a.s.Executor()
	if err != nil {
		return nil, err
	}
	return exec.PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
}

func (a *admittedQueryService) Describe(ctx context.Context, target *query.Target, preparedStatement *query.PreparedStatement, portal *query.Portal, options *query.ExecuteOptions) (*query.StatementDescription, error) {
	exec, err := a.admit(target, AdmissionKind(options.GetReservedConnectionId(), false))
	if err != nil {
		return nil, err
	}
	return exec.Describe(ctx, target, preparedStatement, portal, options)
}

// Close does nothing: the executor belongs to the pooler, not to callers.
func (a *admittedQueryService) Close() error {
	return nil
}

// CopyReady starts a COPY FROM STDIN, which always holds a reserved
// connection.
func (a *admittedQueryService) CopyReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, *query.ReservedState, error) {
	exec, err := a.admit(target, AdmissionKind(options.GetReservedConnectionId(), true))
	if err != nil {
		return 0, nil, nil, err
	}
	return exec.CopyReady(ctx, target, copyQuery, options, reservationOptions)
}

// CopySendData, CopyFinalize, CopyAbort and CopyOutStream continue a COPY that
// CopyReady or CopyOutReady admitted, so they are not admitted again; the
// executor checks that the reserved connection still exists.
func (a *admittedQueryService) CopySendData(ctx context.Context, target *query.Target, data []byte, options *query.ExecuteOptions) error {
	exec, err := a.s.Executor()
	if err != nil {
		return err
	}
	return exec.CopySendData(ctx, target, data, options)
}

func (a *admittedQueryService) CopyFinalize(ctx context.Context, target *query.Target, finalData []byte, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	exec, err := a.s.Executor()
	if err != nil {
		return nil, nil, err
	}
	return exec.CopyFinalize(ctx, target, finalData, options)
}

func (a *admittedQueryService) CopyAbort(ctx context.Context, target *query.Target, errorMsg string, options *query.ExecuteOptions) (*query.ReservedState, error) {
	exec, err := a.s.Executor()
	if err != nil {
		return nil, err
	}
	return exec.CopyAbort(ctx, target, errorMsg, options)
}

// CopyOutReady starts a COPY TO STDOUT, which always holds a reserved
// connection.
func (a *admittedQueryService) CopyOutReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	exec, err := a.admit(target, AdmissionKind(options.GetReservedConnectionId(), true))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	return exec.CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
}

func (a *admittedQueryService) CopyOutStream(ctx context.Context, target *query.Target, options *query.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
	exec, err := a.s.Executor()
	if err != nil {
		return nil, nil, err
	}
	return exec.CopyOutStream(ctx, target, options, onMessage)
}

// ConcludeTransaction, DiscardTempTables and ReleaseReservedConnection act on
// an existing reserved connection: admitted even while draining, since the
// reserved connection itself is the gate.
func (a *admittedQueryService) ConcludeTransaction(ctx context.Context, target *query.Target, options *query.ExecuteOptions, conclusion multipoolerpb.TransactionConclusion, releasePortalNames []string, releaseAllPortals bool, chain bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *query.ReservedState, error) {
	exec, err := a.admit(target, RequestExistingReserved)
	if err != nil {
		return nil, nil, err
	}
	return exec.ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain, rollbackSessionSettings)
}

func (a *admittedQueryService) DiscardTempTables(ctx context.Context, target *query.Target, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	exec, err := a.admit(target, RequestExistingReserved)
	if err != nil {
		return nil, nil, err
	}
	return exec.DiscardTempTables(ctx, target, options)
}

func (a *admittedQueryService) ReleaseReservedConnection(ctx context.Context, target *query.Target, options *query.ExecuteOptions, keepStickyReservations bool) (*query.ReservedState, error) {
	exec, err := a.admit(target, RequestExistingReserved)
	if err != nil {
		return nil, err
	}
	return exec.ReleaseReservedConnection(ctx, target, options, keepStickyReservations)
}

// StreamReplication is not served by the executor: the gRPC handler opens the
// replication tunnel itself.
func (a *admittedQueryService) StreamReplication(ctx context.Context, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	exec, err := a.s.Executor()
	if err != nil {
		return nil, err
	}
	return exec.StreamReplication(ctx, init)
}
