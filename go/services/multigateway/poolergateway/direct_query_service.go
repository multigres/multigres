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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// directQueryService implements queryservice.QueryService by calling the
// pooler in the same process (LocalPooler.QueryService) directly: the same
// admission and executor the pooler's gRPC handler calls, in the caller's
// goroutine and ctx, with nothing marshalled. Errors are returned as the
// pooler made them, except that the caller's deadline or cancellation becomes
// the client error the gRPC path gives (mterrors.FromContext).
//
// StreamReplication returns a gRPC stream, so it goes to the pooler over grpc,
// the in-memory gRPC query service, which also owns the connection Close
// closes.
type directQueryService struct {
	local queryservice.QueryService
	grpc  queryservice.QueryService
}

var _ queryservice.QueryService = (*directQueryService)(nil)

func newDirectQueryService(local, grpc queryservice.QueryService) queryservice.QueryService {
	return &directQueryService{local: local, grpc: grpc}
}

func (d *directQueryService) ExecuteQuery(ctx context.Context, target *querypb.Target, sql string, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.ExecuteQuery(ctx, target, sql, options)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, options *querypb.ExecuteOptions, reservationOptions *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	state, err := d.local.StreamExecute(ctx, target, sql, options, reservationOptions, callback)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) PortalStreamExecute(ctx context.Context, target *querypb.Target, preparedStatement *querypb.PreparedStatement, portal *querypb.Portal, options *querypb.ExecuteOptions, portalOptions *multipoolerservice.PortalExecuteOptions, reservationOptions *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	state, err := d.local.PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) Describe(ctx context.Context, target *querypb.Target, preparedStatement *querypb.PreparedStatement, portal *querypb.Portal, options *querypb.ExecuteOptions) (*querypb.StatementDescription, error) {
	description, err := d.local.Describe(ctx, target, preparedStatement, portal, options)
	return description, mterrors.FromContext(ctx, err)
}

// Close closes the in-memory gRPC connection; the pooler's query service
// belongs to the pooler.
func (d *directQueryService) Close() error {
	return d.grpc.Close()
}

func (d *directQueryService) CopyReady(ctx context.Context, target *querypb.Target, copyQuery string, options *querypb.ExecuteOptions, reservationOptions *querypb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	format, columns, state, err := d.local.CopyReady(ctx, target, copyQuery, options, reservationOptions)
	return format, columns, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) CopySendData(ctx context.Context, target *querypb.Target, data []byte, options *querypb.ExecuteOptions) error {
	return mterrors.FromContext(ctx, d.local.CopySendData(ctx, target, data, options))
}

func (d *directQueryService) CopyFinalize(ctx context.Context, target *querypb.Target, finalData []byte, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.CopyFinalize(ctx, target, finalData, options)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) CopyAbort(ctx context.Context, target *querypb.Target, errorMsg string, options *querypb.ExecuteOptions) (*querypb.ReservedState, error) {
	state, err := d.local.CopyAbort(ctx, target, errorMsg, options)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) CopyOutReady(ctx context.Context, target *querypb.Target, copyQuery string, options *querypb.ExecuteOptions, reservationOptions *querypb.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *querypb.ReservedState, error) {
	format, columns, notices, state, err := d.local.CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
	return format, columns, notices, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) CopyOutStream(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.CopyOutStream(ctx, target, options, onMessage)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) ConcludeTransaction(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, conclusion multipoolerservice.TransactionConclusion, releasePortalNames []string, releaseAllPortals bool, chain bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain, rollbackSessionSettings)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) DiscardTempTables(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.DiscardTempTables(ctx, target, options)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) ReleaseReservedConnection(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, keepStickyReservations bool) (*querypb.ReservedState, error) {
	state, err := d.local.ReleaseReservedConnection(ctx, target, options, keepStickyReservations)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) StreamReplication(ctx context.Context, init *multipoolerservice.StreamReplicationInit) (multipoolerservice.MultipoolerService_StreamReplicationClient, error) {
	return d.grpc.StreamReplication(ctx, init)
}
