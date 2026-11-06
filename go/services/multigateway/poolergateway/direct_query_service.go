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

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// directQueryService implements queryservice.Pooler by calling the pooler in
// the same process (LocalPooler.Service) directly: the same admission,
// executor, credential lookup, health and notifications the pooler's gRPC
// handler calls, in
// the caller's goroutine and ctx, with nothing marshalled. Errors are returned
// as the pooler made them, except that the caller's deadline or cancellation
// becomes the client error the gRPC path gives (mterrors.FromContext).
//
// The caller's identity goes on the current span (callerid.AnnotateSpan) in
// the methods where the pooler's gRPC handler puts it on its operation span,
// so traces attribute a query to its caller on both paths.
type directQueryService struct {
	local queryservice.Pooler
}

var _ queryservice.Pooler = (*directQueryService)(nil)

func newDirectQueryService(local queryservice.Pooler) queryservice.Pooler {
	return &directQueryService{local: local}
}

func (d *directQueryService) ExecuteQuery(ctx context.Context, target *querypb.Target, sql string, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	result, state, err := d.local.ExecuteQuery(ctx, target, sql, options)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, options *querypb.ExecuteOptions, reservationOptions *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	state, err := d.local.StreamExecute(ctx, target, sql, options, reservationOptions, callback)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) PortalStreamExecute(ctx context.Context, target *querypb.Target, preparedStatement *querypb.PreparedStatement, portal *querypb.Portal, options *querypb.ExecuteOptions, portalOptions *multipoolerservice.PortalExecuteOptions, reservationOptions *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	state, err := d.local.PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) Describe(ctx context.Context, target *querypb.Target, preparedStatement *querypb.PreparedStatement, portal *querypb.Portal, options *querypb.ExecuteOptions) (*querypb.StatementDescription, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	description, err := d.local.Describe(ctx, target, preparedStatement, portal, options)
	return description, mterrors.FromContext(ctx, err)
}

// Close does nothing: there is no connection, and the pooler's query service
// belongs to the pooler.
func (d *directQueryService) Close() error {
	return nil
}

func (d *directQueryService) CopyReady(ctx context.Context, target *querypb.Target, copyQuery string, options *querypb.ExecuteOptions, reservationOptions *querypb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
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
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	format, columns, notices, state, err := d.local.CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
	return format, columns, notices, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) CopyOutStream(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *querypb.ReservedState, error) {
	result, state, err := d.local.CopyOutStream(ctx, target, options, onMessage)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) ConcludeTransaction(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, conclusion multipoolerservice.TransactionConclusion, releasePortalNames []string, releaseAllPortals bool, chain bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	result, state, err := d.local.ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain, rollbackSessionSettings)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) DiscardTempTables(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	result, state, err := d.local.DiscardTempTables(ctx, target, options)
	return result, state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) ReleaseReservedConnection(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, keepStickyReservations bool) (*querypb.ReservedState, error) {
	callerid.AnnotateSpan(ctx, callerid.FromContext(ctx))
	state, err := d.local.ReleaseReservedConnection(ctx, target, options, keepStickyReservations)
	return state, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) StreamReplication(ctx context.Context, init *multipoolerservice.StreamReplicationInit) (queryservice.ReplicationStream, error) {
	init.CallerId = callerid.FromContext(ctx)
	stream, err := d.local.StreamReplication(ctx, init)
	if err != nil {
		return nil, mterrors.FromContext(ctx, err)
	}
	return stream, nil
}

func (d *directQueryService) GetAuthCredentials(ctx context.Context, req *multipoolerservice.GetAuthCredentialsRequest) (*multipoolerservice.GetAuthCredentialsResponse, error) {
	resp, err := d.local.GetAuthCredentials(ctx, req)
	return resp, mterrors.FromContext(ctx, err)
}

func (d *directQueryService) SubscribeHealth(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
	return d.local.SubscribeHealth(ctx)
}

func (d *directQueryService) NotificationStream(ctx context.Context) (queryservice.NotificationStream, error) {
	return d.local.NotificationStream(ctx)
}
