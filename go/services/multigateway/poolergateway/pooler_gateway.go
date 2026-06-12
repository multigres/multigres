// Copyright 2025 Supabase, Inc.
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

// Package poolergateway handles selection and communication with multipooler instances.
// It is responsible for:
// - Selecting healthy poolers for a given tablegroup via LoadBalancer
// - Providing QueryService instances for query execution
//
// This is analogous to Vitess's TabletGateway component.
package poolergateway

// TODO: Add PoolerGateway integration tests that verify end-to-end query routing
// through LoadBalancer to PoolerConnection. Currently the selection logic is
// tested via LoadBalancer unit tests.

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/buffer"
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByID returns a QueryService
	QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error)
}

// errorAction classifies whether an error should trigger buffering.
type errorAction int

const (
	actionFail   errorAction = iota // Pass through error to caller
	actionBuffer                    // Buffer and retry after failover
)

// classifyError determines whether an error is eligible for failover buffering.
// Only PRIMARY traffic is buffered, and only for:
//   - MTF01: multipooler signals planned failover (SERVING_RDONLY)
//   - 25006: PostgreSQL read_only_sql_transaction (in-flight query hit a
//     primary that has already transitioned to replica)
func classifyError(err error, target *query.Target) errorAction {
	if target.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
		return actionFail
	}
	if mterrors.IsErrorCode(err, mterrors.MTF01.ID, mterrors.PgSSReadOnlyTransaction) {
		return actionBuffer
	}
	return actionFail
}

// PoolerGateway selects and manages connections to multipooler instances.
type PoolerGateway struct {
	// loadBalancer manages pooler connections and selects connections for queries.
	loadBalancer *LoadBalancer

	// buffer holds requests during PRIMARY failovers. nil if buffering is disabled.
	buffer *buffer.Buffer

	// logger for debugging
	logger *slog.Logger
}

// NewPoolerGateway creates a new PoolerGateway.
func NewPoolerGateway(
	loadBalancer *LoadBalancer,
	buf *buffer.Buffer,
	logger *slog.Logger,
) *PoolerGateway {
	return &PoolerGateway{
		loadBalancer: loadBalancer,
		buffer:       buf,
		logger:       logger,
	}
}

// isSingleQuery reports whether a request is a single autocommit query — no
// existing reserved connection and not about to create one. Such queries can be
// served on a pooler that is draining for a planned failover, so they skip the
// proactive failover-buffer block (see withBuffering). It mirrors the pooler's
// own classification (poolerserver.admissionKind); each caller computes
// willReserve from the signal it carries (reservation options, portal MaxRows).
func isSingleQuery(reservedConnID uint64, willReserve bool) bool {
	return reservedConnID == 0 && !willReserve
}

// withBuffering wraps a query execution with failover buffering. It handles:
//  1. Proactive buffering — if the shard is already known to be failing over,
//     the request waits before sending any query (avoids a wasted round-trip).
//  2. Reactive buffering on GetConnection error — if no PRIMARY is in topology.
//  3. Reactive buffering on query error — if the PRIMARY is demoted mid-query.
//
// The inner function receives the PoolerConnection and executes the actual
// operation. Callers capture multi-return results via closure variables.
// On retry, the loop iterates so inner runs against a fresh connection from
// the new PRIMARY. Retries are capped at constants.MaxBufferingRetries.
//
// Callback safety on retry: inner receives a fresh PoolerConnection on each
// attempt, so callers must not carry over connection-specific state between
// retries. For streaming callbacks this is safe because the two error codes
// that trigger buffering both fire before any data is streamed:
//   - MTF01: returned by StartRequest() before query execution begins
//   - 25006: returned by PostgreSQL at statement start before any output
//
// The gateway handler executes individual statements (not multi-statement
// batches), so the callback is never invoked with partial results before a
// buffer-triggering error.
func (pg *PoolerGateway) withBuffering(
	ctx context.Context,
	target *query.Target,
	singleQuery bool,
	inner func(conn *PoolerConnection) error,
) error {
	bufferedOnce := false
	sk := &clustermetadatapb.ShardKey{
		TableGroup: target.TableGroup,
		Shard:      target.Shard,
	}

	var err error
	for range constants.MaxBufferingRetries + 1 {
		if pg.buffer != nil && !bufferedOnce && target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
			var retryDone buffer.RetryDoneFunc
			var bufErr error
			if err == nil {
				// Proactive: first attempt, check if shard is already buffering.
				//
				// Single autocommit queries skip the proactive block: a pooler
				// draining for a planned failover can still serve them during the
				// first drain stage, so we send them through and only buffer
				// reactively (below) if they actually bounce with MTF01. New
				// transactions/reservations keep the proactive block — the pooler
				// rejects them throughout the drain, so a wasted round-trip is
				// pointless.
				if !singleQuery {
					retryDone, bufErr = pg.buffer.WaitIfAlreadyBuffering(ctx, sk)
				}
			} else {
				// Reactive: after a buffer-worthy error, wait for failover to end.
				retryDone, bufErr = pg.buffer.WaitForFailoverEnd(ctx, sk)
			}
			if bufErr != nil {
				return bufErr
			}
			if retryDone != nil {
				// defer is intentional here: retryDone signals the buffer's drain
				// goroutine that the retry is complete, so it must run at function
				// exit (after the operation finishes), not at loop iteration end.
				// bufferedOnce ensures we only enter this block once per call.
				defer retryDone()
				bufferedOnce = true
			}
		}

		// Get connection.
		var conn *PoolerConnection
		conn, err = pg.loadBalancer.GetConnection(target)
		if err != nil {
			if classifyError(err, target) == actionBuffer {
				continue
			}
			return err
		}

		pg.logger.DebugContext(ctx, "selected pooler for target",
			"tablegroup", target.TableGroup,
			"shard", target.Shard,
			"pooler_type", target.PoolerType.String(),
			"pooler_id", conn.ID())

		// Execute operation.
		err = inner(conn)
		if err != nil && classifyError(err, target) == actionBuffer {
			continue
		}
		return err
	}
	return err
}

// QueryServiceByID implements Gateway.
// It returns a QueryService for a specific pooler ID.
// This is used for reserved connections where queries must be routed to a specific
// pooler instance (e.g., for session affinity with prepared statements and portals).
func (pg *PoolerGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error) {
	// Get connection by pooler ID
	conn, err := pg.loadBalancer.GetConnectionByID(id)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "got connection by pooler ID",
		"pooler_id", conn.ID(),
		"tablegroup", target.TableGroup,
		"shard", target.Shard)

	// Return the connection's QueryService
	return conn.QueryService(), nil
}

// StreamExecute implements queryservice.QueryService.
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var state *query.ReservedState
	// StreamExecute creates a reservation only when ReservationOptions is set.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), reservationOptions != nil), func(conn *PoolerConnection) error {
		var err error
		state, err = conn.QueryService().StreamExecute(ctx, target, sql, options, reservationOptions, callback)
		return err
	})
	return state, err
}

// ExecuteQuery implements queryservice.QueryService.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	var result *sqltypes.Result
	var state *query.ReservedState
	// ExecuteQuery cannot create a reservation.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), false), func(conn *PoolerConnection) error {
		var err error
		result, state, err = conn.QueryService().ExecuteQuery(ctx, target, sql, options)
		return err
	})
	return result, state, err
}

// PortalStreamExecute implements queryservice.QueryService.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	portalOptions *multipoolerpb.PortalExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var state *query.ReservedState
	// A portal reserves a connection only as a suspendable cursor (MaxRows > 0);
	// a fetch-all portal (MaxRows == 0) runs on a pooled connection and is a
	// single query — mirrors the pooler's classification.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), options.GetMaxRows() > 0), func(conn *PoolerConnection) error {
		var err error
		state, err = conn.QueryService().PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
		return err
	})
	return state, err
}

// Describe implements queryservice.QueryService.
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	var desc *query.StatementDescription
	// Describe never creates a reservation.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), false), func(conn *PoolerConnection) error {
		var err error
		desc, err = conn.QueryService().Describe(ctx, target, preparedStatement, portal, options)
		return err
	})
	return desc, err
}

// CopyReady implements queryservice.QueryService.
// It initiates a COPY FROM STDIN operation and returns format information.
func (pg *PoolerGateway) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, *query.ReservedState, error) {
	var (
		format     int16
		colFormats []int16
		state      *query.ReservedState
	)
	err := pg.withBuffering(ctx, target, false, func(conn *PoolerConnection) error {
		var err error
		format, colFormats, state, err = conn.QueryService().CopyReady(ctx, target, copyQuery, options, reservationOptions)
		return err
	})
	return format, colFormats, state, err
}

// CopyOutReady implements queryservice.QueryService.
// It initiates a COPY ... TO STDOUT operation and returns format information
// plus any pre-CopyOutResponse notices.
func (pg *PoolerGateway) CopyOutReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	var (
		format     int16
		colFormats []int16
		notices    []*mterrors.PgDiagnostic
		state      *query.ReservedState
	)
	err := pg.withBuffering(ctx, target, false, func(conn *PoolerConnection) error {
		var err error
		format, colFormats, notices, state, err = conn.QueryService().CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
		return err
	})
	return format, colFormats, notices, state, err
}

// CopyOutStream implements queryservice.QueryService.
// Pumps CopyData / NoticeResponse frames from the multipooler back through
// the supplied callback until RESULT/ERROR.
//
// Uses loadBalancer.GetConnection directly rather than withBuffering — same
// as CopySendData / CopyFinalize for the FROM-STDIN data phase. The stream
// has already been established by CopyOutReady and lives on a specific
// PoolerConnection's grpcQueryService.copyStreams map keyed by the
// reserved connection ID; routing this call through withBuffering's
// proactive failover check (which may stall, retry, and land on a
// different PoolerConnection) would only surface a confusing
// "no active COPY stream for reserved connection X" error in place of
// the real failure. Failover during a live COPY stream is a connection-level
// failure that the executor's CopyOutStream handles via
// IsConnectionError → Release(ReleaseError).
func (pg *PoolerGateway) CopyOutStream(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	onMessage func(client.CopyOutMessage) error,
) (*sqltypes.Result, *query.ReservedState, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService().CopyOutStream(ctx, target, options, onMessage)
}

// Close implements queryservice.QueryService.
// It closes all connections to poolers.
func (pg *PoolerGateway) Close() error {
	return pg.loadBalancer.Close()
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// GetAuthCredentials fetches authentication credentials from an available pooler.
// It uses withBuffering so that auth requests are buffered during planned failovers,
// just like query execution.
func (pg *PoolerGateway) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	target := &query.Target{
		TableGroup: "default", // TODO: Make configurable or discover from database
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	var resp *multipoolerpb.GetAuthCredentialsResponse
	err := pg.withBuffering(ctx, target, false, func(conn *PoolerConnection) error {
		var err error
		resp, err = conn.ServiceClient().GetAuthCredentials(ctx, req)
		// Convert gRPC error so classifyError can read the PgDiagnostic SQLSTATE for buffering.
		return mterrors.FromGRPC(err)
	})
	return resp, err
}

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	return map[string]any{
		"active_connections": pg.loadBalancer.ConnectionCount(),
	}
}

// LeadershipByID returns the consensus leadership role of each connected pooler,
// keyed by serialized pooler ID, for the admin/status page.
// See LoadBalancer.LeadershipByID.
func (pg *PoolerGateway) LeadershipByID() map[MultiPoolerID]string {
	return pg.loadBalancer.LeadershipByID()
}

// CopySendData implements queryservice.QueryService.
// It sends a chunk of data for an active COPY operation.
func (pg *PoolerGateway) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopySendData(ctx, target, data, options)
}

// CopyFinalize implements queryservice.QueryService.
// It completes a COPY operation, sending final data and returning the result.
func (pg *PoolerGateway) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopyFinalize(ctx, target, finalData, options)
}

// CopyAbort implements queryservice.QueryService.
// It aborts a COPY operation.
func (pg *PoolerGateway) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) (*query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopyAbort(ctx, target, errorMsg, options)
}

// ConcludeTransaction implements queryservice.QueryService.
// It concludes a transaction with COMMIT or ROLLBACK.
func (pg *PoolerGateway) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerpb.TransactionConclusion,
	releasePortalNames []string,
	releaseAllPortals bool,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals)
}

// DiscardTempTables implements queryservice.QueryService.
// It sends DISCARD TEMP on a reserved connection and removes the temp table reason.
func (pg *PoolerGateway) DiscardTempTables(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().DiscardTempTables(ctx, target, options)
}

// ReleaseReservedConnection implements queryservice.QueryService.
// It forcefully releases a reserved connection regardless of reason.
func (pg *PoolerGateway) ReleaseReservedConnection(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) error {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService().ReleaseReservedConnection(ctx, target, options)
}
