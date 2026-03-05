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
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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
// Only PRIMARY traffic with specific error codes is buffered.
func classifyError(err error, target *query.Target) errorAction {
	if target.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
		return actionFail
	}
	code := mterrors.Code(err)
	switch code {
	case mtrpcpb.Code_UNAVAILABLE, mtrpcpb.Code_CLUSTER_EVENT, mtrpcpb.Code_READ_ONLY:
		return actionBuffer
	default:
		return actionFail
	}
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

// withBuffering wraps a query execution with failover buffering. It handles:
//  1. Proactive buffering — if the shard is already known to be failing over,
//     the request waits before sending any query (avoids a wasted round-trip).
//  2. Reactive buffering on GetConnection error — if no PRIMARY is in topology.
//  3. Reactive buffering on query error — if the PRIMARY is demoted mid-query.
//
// The inner function receives the connection's QueryService and executes the
// actual query. Callers capture multi-return results via closure variables.
// On retry, withBuffering is called recursively so inner runs against a fresh
// connection from the new PRIMARY.
func (pg *PoolerGateway) withBuffering(
	ctx context.Context,
	target *query.Target,
	inner func(qs queryservice.QueryService) error,
) error {
	retry := func() error {
		return pg.withBuffering(ctx, target, inner)
	}

	// 1. Proactive: if shard is already buffering, wait then retry.
	if pg.buffer != nil && target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
		retryDone, bufErr := pg.buffer.WaitIfAlreadyBuffering(ctx, commontypes.ShardKey{
			TableGroup: target.TableGroup,
			Shard:      target.Shard,
		})
		if bufErr != nil {
			return bufErr
		}
		if retryDone != nil {
			defer retryDone()
			return retry()
		}
	}

	// 2. Get connection.
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			retryDone, bufErr := pg.buffer.WaitForFailoverEnd(ctx, commontypes.ShardKey{
				TableGroup: target.TableGroup,
				Shard:      target.Shard,
			})
			if bufErr != nil {
				return bufErr
			}
			if retryDone != nil {
				defer retryDone()
				return retry()
			}
		}
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// 3. Execute query, reactive buffer on error.
	err = inner(conn.QueryService())
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		retryDone, bufErr := pg.buffer.WaitForFailoverEnd(ctx, commontypes.ShardKey{
			TableGroup: target.TableGroup,
			Shard:      target.Shard,
		})
		if bufErr != nil {
			return bufErr
		}
		if retryDone != nil {
			defer retryDone()
			return retry()
		}
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
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		return qs.StreamExecute(ctx, target, sql, options, callback)
	})
}

// ExecuteQuery implements queryservice.QueryService.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	var result *sqltypes.Result
	err := pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		var err error
		result, err = qs.ExecuteQuery(ctx, target, sql, options)
		return err
	})
	return result, err
}

// PortalStreamExecute implements queryservice.QueryService.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	var state queryservice.ReservedState
	err := pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		var err error
		state, err = qs.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
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
	err := pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		var err error
		desc, err = qs.Describe(ctx, target, preparedStatement, portal, options)
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
	reservationOptions *multipoolerpb.ReservationOptions,
) (int16, []int16, queryservice.ReservedState, error) {
	var (
		format     int16
		colFormats []int16
		state      queryservice.ReservedState
	)
	err := pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		var err error
		format, colFormats, state, err = qs.CopyReady(ctx, target, copyQuery, options, reservationOptions)
		return err
	})
	return format, colFormats, state, err
}

// ReserveStreamExecute implements queryservice.QueryService.
// It creates a reserved connection and executes the query.
func (pg *PoolerGateway) ReserveStreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *multipoolerpb.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	var state queryservice.ReservedState
	err := pg.withBuffering(ctx, target, func(qs queryservice.QueryService) error {
		var err error
		state, err = qs.ReserveStreamExecute(ctx, target, sql, options, reservationOptions, callback)
		return err
	})
	return state, err
}

// Close implements queryservice.QueryService.
// It closes all connections to poolers.
func (pg *PoolerGateway) Close() error {
	return pg.loadBalancer.Close()
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// getSystemServiceClient returns a MultiPoolerServiceClient for the given database.
// This can be used for authentication or other system-level operations.
// It finds any available pooler and returns a client connected to it.
func (pg *PoolerGateway) getSystemServiceClient(ctx context.Context, database string) (multipoolerpb.MultiPoolerServiceClient, error) {
	// Find any pooler - for authentication we just need access to pg_authid
	// which is available from any pooler connected to this database.
	// Try PRIMARY first, fall back to REPLICA if not found.
	target := &query.Target{
		TableGroup: "default", // TODO: Make configurable or discover from database
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		// PRIMARY not found, try REPLICA
		target.PoolerType = clustermetadatapb.PoolerType_REPLICA
		conn, err = pg.loadBalancer.GetConnection(target)
		if err != nil {
			return nil, fmt.Errorf("no pooler found for database %q: %w", database, err)
		}
	}

	// Return the service client from the connection
	return conn.ServiceClient(), nil
}

// SystemClientFunc returns a function that can be used with auth.PoolerHashProvider.
// The returned function discovers an available pooler and returns a system client for it.
func (pg *PoolerGateway) SystemClientFunc() func(ctx context.Context, database string) (multipoolerpb.MultiPoolerServiceClient, error) {
	return pg.getSystemServiceClient
}

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	return map[string]any{
		"active_connections": pg.loadBalancer.ConnectionCount(),
	}
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
) (*sqltypes.Result, error) {
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
	return conn.QueryService().CopyFinalize(ctx, target, finalData, options)
}

// CopyAbort implements queryservice.QueryService.
// It aborts a COPY operation.
func (pg *PoolerGateway) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
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
	return conn.QueryService().CopyAbort(ctx, target, errorMsg, options)
}

// ConcludeTransaction implements queryservice.QueryService.
// It concludes a transaction with COMMIT or ROLLBACK.
func (pg *PoolerGateway) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerpb.TransactionConclusion,
) (*sqltypes.Result, uint32, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, 0, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().ConcludeTransaction(ctx, target, options, conclusion)
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
