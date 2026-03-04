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

// bufferAndRetry waits for a failover to end and then retries the operation.
func (pg *PoolerGateway) bufferAndRetry(ctx context.Context, target *query.Target, retryFunc func() error) error {
	retryDone, bufErr := pg.buffer.WaitForFailoverEnd(ctx, target.TableGroup, target.Shard)
	if bufErr != nil {
		return bufErr
	}
	// TODO: This logic seems out of whack, we return nil, nil when we want to retry when already draining.
	if retryDone == nil {
		// Not buffered (disabled, timing guard, etc.) — return original error.
		return nil
	}
	defer retryDone()
	return retryFunc()
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
// It routes the query to the appropriate multipooler instance based on the target.
// If the query fails with a bufferable error during a PRIMARY failover,
// the request is buffered and retried once a new PRIMARY is available.
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				return pg.StreamExecute(ctx, target, sql, options, callback)
			}); retryErr != nil {
				return retryErr
			}
			return nil
		}
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	err = conn.QueryService().StreamExecute(ctx, target, sql, options, callback)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			return pg.StreamExecute(ctx, target, sql, options, callback)
		}); retryErr != nil {
			return retryErr
		}
		return nil
	}
	return err
}

// ExecuteQuery implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			var result *sqltypes.Result
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				var retryErr error
				result, retryErr = pg.ExecuteQuery(ctx, target, sql, options)
				return retryErr
			}); retryErr != nil {
				return nil, retryErr
			}
			return result, nil
		}
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	result, err := conn.QueryService().ExecuteQuery(ctx, target, sql, options)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			var retryErr error
			result, retryErr = pg.ExecuteQuery(ctx, target, sql, options)
			return retryErr
		}); retryErr != nil {
			return nil, retryErr
		}
		return result, nil
	}
	return result, err
}

// PortalStreamExecute implements queryservice.QueryService.
// It executes a portal and returns reservation information.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			var state queryservice.ReservedState
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				var retryErr error
				state, retryErr = pg.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
				return retryErr
			}); retryErr != nil {
				return queryservice.ReservedState{}, retryErr
			}
			return state, nil
		}
		return queryservice.ReservedState{}, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	state, err := conn.QueryService().PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			var retryErr error
			state, retryErr = pg.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
			return retryErr
		}); retryErr != nil {
			return queryservice.ReservedState{}, retryErr
		}
		return state, nil
	}
	return state, err
}

// Describe implements queryservice.QueryService.
// It returns metadata about a prepared statement or portal.
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			var desc *query.StatementDescription
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				var retryErr error
				desc, retryErr = pg.Describe(ctx, target, preparedStatement, portal, options)
				return retryErr
			}); retryErr != nil {
				return nil, retryErr
			}
			return desc, nil
		}
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	desc, err := conn.QueryService().Describe(ctx, target, preparedStatement, portal, options)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			var retryErr error
			desc, retryErr = pg.Describe(ctx, target, preparedStatement, portal, options)
			return retryErr
		}); retryErr != nil {
			return nil, retryErr
		}
		return desc, nil
	}
	return desc, err
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

// CopyReady implements queryservice.QueryService.
// It initiates a COPY FROM STDIN operation and returns format information.
func (pg *PoolerGateway) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *multipoolerpb.ReservationOptions,
) (int16, []int16, queryservice.ReservedState, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			var (
				format     int16
				colFormats []int16
				state      queryservice.ReservedState
			)
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				var retryErr error
				format, colFormats, state, retryErr = pg.CopyReady(ctx, target, copyQuery, options, reservationOptions)
				return retryErr
			}); retryErr != nil {
				return 0, nil, queryservice.ReservedState{}, retryErr
			}
			return format, colFormats, state, nil
		}
		return 0, nil, queryservice.ReservedState{}, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	format, colFormats, state, err := conn.QueryService().CopyReady(ctx, target, copyQuery, options, reservationOptions)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			var retryErr error
			format, colFormats, state, retryErr = pg.CopyReady(ctx, target, copyQuery, options, reservationOptions)
			return retryErr
		}); retryErr != nil {
			return 0, nil, queryservice.ReservedState{}, retryErr
		}
		return format, colFormats, state, nil
	}
	return format, colFormats, state, err
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
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		if pg.buffer != nil && classifyError(err, target) == actionBuffer {
			var state queryservice.ReservedState
			if retryErr := pg.bufferAndRetry(ctx, target, func() error {
				var retryErr error
				state, retryErr = pg.ReserveStreamExecute(ctx, target, sql, options, reservationOptions, callback)
				return retryErr
			}); retryErr != nil {
				return queryservice.ReservedState{}, retryErr
			}
			return state, nil
		}
		return queryservice.ReservedState{}, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	state, err := conn.QueryService().ReserveStreamExecute(ctx, target, sql, options, reservationOptions, callback)
	if err != nil && pg.buffer != nil && classifyError(err, target) == actionBuffer {
		if retryErr := pg.bufferAndRetry(ctx, target, func() error {
			var retryErr error
			state, retryErr = pg.ReserveStreamExecute(ctx, target, sql, options, reservationOptions, callback)
			return retryErr
		}); retryErr != nil {
			return queryservice.ReservedState{}, retryErr
		}
		return state, nil
	}
	return state, err
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
