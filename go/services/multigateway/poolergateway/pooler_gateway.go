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

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByID returns a QueryService
	QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error)
}

// PoolerGateway selects and manages connections to multipooler instances.
type PoolerGateway struct {
	// loadBalancer manages pooler connections and selects connections for queries.
	loadBalancer *LoadBalancer

	// logger for debugging
	logger *slog.Logger
}

// NewPoolerGateway creates a new PoolerGateway.
func NewPoolerGateway(
	loadBalancer *LoadBalancer,
	logger *slog.Logger,
) *PoolerGateway {
	return &PoolerGateway{
		loadBalancer: loadBalancer,
		logger:       logger,
	}
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
//
// This method:
// 1. Uses LoadBalancer to get a connection matching the target specification
// 2. Delegates to the pooler's QueryService for execution
//
// The target specifies:
// - TableGroup: Required
// - PoolerType: PRIMARY (writes), REPLICA (reads), etc. Defaults to PRIMARY if not set.
// - Shard: Optional, empty matches any shard
//
// TODO: Add retry logic for transient failures (UNAVAILABLE errors)
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
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
	return conn.QueryService().StreamExecute(ctx, target, sql, options, callback)
}

// ExecuteQuery implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
//
// TODO: Add retry logic for transient failures (UNAVAILABLE errors)
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
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
	return conn.QueryService().ExecuteQuery(ctx, target, sql, options)
}

// PortalStreamExecute implements queryservice.QueryService.
// It executes a portal and returns reservation information.
//
// TODO: Add retry logic for transient failures (UNAVAILABLE errors)
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return queryservice.ReservedState{}, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
}

// Describe implements queryservice.QueryService.
// It returns metadata about a prepared statement or portal.
//
// TODO: Add retry logic for transient failures (UNAVAILABLE errors)
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
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
	return conn.QueryService().Describe(ctx, target, preparedStatement, portal, options)
}

// getQueryServiceForTarget is a helper that gets a QueryService for the given target.
// This is used by methods that need to get a QueryService and handle errors consistently.
func (pg *PoolerGateway) getQueryServiceForTarget(ctx context.Context, target *query.Target) (queryservice.QueryService, error) {
	conn, err := pg.loadBalancer.GetConnection(target)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService(), nil
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
		Shard:      "0-inf",   // TODO: Use proper shard constant
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
) (int16, []int16, queryservice.ReservedState, error) {
	// Get a pooler matching the target
	qs, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return 0, nil, queryservice.ReservedState{}, err
	}

	// Delegate to the pooler's QueryService
	return qs.CopyReady(ctx, target, copyQuery, options)
}

// CopySendData implements queryservice.QueryService.
// It sends a chunk of data for an active COPY operation.
func (pg *PoolerGateway) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	// Get a pooler matching the target
	qs, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return err
	}

	// Delegate to the pooler's QueryService
	return qs.CopySendData(ctx, target, data, options)
}

// CopyFinalize implements queryservice.QueryService.
// It completes a COPY operation, sending final data and returning the result.
func (pg *PoolerGateway) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, error) {
	// Get a pooler matching the target
	qs, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	// Delegate to the pooler's QueryService
	return qs.CopyFinalize(ctx, target, finalData, options)
}

// CopyAbort implements queryservice.QueryService.
// It aborts a COPY operation.
func (pg *PoolerGateway) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) error {
	// Get a pooler matching the target
	qs, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return err
	}

	// Delegate to the pooler's QueryService
	return qs.CopyAbort(ctx, target, errorMsg, options)
}
