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

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
func (pg *PoolerGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error) {
	// TODO: IMPLEMENT queryservicebyid
	return pg, nil
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
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Get a connection matching the target (waits for discovery if needed)
	conn, err := pg.loadBalancer.GetConnectionContext(ctx, target, nil)
	if err != nil {
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.StreamExecute(ctx, target, sql, options, callback)
}

// ExecuteQuery implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	// Get a connection matching the target (waits for discovery if needed)
	conn, err := pg.loadBalancer.GetConnectionContext(ctx, target, nil)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.ExecuteQuery(ctx, target, sql, options)
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
	// Get a connection matching the target (waits for discovery if needed)
	conn, err := pg.loadBalancer.GetConnectionContext(ctx, target, nil)
	if err != nil {
		return queryservice.ReservedState{}, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
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
	// Get a connection matching the target (waits for discovery if needed)
	conn, err := pg.loadBalancer.GetConnectionContext(ctx, target, nil)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.Describe(ctx, target, preparedStatement, portal, options)
}

// Close implements queryservice.QueryService.
// It closes all connections to poolers.
func (pg *PoolerGateway) Close() error {
	return pg.loadBalancer.Close()
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	return map[string]any{
		"active_connections": pg.loadBalancer.ConnectionCount(),
	}
}
