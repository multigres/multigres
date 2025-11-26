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

// Package scatterconn handles coordinated query execution across multiple
// multipooler instances. It implements the IExecute interface from the engine
// package and is responsible for:
// - Selecting appropriate poolers for a given tablegroup
// - Executing queries via gRPC
// - Streaming results back
// - Handling failures and retries
package scatterconn

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/multigateway/engine"
	"github.com/multigres/multigres/go/multigateway/handler"
	"github.com/multigres/multigres/go/multigateway/poolergateway"
	"github.com/multigres/multigres/go/multipooler/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/protoutil"
)

// ScatterConn coordinates query execution across multiple multipooler instances.
// It implements the engine.IExecute interface.
type ScatterConn struct {
	logger *slog.Logger

	// gateway is used for executing queries (typically a PoolerGateway)
	gateway poolergateway.Gateway
}

// NewScatterConn creates a new ScatterConn instance.
func NewScatterConn(gateway poolergateway.Gateway, logger *slog.Logger) *ScatterConn {
	return &ScatterConn{
		logger:  logger,
		gateway: gateway,
	}
}

// StreamExecute executes a query on the specified tablegroup and streams results.
// This is the implementation of engine.IExecute.StreamExecute().
// - Creates Target with tablegroup, shard, and PRIMARY pooler type
// - Uses PoolerGateway to select matching pooler
// - Executes query via gRPC to the pooler
// - Streams actual results back via callback
func (sc *ScatterConn) StreamExecute(
	ctx context.Context,
	tableGroup string,
	shard string,
	sql string,
	options *handler.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) error {
	sc.logger.DebugContext(ctx, "scatter conn executing query",
		"tablegroup", tableGroup,
		"shard", shard,
		"query", sql)

	// Create target for routing
	// TODO: Add query analysis to determine if this is a read or write query
	// For now, always route to PRIMARY (safe default)
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &query.ExecuteOptions{
		PreparedStatement: options.PreparedStatement,
		Portal:            options.Portal,
		MaxRows:           options.MaxRows,
	}

	var qs queryservice.QueryService = sc.gateway
	var err error

	ss := getMatchingShardState(options.ShardStates, target)
	// If we have a reserved connection, we have to ensure
	// we are routing the query to the pooler where we got the reserved
	// connection from. If a reparent happened, then we will get an error
	// back.
	if ss != nil && ss.ReservedConnectionId != 0 {
		eo.ReservedConnectionId = uint64(ss.ReservedConnectionId)
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.PoolerID, target)
	}
	if err != nil {
		return err
	}

	// Execute query via QueryService (PoolerGateway) and stream results
	// PoolerGateway will use the target to find the right pooler
	sc.logger.DebugContext(ctx, "executing query via query service",
		"tablegroup", tableGroup,
		"shard", shard,
		"pooler_type", target.PoolerType.String())

	if err := qs.StreamExecute(ctx, target, sql, eo, callback); err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	sc.logger.DebugContext(ctx, "query execution completed successfully",
		"tablegroup", tableGroup,
		"shard", shard)

	return nil
}

// getMatchingShardState gets the shardState (if any) that matches the target specified.
func getMatchingShardState(shardState []*handler.ShardState, target *query.Target) *handler.ShardState {
	for _, ss := range shardState {
		if protoutil.TargetEquals(ss.Target, target) {
			return ss
		}
	}
	return nil
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
