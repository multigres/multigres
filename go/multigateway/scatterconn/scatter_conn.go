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
	"github.com/multigres/multigres/go/pb/query"
)

// ScatterConn coordinates query execution across multiple multipooler instances.
// It implements the engine.IExecute interface.
type ScatterConn struct {
	logger *slog.Logger

	// TODO(Phase 3): Add PoolerGateway reference
	// gateway *poolergateway.PoolerGateway
}

// NewScatterConn creates a new ScatterConn instance.
func NewScatterConn(logger *slog.Logger) *ScatterConn {
	return &ScatterConn{
		logger: logger,
	}
}

// StreamExecute executes a query on the specified tablegroup and streams results.
// This is the implementation of engine.IExecute.StreamExecute().
//
// Phase 2 Implementation:
// - Returns stub results showing that ScatterConn was reached
// - Logs execution parameters for debugging
//
// Phase 3 will:
// - Use PoolerGateway to select a healthy multipooler
// - Execute query via gRPC to the pooler
// - Stream actual results back
func (sc *ScatterConn) StreamExecute(
	ctx context.Context,
	tableGroup string,
	database string,
	sql string,
	callback func(*query.QueryResult) error,
) error {
	sc.logger.Debug("scatter conn executing query",
		"tablegroup", tableGroup,
		"database", database,
		"query", sql)

	// TODO(Phase 3): Replace with actual pooler gateway execution
	// pooler, err := sc.gateway.GetPooler(ctx, tableGroup)
	// if err != nil {
	//     return fmt.Errorf("failed to get pooler for tablegroup %s: %w", tableGroup, err)
	// }
	//
	// return sc.executeOnPooler(ctx, pooler, database, sql, callback)

	// Phase 2: Return stub result showing ScatterConn was reached
	result := &query.QueryResult{
		Fields: []*query.Field{
			{
				Name:         "message",
				Type:         "text",
				DataTypeOid:  25, // text type OID
				DataTypeSize: -1, // variable length
				Format:       0,  // text format
			},
		},
		Rows: []*query.Row{
			{
				Values: [][]byte{
					[]byte(fmt.Sprintf("ScatterConn: tablegroup=%s, database=%s, query=%s",
						tableGroup, database, sql)),
				},
			},
		},
		CommandTag: "SELECT 1",
	}

	sc.logger.Debug("scatter conn returning stub result")
	return callback(result)
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
