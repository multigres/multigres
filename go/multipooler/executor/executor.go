// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package executor implements query execution for multipooler.
// It provides the QueryService interface implementation that executes queries
// against PostgreSQL using per-user connection pools.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
	"github.com/multigres/multigres/go/pb/query"
)

// Executor implements the QueryService interface for executing queries against PostgreSQL.
// It uses the connpoolmanager for per-user connection pool management and consolidates
// prepared statements across connections to avoid redundant parsing.
type Executor struct {
	logger       *slog.Logger
	poolManager  connpoolmanager.PoolManager
	consolidator *preparedstatement.Consolidator
}

// NewExecutor creates a new Executor instance.
func NewExecutor(logger *slog.Logger, poolManager connpoolmanager.PoolManager) *Executor {
	return &Executor{
		logger:       logger,
		poolManager:  poolManager,
		consolidator: preparedstatement.NewConsolidator(),
	}
}

// ExecuteQuery implements queryservice.QueryService.
// It executes a query using a pooled connection for the specified user.
// If ReservedConnectionId is set in options, uses that reserved connection instead.
func (e *Executor) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	if target == nil {
		target = &query.Target{}
	}

	user := e.getUserFromOptions(options)
	e.logger.DebugContext(ctx, "executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"user", user,
		"query", sql)

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}

		results, err := reservedConn.Query(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}

		// Capture any ParameterStatus changes from the backend
		paramStatus := reservedConn.Conn().RawConn().GetParameterStatus()

		if len(results) == 0 {
			result := &sqltypes.Result{}
			result.ParameterStatus = paramStatus
			return result, nil
		}
		results[0].ParameterStatus = paramStatus
		return results[0], nil
	}

	// Get session settings from options
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	// Get a connection from the pool for this user
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Execute the query - the regular.Conn.Query returns []*sqltypes.Result
	// with proper field info, rows, and command tags already populated
	results, err := conn.Conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Capture any ParameterStatus changes from the backend (e.g., SET commands)
	paramStatus := conn.Conn.RawConn().GetParameterStatus()

	// Return first result (simple query returns single result)
	if len(results) == 0 {
		result := &sqltypes.Result{}
		result.ParameterStatus = paramStatus
		return result, nil
	}
	results[0].ParameterStatus = paramStatus
	return results[0], nil
}

// StreamExecute executes a query and streams results back via callback.
// This implements the queryservice.QueryService interface.
// If ReservedConnectionId is set in options, uses that reserved connection instead.
func (e *Executor) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if target == nil {
		target = &query.Target{}
	}

	user := e.getUserFromOptions(options)
	e.logger.DebugContext(ctx, "stream executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"user", user,
		"query", sql)

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}

		if err := reservedConn.QueryStreaming(ctx, sql, callback); err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}

		// Send any ParameterStatus changes from the backend
		if ps := reservedConn.Conn().RawConn().GetParameterStatus(); len(ps) > 0 {
			if err := callback(ctx, &sqltypes.Result{ParameterStatus: ps}); err != nil {
				return err
			}
		}
		return nil
	}

	// Get session settings from options
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	// Get a connection from the pool for this user
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Use streaming query execution
	if err := conn.Conn.QueryStreaming(ctx, sql, callback); err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Send any ParameterStatus changes from the backend (e.g., SET commands)
	if ps := conn.Conn.RawConn().GetParameterStatus(); len(ps) > 0 {
		if err := callback(ctx, &sqltypes.Result{ParameterStatus: ps}); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the executor and releases resources.
// Note: The poolManager is managed by the caller (QueryPoolerServer), not closed here.
func (e *Executor) Close(_ context.Context) error {
	return nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// If MaxRows > 0, a reserved connection is used since the portal may be suspended and need resumption.
// Otherwise, a regular connection is used for better pool efficiency.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	if target == nil {
		target = &query.Target{}
	}
	if preparedStatement == nil {
		return queryservice.ReservedState{}, errors.New("prepared statement is required")
	}
	if portal == nil {
		return queryservice.ReservedState{}, errors.New("portal is required")
	}

	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	maxRows := int32(0)
	if options != nil && options.MaxRows > 0 {
		maxRows = int32(options.MaxRows)
	}

	e.logger.DebugContext(ctx, "portal stream execute",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"user", user,
		"statement", preparedStatement.Name,
		"portal", portal.Name,
		"max_rows", maxRows)

	// Convert formats from int32 to int16
	paramFormats := int32ToInt16Slice(portal.ParamFormats)
	resultFormats := int32ToInt16Slice(portal.ResultFormats)

	// Use reserved connection if:
	// 1. ReservedConnectionId is already set (e.g., from transaction or previous portal)
	// 2. MaxRows > 0 (portal may be suspended and need resumption)
	if (options != nil && options.ReservedConnectionId > 0) || maxRows > 0 {
		return e.portalExecuteWithReserved(ctx, preparedStatement, portal, options, settings, user, maxRows, paramFormats, resultFormats, callback)
	}

	// Use regular connection for non-suspended execution with no existing reservation
	return e.portalExecuteWithRegular(ctx, preparedStatement, portal, settings, user, paramFormats, resultFormats, callback)
}

// portalExecuteWithReserved executes a portal using a reserved connection.
func (e *Executor) portalExecuteWithReserved(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	settings map[string]string,
	user string,
	maxRows int32,
	paramFormats, resultFormats []int16,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	var reservedConn *reserved.Conn
	var err error

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return queryservice.ReservedState{}, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}
	} else {
		// Create a new reserved connection
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user)
		if err != nil {
			return queryservice.ReservedState{}, fmt.Errorf("failed to create reserved connection for user %s: %w", user, err)
		}
	}

	// Ensure the statement is prepared on this connection (with consolidation)
	canonicalName, err := e.ensurePrepared(ctx, reservedConn.Conn(), preparedStatement)
	if err != nil {
		reservedConn.Release(reserved.ReleaseError)
		return queryservice.ReservedState{}, err
	}

	// Bind and execute using the canonical statement name
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	completed, err := reservedConn.BindAndExecute(ctx, canonicalName, params, paramFormats, resultFormats, maxRows, callback)
	if err != nil {
		reservedConn.Release(reserved.ReleaseError)
		return queryservice.ReservedState{}, fmt.Errorf("failed to execute portal: %w", err)
	}

	// If portal is suspended (not completed), keep the reserved connection for continuation
	if !completed {
		reservedConn.ReserveForPortal(portal.Name)
	} else {
		// Portal completed, release this portal's reservation
		shouldRelease := reservedConn.ReleasePortal(portal.Name)
		// If no more portal reservations and not in a transaction, release the connection
		if shouldRelease && !reservedConn.IsInTransaction() {
			reservedConn.Release(reserved.ReleasePortalComplete)
		}
	}

	return queryservice.ReservedState{
		ReservedConnectionId: uint64(reservedConn.ConnID),
	}, nil
}

// portalExecuteWithRegular executes a portal using a regular pooled connection.
func (e *Executor) portalExecuteWithRegular(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	settings map[string]string,
	user string,
	paramFormats, resultFormats []int16,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return queryservice.ReservedState{}, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Ensure the statement is prepared on this connection (with consolidation)
	canonicalName, err := e.ensurePrepared(ctx, conn.Conn, preparedStatement)
	if err != nil {
		return queryservice.ReservedState{}, err
	}

	// Bind and execute with maxRows=0 (fetch all) using the canonical statement name
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	_, err = conn.Conn.BindAndExecute(ctx, canonicalName, params, paramFormats, resultFormats, 0, callback)
	if err != nil {
		return queryservice.ReservedState{}, fmt.Errorf("failed to execute portal: %w", err)
	}

	// No reserved connection for regular execution
	return queryservice.ReservedState{}, nil
}

// Describe returns metadata about a prepared statement or portal.
func (e *Executor) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	if target == nil {
		target = &query.Target{}
	}

	if preparedStatement == nil {
		return nil, errors.New("no prepared statement provided")
	}

	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	e.logger.DebugContext(ctx, "describe",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"user", user,
		"has_statement", preparedStatement != nil,
		"has_portal", portal != nil)

	// Get a connection from the pool
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Describe prepared statement
	// Ensure the statement is prepared on this connection
	canonicalName, err := e.ensurePrepared(ctx, conn.Conn, preparedStatement)
	if err != nil {
		return nil, err
	}

	// If portal is provided, we need to bind first, then describe
	if portal != nil {
		// Bind and describe using canonical name
		paramFormats := int32ToInt16Slice(portal.ParamFormats)
		resultFormats := int32ToInt16Slice(portal.ResultFormats)
		params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
		desc, err := conn.Conn.BindAndDescribe(ctx, canonicalName, params, paramFormats, resultFormats)
		if err != nil {
			return nil, fmt.Errorf("failed to describe portal: %w", err)
		}
		return desc, nil
	}

	// Describe prepared using canonical name
	desc, err := conn.Conn.DescribePrepared(ctx, canonicalName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe prepared statement: %w", err)
	}
	return desc, nil
}

// ensurePrepared ensures the prepared statement is available on the connection.
// It uses the consolidator to get a canonical statement name and checks the connection state
// to avoid redundant parsing. Returns the canonical statement name to use.
func (e *Executor) ensurePrepared(ctx context.Context, conn *regular.Conn, stmt *query.PreparedStatement) (string, error) {
	// We use connId 0 since we just need the canonical name mapping
	// Get the canonical prepared statement info
	psi := e.consolidator.GetPreparedStatementInfo(0, stmt.Name)
	if psi == nil {
		// Add to consolidator to get/create canonical name
		var err error
		psi, err = e.consolidator.AddPreparedStatement(0, stmt.Name, stmt.Query, stmt.ParamTypes)
		if err != nil {
			return "", fmt.Errorf("failed to consolidate prepared statement: %w", err)
		}
	}
	canonicalName := psi.Name

	// Check if this connection already has the statement prepared
	connState := conn.State()
	existing := connState.GetPreparedStatement(canonicalName)
	if existing != nil && existing.Query == stmt.Query {
		// Statement already prepared on this connection, reuse it
		return canonicalName, nil
	}

	// Parse the statement on this connection
	if err := conn.Parse(ctx, canonicalName, stmt.Query, stmt.ParamTypes); err != nil {
		return "", fmt.Errorf("failed to parse statement: %w", err)
	}

	// Store in connection state for future reuse
	connState.StorePreparedStatement(&query.PreparedStatement{
		Name:       canonicalName,
		Query:      stmt.Query,
		ParamTypes: stmt.ParamTypes,
	})

	return canonicalName, nil
}

// CopyReady initiates a COPY FROM STDIN operation and returns format information.
// Uses an existing reserved connection if ReservedConnectionId is set in options,
// otherwise creates a new reserved connection (COPY requires connection affinity).
func (e *Executor) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
) (int16, []int16, queryservice.ReservedState, error) {
	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	e.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", copyQuery,
		"user", user)

	var reservedConn *reserved.Conn
	var err error

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return 0, nil, queryservice.ReservedState{}, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}
	} else {
		// Create a new reserved connection (COPY requires connection affinity)
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user)
		if err != nil {
			return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to create reserved connection for COPY: %w", err)
		}
	}

	connID := reservedConn.ConnID

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send COPY command and read CopyInResponse
	format, columnFormats, err := conn.InitiateCopyFromStdin(ctx, copyQuery)
	if err != nil {
		// Connection is in bad state after failed COPY initiation - close it instead of recycling
		reservedConn.Close()
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to initiate COPY FROM STDIN: %w", err)
	}

	e.logger.DebugContext(ctx, "COPY INITIATE successful",
		"conn_id", connID,
		"format", format,
		"num_columns", len(columnFormats))

	reservedState := queryservice.ReservedState{
		ReservedConnectionId: uint64(connID),
		PoolerID:             nil, // TODO: implement pooler ID retrieval
	}

	return format, columnFormats, reservedState, nil
}

// CopySendData sends a chunk of data for an active COPY operation.
func (e *Executor) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return errors.New("options.ReservedConnectionId is required for CopySendData")
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		return fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
	}

	e.logger.DebugContext(ctx, "sending COPY data",
		"data_size", len(data),
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Write CopyData to PostgreSQL
	if err := conn.WriteCopyData(data); err != nil {
		e.logger.ErrorContext(ctx, "failed to write COPY data",
			"error", err,
			"data_size", len(data))
		return fmt.Errorf("failed to write COPY data: %w", err)
	}

	e.logger.DebugContext(ctx, "COPY DATA sent successfully",
		"data_size", len(data))

	return nil
}

// CopyFinalize completes a COPY operation, sending final data and returning the result.
func (e *Executor) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, errors.New("options.ReservedConnectionId is required for CopyFinalize")
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		return nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
	}

	e.logger.DebugContext(ctx, "finalizing COPY",
		"final_data_size", len(finalData),
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send any remaining data first
	if len(finalData) > 0 {
		if err := conn.WriteCopyData(finalData); err != nil {
			e.logger.ErrorContext(ctx, "failed to write final COPY data", "error", err)
			// Connection is in bad state - close it instead of recycling
			reservedConn.Close()
			return nil, fmt.Errorf("failed to write final COPY data: %w", err)
		}
		e.logger.DebugContext(ctx, "sent final COPY data", "size", len(finalData))
	}

	// Send CopyDone to signal completion
	if err := conn.WriteCopyDone(); err != nil {
		e.logger.ErrorContext(ctx, "failed to write CopyDone", "error", err)
		// Connection is in bad state - close it instead of recycling
		reservedConn.Close()
		return nil, fmt.Errorf("failed to write CopyDone: %w", err)
	}

	// Read CommandComplete response from PostgreSQL
	commandTag, rowsAffected, err := conn.ReadCopyDoneResponse(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "COPY operation failed", "error", err)
		// Connection might be in bad state - close it instead of recycling
		reservedConn.Close()
		return nil, fmt.Errorf("COPY operation failed: %w", err)
	}

	e.logger.DebugContext(ctx, "COPY DONE successful",
		"rows_affected", rowsAffected,
		"command_tag", commandTag)

	// Build result
	result := &sqltypes.Result{
		CommandTag:   commandTag,
		RowsAffected: rowsAffected,
	}

	// Success - release connection back to pool for reuse
	reservedConn.Release(reserved.ReleasePortalComplete)

	return result, nil
}

// CopyAbort aborts a COPY operation.
func (e *Executor) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		// Already cleaned up or never initiated
		return nil
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		// Already cleaned up
		e.logger.DebugContext(ctx, "COPY connection already cleaned up",
			"conn_id", options.ReservedConnectionId)
		return nil
	}

	e.logger.DebugContext(ctx, "aborting COPY",
		"error", errorMsg,
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send CopyFail to abort the operation
	writeFailed := false
	if err := conn.WriteCopyFail(errorMsg); err != nil {
		e.logger.ErrorContext(ctx, "failed to write CopyFail", "error", err)
		writeFailed = true
		// Continue to try reading response
	}

	// Read ErrorResponse from PostgreSQL
	// After CopyFail, PostgreSQL should respond with ErrorResponse and ReadyForQuery
	// We can try to read it, but if it fails, that's okay since we're aborting anyway
	_, _, readErr := conn.ReadCopyDoneResponse(ctx)
	if readErr != nil {
		e.logger.DebugContext(ctx, "error reading response after CopyFail (expected)", "error", readErr)
	}

	e.logger.DebugContext(ctx, "COPY FAIL completed")

	// If write or read failed, connection might be in bad state - close it
	if writeFailed || readErr != nil {
		reservedConn.Close()
	} else {
		// Clean abort - release connection back to pool
		reservedConn.Release(reserved.ReleasePortalComplete)
	}

	return nil
}

// getUserFromOptions extracts the user from ExecuteOptions.
// Returns "postgres" as default if no user is specified.
func (e *Executor) getUserFromOptions(options *query.ExecuteOptions) string {
	if options != nil && options.User != "" {
		return options.User
	}
	// Default to postgres superuser if no user specified
	return "postgres"
}

// int32ToInt16Slice converts a slice of int32 to int16.
func int32ToInt16Slice(in []int32) []int16 {
	if in == nil {
		return nil
	}
	out := make([]int16, len(in))
	for i, v := range in {
		out[i] = int16(v)
	}
	return out
}

// Ensure Executor implements queryservice.QueryService
var _ queryservice.QueryService = (*Executor)(nil)
