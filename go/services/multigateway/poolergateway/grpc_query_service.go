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

package poolergateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"

	"google.golang.org/grpc"
)

// grpcQueryService implements queryservice.QueryService using gRPC to communicate with a multipooler instance.
// This is a private implementation used internally by PoolerGateway.
type grpcQueryService struct {
	// conn is the gRPC connection to the multipooler
	conn *grpc.ClientConn

	// client is the generated gRPC client
	client multipoolerservice.MultiPoolerServiceClient

	// logger for debugging
	logger *slog.Logger

	// poolerID for logging
	poolerID string

	// copyStreamsMu protects copyStreams map
	copyStreamsMu sync.Mutex

	// copyStreams maps reserved connection IDs to active bidirectional streams for COPY operations
	copyStreams map[uint64]multipoolerservice.MultiPoolerService_CopyBidiExecuteClient
}

// newGRPCQueryService creates a new QueryService that uses gRPC to communicate
// with a multipooler instance.
func newGRPCQueryService(
	conn *grpc.ClientConn,
	poolerID string,
	logger *slog.Logger,
) queryservice.QueryService {
	return &grpcQueryService{
		conn:        conn,
		client:      multipoolerservice.NewMultiPoolerServiceClient(conn),
		logger:      logger,
		poolerID:    poolerID,
		copyStreams: make(map[uint64]multipoolerservice.MultiPoolerService_CopyBidiExecuteClient),
	}
}

// StreamExecute executes a query and streams results back via callback.
func (g *grpcQueryService) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	g.logger.DebugContext(ctx, "streaming query execution",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Create the request
	req := &multipoolerservice.StreamExecuteRequest{
		Query:   sql,
		Target:  target,
		Options: options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC StreamExecute
	stream, err := g.client.StreamExecute(ctx, req)
	if err != nil {
		// Convert gRPC error - if it's a PostgreSQL error, preserve it
		grpcErr := mterrors.FromGRPC(err)
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(grpcErr, &pgDiag) {
			return grpcErr
		}
		return mterrors.Wrapf(grpcErr, "failed to start stream execute")
	}

	// Stream results back via callback
	for {
		payload, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Stream completed successfully
			g.logger.DebugContext(ctx, "stream completed", "pooler_id", g.poolerID)
			return nil
		}
		if err != nil {
			// Convert gRPC error - if it's a PostgreSQL error, preserve it
			grpcErr := mterrors.FromGRPC(err)
			var pgDiag *mterrors.PgDiagnostic
			if errors.As(grpcErr, &pgDiag) {
				return grpcErr
			}
			return mterrors.Wrapf(grpcErr, "stream receive error")
		}

		// Handle the union type payload
		switch p := payload.GetPayload().(type) {
		case *query.QueryResultPayload_Result:
			// Row data - convert and send to callback
			result := sqltypes.ResultFromProto(p.Result)
			if err := callback(ctx, result); err != nil {
				g.logger.DebugContext(ctx, "callback returned error, stopping stream",
					"pooler_id", g.poolerID,
					"error", err)
				return err
			}
		case *query.QueryResultPayload_Diagnostic:
			// Diagnostic (notice or error) - convert to Result with notice for backwards compat
			diag := mterrors.PgDiagnosticFromProto(p.Diagnostic)
			noticeResult := &sqltypes.Result{
				Notices: []*mterrors.PgDiagnostic{diag},
			}
			if err := callback(ctx, noticeResult); err != nil {
				g.logger.DebugContext(ctx, "callback returned error on notice, stopping stream",
					"pooler_id", g.poolerID,
					"error", err)
				return err
			}
		default:
			g.logger.WarnContext(ctx, "received response with unknown payload type", "pooler_id", g.poolerID)
		}
	}
}

// ExecuteQuery implements queryservice.QueryService.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (g *grpcQueryService) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	g.logger.DebugContext(ctx, "Executing query",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Create the request
	req := &multipoolerservice.ExecuteQueryRequest{
		Query:   sql,
		Target:  target,
		Options: options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC ExecuteQuery
	res, err := g.client.ExecuteQuery(ctx, req)
	if err != nil {
		return nil, err
	}
	// Convert proto result to sqltypes (preserves NULL vs empty string)
	return sqltypes.ResultFromProto(res.GetResult()), nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// Returns ReservedState containing information about the reserved connection used for this execution.
func (g *grpcQueryService) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	g.logger.DebugContext(ctx, "portal stream execute",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"portal", portal.Name)

	// Create the request
	req := &multipoolerservice.PortalStreamExecuteRequest{
		Target:            target,
		PreparedStatement: preparedStatement,
		Portal:            portal,
		Options:           options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC PortalStreamExecute
	stream, err := g.client.PortalStreamExecute(ctx, req)
	if err != nil {
		// Convert gRPC error - if it's a PostgreSQL error, preserve it
		grpcErr := mterrors.FromGRPC(err)
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(grpcErr, &pgDiag) {
			return queryservice.ReservedState{}, grpcErr
		}
		return queryservice.ReservedState{}, mterrors.Wrapf(grpcErr, "failed to start portal stream execute")
	}

	var reservedState queryservice.ReservedState

	// Stream results back via callback
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Stream completed successfully
			g.logger.DebugContext(ctx, "portal stream completed", "pooler_id", g.poolerID)
			return reservedState, nil
		}
		if err != nil {
			// Convert gRPC error - if it's a PostgreSQL error, preserve it
			grpcErr := mterrors.FromGRPC(err)
			var pgDiag *mterrors.PgDiagnostic
			if errors.As(grpcErr, &pgDiag) {
				return reservedState, grpcErr
			}
			return reservedState, mterrors.Wrapf(grpcErr, "portal stream receive error")
		}

		// Extract reserved state if present
		if response.ReservedConnectionId != 0 {
			reservedState.ReservedConnectionId = response.ReservedConnectionId
			reservedState.PoolerID = response.PoolerId
			g.logger.DebugContext(ctx, "received reserved connection",
				"reserved_connection_id", response.ReservedConnectionId,
				"pooler_id", response.PoolerId.String())
		}

		// Handle the union type payload (if present)
		if response.Result != nil {
			switch p := response.Result.GetPayload().(type) {
			case *query.QueryResultPayload_Result:
				// Row data - convert and send to callback
				result := sqltypes.ResultFromProto(p.Result)
				if err := callback(ctx, result); err != nil {
					g.logger.DebugContext(ctx, "callback returned error, stopping stream",
						"pooler_id", g.poolerID,
						"error", err)
					return reservedState, err
				}
			case *query.QueryResultPayload_Diagnostic:
				// Diagnostic (notice or error) - convert to Result with notice for backwards compat
				diag := mterrors.PgDiagnosticFromProto(p.Diagnostic)
				noticeResult := &sqltypes.Result{
					Notices: []*mterrors.PgDiagnostic{diag},
				}
				if err := callback(ctx, noticeResult); err != nil {
					g.logger.DebugContext(ctx, "callback returned error on notice, stopping stream",
						"pooler_id", g.poolerID,
						"error", err)
					return reservedState, err
				}
			default:
				g.logger.WarnContext(ctx, "received response with unknown payload type", "pooler_id", g.poolerID)
			}
		}
	}
}

// Describe returns metadata about a prepared statement or portal.
func (g *grpcQueryService) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	g.logger.DebugContext(ctx, "describing statement/portal",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String())

	// Create the request
	req := &multipoolerservice.DescribeRequest{
		Target:            target,
		PreparedStatement: preparedStatement,
		Portal:            portal,
		Options:           options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC Describe
	response, err := g.client.Describe(ctx, req)
	if err != nil {
		// Convert gRPC error - if it's a PostgreSQL error, preserve it
		grpcErr := mterrors.FromGRPC(err)
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(grpcErr, &pgDiag) {
			return nil, grpcErr
		}
		return nil, mterrors.Wrapf(grpcErr, "describe failed")
	}

	g.logger.DebugContext(ctx, "describe completed successfully", "pooler_id", g.poolerID)
	return response.Description, nil
}

// Close closes the gRPC connection.
func (g *grpcQueryService) Close(ctx context.Context) error {
	g.logger.DebugContext(ctx, "closing gRPC query service", "pooler_id", g.poolerID)
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// CopyReady initiates a COPY FROM STDIN operation and returns format information.
// The stream is stored internally and can be accessed via options.ReservedConnectionId in subsequent calls.
func (g *grpcQueryService) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
) (int16, []int16, queryservice.ReservedState, error) {
	g.logger.DebugContext(ctx, "initiating COPY",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", copyQuery)

	// Start the bidirectional stream
	stream, err := g.client.CopyBidiExecute(ctx)
	if err != nil {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to start bidirectional execute stream: %w", err)
	}

	// Ensure stream is closed if we fail before adding it to copyStreams
	success := false
	defer func() {
		if !success {
			_ = stream.CloseSend()
			// Drain any pending response to allow server-side cleanup
			_, _ = stream.Recv()
		}
	}()

	// Send INITIATE message
	initiateReq := &multipoolerservice.CopyBidiExecuteRequest{
		Phase:   multipoolerservice.CopyBidiExecuteRequest_INITIATE,
		Query:   copyQuery,
		Target:  target,
		Options: options,
	}

	if err := stream.Send(initiateReq); err != nil {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to send INITIATE: %w", err)
	}

	g.logger.DebugContext(ctx, "sent INITIATE message", "pooler_id", g.poolerID)

	// Receive READY response
	resp, err := stream.Recv()
	if err != nil {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to receive READY response: %w", err)
	}

	// Check for ERROR response
	if resp.Phase == multipoolerservice.CopyBidiExecuteResponse_ERROR {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("COPY initiation failed: %s", resp.Error)
	}

	// Validate READY response
	if resp.Phase != multipoolerservice.CopyBidiExecuteResponse_READY {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("expected READY, got %v", resp.Phase)
	}

	// Convert columnFormats from []int32 to []int16
	columnFormats := make([]int16, len(resp.ColumnFormats))
	for i, f := range resp.ColumnFormats {
		columnFormats[i] = int16(f)
	}

	reservedState := queryservice.ReservedState{
		ReservedConnectionId: resp.ReservedConnectionId,
		PoolerID:             resp.PoolerId,
	}

	// Store the stream keyed by reserved connection ID
	g.copyStreamsMu.Lock()
	g.copyStreams[resp.ReservedConnectionId] = stream
	success = true // Commit point: stream is now managed by copyStreams
	g.copyStreamsMu.Unlock()

	g.logger.DebugContext(ctx, "received READY response",
		"pooler_id", g.poolerID,
		"reserved_conn_id", resp.ReservedConnectionId,
		"format", resp.Format,
		"num_columns", len(columnFormats))

	return int16(resp.Format), columnFormats, reservedState, nil
}

// CopySendData sends a chunk of data for an active COPY operation.
func (g *grpcQueryService) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return errors.New("options.ReservedConnectionId is required for CopySendData")
	}

	// Look up the stream by reserved connection ID
	g.copyStreamsMu.Lock()
	stream, ok := g.copyStreams[options.ReservedConnectionId]
	g.copyStreamsMu.Unlock()

	if !ok {
		return fmt.Errorf("no active COPY stream for reserved connection %d", options.ReservedConnectionId)
	}

	dataReq := &multipoolerservice.CopyBidiExecuteRequest{
		Phase: multipoolerservice.CopyBidiExecuteRequest_DATA,
		Data:  data,
	}

	if err := stream.Send(dataReq); err != nil {
		return fmt.Errorf("failed to send DATA: %w", err)
	}

	g.logger.DebugContext(ctx, "sent DATA message",
		"pooler_id", g.poolerID,
		"reserved_conn_id", options.ReservedConnectionId,
		"size", len(data))

	return nil
}

// CopyFinalize completes a COPY operation, sending final data and returning the result.
func (g *grpcQueryService) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, errors.New("options.ReservedConnectionId is required for CopyFinalize")
	}

	// Look up and remove the stream by reserved connection ID
	g.copyStreamsMu.Lock()
	stream, ok := g.copyStreams[options.ReservedConnectionId]
	if ok {
		delete(g.copyStreams, options.ReservedConnectionId)
	}
	g.copyStreamsMu.Unlock()

	if !ok {
		return nil, fmt.Errorf("no active COPY stream for reserved connection %d", options.ReservedConnectionId)
	}

	// Send DONE message with final data
	doneReq := &multipoolerservice.CopyBidiExecuteRequest{
		Phase: multipoolerservice.CopyBidiExecuteRequest_DONE,
		Data:  finalData,
	}

	if err := stream.Send(doneReq); err != nil {
		return nil, fmt.Errorf("failed to send DONE: %w", err)
	}

	// Close send direction
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close send: %w", err)
	}

	g.logger.DebugContext(ctx, "sent DONE message",
		"pooler_id", g.poolerID,
		"reserved_conn_id", options.ReservedConnectionId,
		"final_data_size", len(finalData))

	// Receive RESULT response
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive RESULT response: %w", err)
	}

	// Check for ERROR response
	if resp.Phase == multipoolerservice.CopyBidiExecuteResponse_ERROR {
		return nil, fmt.Errorf("COPY finalization failed: %s", resp.Error)
	}

	// Validate RESULT response
	if resp.Phase != multipoolerservice.CopyBidiExecuteResponse_RESULT {
		return nil, fmt.Errorf("expected RESULT, got %v", resp.Phase)
	}

	result := sqltypes.ResultFromProto(resp.Result)

	g.logger.DebugContext(ctx, "received RESULT response",
		"pooler_id", g.poolerID,
		"reserved_conn_id", options.ReservedConnectionId)

	return result, nil
}

// CopyAbort aborts a COPY operation.
func (g *grpcQueryService) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return errors.New("options.ReservedConnectionId is required for CopyAbort")
	}

	// Look up and remove the stream by reserved connection ID
	g.copyStreamsMu.Lock()
	stream, ok := g.copyStreams[options.ReservedConnectionId]
	if ok {
		delete(g.copyStreams, options.ReservedConnectionId)
	}
	g.copyStreamsMu.Unlock()

	if !ok {
		// Already cleaned up or never existed - that's okay for abort
		g.logger.DebugContext(ctx, "COPY stream already cleaned up",
			"pooler_id", g.poolerID,
			"reserved_conn_id", options.ReservedConnectionId)
		return nil
	}

	// Send FAIL message
	failReq := &multipoolerservice.CopyBidiExecuteRequest{
		Phase:        multipoolerservice.CopyBidiExecuteRequest_FAIL,
		ErrorMessage: errorMsg,
	}

	if err := stream.Send(failReq); err != nil {
		// Log but don't return error - we're already aborting
		g.logger.DebugContext(ctx, "failed to send FAIL message",
			"pooler_id", g.poolerID,
			"reserved_conn_id", options.ReservedConnectionId,
			"error", err)
	}

	// Close send direction
	if err := stream.CloseSend(); err != nil {
		g.logger.DebugContext(ctx, "failed to close send after FAIL",
			"pooler_id", g.poolerID,
			"reserved_conn_id", options.ReservedConnectionId,
			"error", err)
	}

	// Try to receive response (may be ERROR)
	_, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		g.logger.DebugContext(ctx, "error receiving response after FAIL (expected)",
			"pooler_id", g.poolerID,
			"reserved_conn_id", options.ReservedConnectionId,
			"error", err)
	}

	g.logger.DebugContext(ctx, "COPY aborted",
		"pooler_id", g.poolerID,
		"reserved_conn_id", options.ReservedConnectionId)

	return nil
}

// ReserveStreamExecute creates a reserved connection and executes a query.
// Based on reservationOptions.Reasons bitmask, may execute BEGIN before the query.
func (g *grpcQueryService) ReserveStreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *multipoolerservice.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	reasons := protoutil.GetReasons(reservationOptions)

	g.logger.DebugContext(ctx, "reserve stream execute",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"reasons", protoutil.ReasonsString(reasons),
		"query", sql)

	// Create the request
	req := &multipoolerservice.ReserveStreamExecuteRequest{
		Query:              sql,
		Target:             target,
		Options:            options,
		ReservationOptions: reservationOptions,
	}

	// Call the gRPC ReserveStreamExecute
	stream, err := g.client.ReserveStreamExecute(ctx, req)
	if err != nil {
		// Convert gRPC error - if it's a PostgreSQL error, preserve it
		grpcErr := mterrors.FromGRPC(err)
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(grpcErr, &pgDiag) {
			return queryservice.ReservedState{}, grpcErr
		}
		return queryservice.ReservedState{}, mterrors.Wrapf(grpcErr, "failed to start reserve stream execute")
	}

	var reservedState queryservice.ReservedState

	// Stream results back via callback
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			g.logger.DebugContext(ctx, "reserve stream completed", "pooler_id", g.poolerID)
			return reservedState, nil
		}
		if err != nil {
			// Convert gRPC error - if it's a PostgreSQL error, preserve it
			grpcErr := mterrors.FromGRPC(err)
			var pgDiag *mterrors.PgDiagnostic
			if errors.As(grpcErr, &pgDiag) {
				return reservedState, grpcErr
			}
			return reservedState, mterrors.Wrapf(grpcErr, "reserve stream receive error")
		}

		// Extract reserved state from response
		if response.ReservedConnectionId != 0 && reservedState.ReservedConnectionId == 0 {
			reservedState.ReservedConnectionId = response.ReservedConnectionId
			reservedState.PoolerID = response.PoolerId
			g.logger.DebugContext(ctx, "received reserved connection",
				"reserved_connection_id", response.ReservedConnectionId)
		}

		// Extract result from response
		if response.Result == nil {
			continue
		}

		result := sqltypes.ResultFromProto(response.Result)
		if err := callback(ctx, result); err != nil {
			return reservedState, err
		}
	}
}

// ConcludeTransaction concludes a transaction on a reserved connection.
// Returns the result and the remaining reservation reasons (0 if released).
func (g *grpcQueryService) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerservice.TransactionConclusion,
) (*sqltypes.Result, uint32, error) {
	g.logger.DebugContext(ctx, "conclude transaction",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"conclusion", conclusion.String(),
		"reserved_conn_id", options.ReservedConnectionId)

	// Create the request
	req := &multipoolerservice.ConcludeTransactionRequest{
		Target:     target,
		Options:    options,
		Conclusion: conclusion,
	}

	// Call the gRPC ConcludeTransaction
	response, err := g.client.ConcludeTransaction(ctx, req)
	if err != nil {
		return nil, 0, fmt.Errorf("conclude transaction failed: %w", err)
	}

	result := sqltypes.ResultFromProto(response.Result)
	remainingReasons := response.RemainingReasons

	if remainingReasons == 0 {
		g.logger.DebugContext(ctx, "transaction concluded, connection released",
			"pooler_id", g.poolerID,
			"command_tag", result.CommandTag)
	} else {
		g.logger.DebugContext(ctx, "transaction concluded, connection still reserved",
			"pooler_id", g.poolerID,
			"command_tag", result.CommandTag,
			"remaining_reasons", protoutil.ReasonsString(remainingReasons))
	}

	return result, remainingReasons, nil
}

// ReleaseReservedConnection forcefully releases a reserved connection.
func (g *grpcQueryService) ReleaseReservedConnection(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil
	}

	g.logger.DebugContext(ctx, "releasing reserved connection",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"reserved_conn_id", options.ReservedConnectionId)

	// Clean up any stale COPY stream for this connection.
	g.copyStreamsMu.Lock()
	if stream, ok := g.copyStreams[options.ReservedConnectionId]; ok {
		delete(g.copyStreams, options.ReservedConnectionId)
		_ = stream.CloseSend()
	}
	g.copyStreamsMu.Unlock()

	req := &multipoolerservice.ReleaseReservedConnectionRequest{
		Target:  target,
		Options: options,
	}

	_, err := g.client.ReleaseReservedConnection(ctx, req)
	if err != nil {
		return fmt.Errorf("release reserved connection failed: %w", err)
	}

	g.logger.DebugContext(ctx, "reserved connection released",
		"pooler_id", g.poolerID,
		"reserved_conn_id", options.ReservedConnectionId)

	return nil
}

// Ensure grpcQueryService implements queryservice.QueryService
var _ queryservice.QueryService = (*grpcQueryService)(nil)
