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

// Package grpcpoolerservice implements the gRPC server for Multipooler
package grpcpoolerservice

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// poolerService is the gRPC wrapper for Multipooler
type poolerService struct {
	multipoolerpb.UnimplementedMultipoolerServiceServer
	pooler *poolerserver.QueryPoolerServer
}

func RegisterPoolerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the pooler starts
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, func(p *poolerserver.QueryPoolerServer) {
		if grpc.CheckServiceMap("pooler", senv) {
			srv := &poolerService{pooler: p}
			multipoolerpb.RegisterMultipoolerServiceServer(grpc.Server, srv)
		}
	})
}

// StreamExecute executes a SQL query and streams the results back to the client.
// This is the main execution method used by multigateway.
// When req.ReservationOptions has non-zero reasons, creates or extends a reserved connection.
func (s *poolerService) StreamExecute(req *multipoolerpb.StreamExecuteRequest, stream multipoolerpb.MultipoolerService_StreamExecuteServer) error {
	callerid.AnnotateSpan(stream.Context(), req.GetCallerId())
	// The pooler's query service admits the request and validates its
	// reservation reasons before running it.
	return s.streamExecuteTo(stream, s.pooler.QueryService(), req)
}

// streamExecuteTo runs the query on exec and writes the response sequence to
// stream. Split from StreamExecute so the wire sequence can be tested without a
// live pooler.
//
// The terminal chunk of each statement (the one carrying the CommandTag, which
// the client emits on CommandComplete) is held back until the next callback or
// the end of execution so the reservation state can ride on it instead of a
// separate trailing message. For a typical single-chunk result this turns two
// DATA frames (result, then reserved state) into one on the gateway<->pooler
// hop; each frame otherwise costs a loopy-writer wakeup, a flush and a socket
// write on both sides. Intermediate chunks of a large result (no CommandTag)
// are sent immediately, so streaming of multi-chunk results is unchanged, and
// a suspended portal or an empty query, which end without a CommandTag, keep
// sending the reservation state on its own. Notices are still sent ahead of the
// data they belong to. The gateway already accepts result and reserved state in
// the same message.
func (s *poolerService) streamExecuteTo(
	stream multipoolerpb.MultipoolerService_StreamExecuteServer,
	exec queryservice.QueryService,
	req *multipoolerpb.StreamExecuteRequest,
) error {
	var pending *multipoolerpb.StreamExecuteResponse
	flushPending := func() error {
		if pending == nil {
			return nil
		}
		resp := pending
		pending = nil
		return stream.Send(resp)
	}

	// Execute the query and stream results
	reservedState, err := exec.StreamExecute(stream.Context(), req.Target, req.Query, req.Options, req.GetReservationOptions(), func(ctx context.Context, result *sqltypes.Result) error {
		// Send notices first (if any) as separate diagnostic messages
		if len(result.Notices) > 0 {
			if err := flushPending(); err != nil {
				return err
			}
		}
		for _, notice := range result.Notices {
			noticePayload := &query.QueryResultPayload{
				Payload: &query.QueryResultPayload_Diagnostic{
					Diagnostic: mterrors.PgDiagnosticToProto(notice),
				},
			}
			resp := &multipoolerpb.StreamExecuteResponse{
				Result: noticePayload,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}

		// Send row data (if any). Notices are streamed above as separate
		// diagnostics, so keep the result payload notice-free to avoid duplicate
		// NoticeResponse frames on the gateway.
		if len(result.Rows) > 0 || len(result.PassthroughBlock) > 0 || result.CommandTag != "" || len(result.ParameterStatus) > 0 {
			if err := flushPending(); err != nil {
				return err
			}
			protoResult := result.ToProto()
			protoResult.Notices = nil
			resp := &multipoolerpb.StreamExecuteResponse{
				Result: &query.QueryResultPayload{
					Payload: &query.QueryResultPayload_Result{
						Result: protoResult,
					},
				},
			}
			if result.CommandTag == "" {
				// Intermediate chunk: nothing will be attached to it, stream it now.
				return stream.Send(resp)
			}
			pending = resp
		}
		return nil
	})

	// Attach the reserved state to the last data message when on a reserved
	// connection; if nothing is pending it goes out on its own as before.
	if reservedState.GetReservedConnectionId() > 0 {
		if pending == nil {
			pending = &multipoolerpb.StreamExecuteResponse{}
		}
		pending.ReservedState = reservedState
	}
	if err != nil {
		// The send error is intentionally discarded: if the stream is already
		// broken the gateway will clean up via ReleaseReservedConnection on
		// client disconnect. The execution error is what the gateway needs.
		_ = flushPending()
		// Convert errors to gRPC format, preserving PostgreSQL error details
		return mterrors.ToGRPC(err)
	}
	return mterrors.ToGRPC(flushPending())
}

// ExecuteQuery executes a SQL query and returns the result
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (s *poolerService) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	callerid.AnnotateSpan(ctx, req.GetCallerId())
	res, reservedState, err := s.pooler.QueryService().ExecuteQuery(ctx, req.Target, req.Query, req.Options)
	if err != nil {
		// Convert errors to gRPC format, preserving PostgreSQL error details
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolerpb.ExecuteQueryResponse{
		Result:        res.ToProto(),
		ReservedState: reservedState,
	}, nil
}

// GetAuthCredentials retrieves authentication credentials (SCRAM hash) for a PostgreSQL user.
// This is used by multigateway to authenticate clients using SCRAM-SHA-256.
//
// This method uses an admin connection directly since normally a non-superuser wouldn't
// have access to password hashes and at the time of this request we wouldn't have authenticated
// that we have permission to run queries under any other user's role.
func (s *poolerService) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	resp, err := s.pooler.GetAuthCredentials(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Describe returns metadata about a prepared statement or portal.
// Used by multigateway for the Extended Query Protocol.
func (s *poolerService) Describe(ctx context.Context, req *multipoolerpb.DescribeRequest) (*multipoolerpb.DescribeResponse, error) {
	callerid.AnnotateSpan(ctx, req.GetCallerId())
	desc, err := s.pooler.QueryService().Describe(ctx, req.Target, req.PreparedStatement, req.Portal, req.Options)
	if err != nil {
		// Convert errors to gRPC format, preserving PostgreSQL error details
		return nil, mterrors.ToGRPC(err)
	}

	// protobuf collapses an empty `repeated fields` to nil on the wire, losing
	// the RowDescription(0 fields) vs NoData distinction. Record it explicitly
	// so the gateway can restore it. nil Fields => NoData (no result set);
	// non-nil (incl. empty) => RowDescription.
	sqltypes.SetStatementDescriptionHasFields(desc)

	return &multipoolerpb.DescribeResponse{
		Description: desc,
	}, nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results.
// Used by multigateway for the Extended Query Protocol.
func (s *poolerService) PortalStreamExecute(req *multipoolerpb.PortalStreamExecuteRequest, stream multipoolerpb.MultipoolerService_PortalStreamExecuteServer) error {
	callerid.AnnotateSpan(stream.Context(), req.GetCallerId())
	// The pooler's query service admits the portal (as a reservation when it is
	// a suspendable cursor or carries reasons) and validates its reasons.
	return s.portalStreamExecuteTo(stream, s.pooler.QueryService(), req)
}

// portalStreamExecuteTo runs the portal on exec and writes the response
// sequence to stream. Split from PortalStreamExecute so the wire sequence can
// be tested without a live pooler. See streamExecuteTo for why the last data
// message is held back and the reservation state attached to it.
func (s *poolerService) portalStreamExecuteTo(
	stream multipoolerpb.MultipoolerService_PortalStreamExecuteServer,
	exec queryservice.QueryService,
	req *multipoolerpb.PortalStreamExecuteRequest,
) error {
	var pending *multipoolerpb.PortalStreamExecuteResponse
	flushPending := func() error {
		if pending == nil {
			return nil
		}
		resp := pending
		pending = nil
		return stream.Send(resp)
	}

	// Execute the portal and stream results
	reservedState, err := exec.PortalStreamExecute(
		stream.Context(),
		req.Target,
		req.PreparedStatement,
		req.Portal,
		req.Options,
		req.PortalOptions,
		req.GetReservationOptions(),
		func(ctx context.Context, result *sqltypes.Result) error {
			// Send notices first (if any) as separate diagnostic messages
			if len(result.Notices) > 0 {
				if err := flushPending(); err != nil {
					return err
				}
			}
			for _, notice := range result.Notices {
				noticePayload := &query.QueryResultPayload{
					Payload: &query.QueryResultPayload_Diagnostic{
						Diagnostic: mterrors.PgDiagnosticToProto(notice),
					},
				}
				noticeResponse := &multipoolerpb.PortalStreamExecuteResponse{
					Result: noticePayload,
				}
				if err := stream.Send(noticeResponse); err != nil {
					return err
				}
			}

			// Send row data (if any). Notices are streamed above as separate
			// diagnostics, so keep the result payload notice-free to avoid duplicate
			// NoticeResponse frames on the gateway.
			if len(result.Rows) > 0 || len(result.PassthroughBlock) > 0 || result.CommandTag != "" || len(result.ParameterStatus) > 0 {
				if err := flushPending(); err != nil {
					return err
				}
				protoResult := result.ToProto()
				protoResult.Notices = nil
				resp := &multipoolerpb.PortalStreamExecuteResponse{
					Result: &query.QueryResultPayload{
						Payload: &query.QueryResultPayload_Result{
							Result: protoResult,
						},
					},
				}
				if result.CommandTag == "" {
					// Intermediate chunk (or a suspended portal's last rows): nothing
					// will be attached to it, stream it now.
					return stream.Send(resp)
				}
				// Terminal chunk (CommandComplete): hold it so the reservation
				// state can ride on it.
				pending = resp
			}
			return nil
		},
	)

	// Attach the reserved state to the last data message when a reserved
	// connection is in play; if nothing is pending it goes out on its own as
	// before. This is also the authoritative state the gateway must see when the
	// portal failed: a PostgreSQL-level error can leave the reserved backend
	// alive (typically in an aborted transaction, awaiting ROLLBACK), and without
	// it the gateway could route follow-up cleanup to a different backend.
	if reservedState.GetReservedConnectionId() > 0 {
		if pending == nil {
			pending = &multipoolerpb.PortalStreamExecuteResponse{}
		}
		pending.ReservedState = reservedState
	}
	if err != nil {
		// Same shape as streamExecuteTo: the send error is intentionally
		// discarded. If the stream is already broken nothing returned here
		// reaches the gateway anyway, and it cleans up via
		// ReleaseReservedConnection on client disconnect; the execution error,
		// with its PostgreSQL diagnostics, is the one worth returning.
		_ = flushPending()
		return mterrors.ToGRPC(err)
	}
	return mterrors.ToGRPC(flushPending())
}

// CopyBidiExecute handles bidirectional streaming operations (e.g., COPY commands).
// The gateway sends: INITIATE → DATA (repeated) → DONE/FAIL  (for COPY FROM STDIN)
// The gateway sends: INITIATE                                (for COPY TO STDOUT)
// The pooler responds: READY → DATA (for COPY TO STDOUT) → RESULT/ERROR
func (s *poolerService) CopyBidiExecute(stream multipoolerpb.MultipoolerService_CopyBidiExecuteServer) error {
	ctx := stream.Context()

	// Receive INITIATE message first so we can check reserved connection ID
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to receive INITIATE: %v", err)
	}

	if req.Phase != multipoolerpb.CopyBidiExecuteRequest_INITIATE {
		return status.Errorf(codes.InvalidArgument, "expected INITIATE, got %v", req.Phase)
	}
	callerid.AnnotateSpan(ctx, req.GetCallerId())

	// COPY always pins a connection (the executor adds ReasonCopy internally, so
	// reservation reasons in the request can be 0 for an autocommit COPY that
	// still reserves). So a COPY without an existing reserved connection is always
	// a new reservation, never a single query.
	//
	// Admitted here, before CopyReady, rather than by the pooler's query
	// service: a CopyReady error is answered with an ERROR phase carrying the
	// reserved state, which an admission failure must not take.
	if err := s.pooler.StartRequest(req.Target, poolerserver.AdmissionKind(req.Options.GetReservedConnectionId(), true)); err != nil {
		return mterrors.ToGRPC(err)
	}

	// Get the executor from the pooler
	exec, err := s.pooler.Executor()
	if err != nil {
		return mterrors.ToGRPC(err)
	}

	// Dispatch on direction: COPY TO STDOUT has a server-push data phase
	// and does not consume client DATA messages, so it goes through a
	// dedicated handler. COPY FROM STDIN falls through to the existing flow.
	if req.Direction == multipoolerpb.CopyBidiExecuteRequest_TO_STDOUT {
		return s.copyBidiExecuteToStdout(ctx, stream, exec, req)
	}

	// Phase 1: INITIATE - Send COPY command and get reserved connection
	format, columnFormats, reservedState, err := exec.CopyReady(ctx, req.Target, req.Query, req.Options, req.ReservationOptions)
	if err != nil {
		// CopyReady returns a non-nil reservedState when the COPY query was
		// rejected by PostgreSQL (e.g., invalid column, conflicting options)
		// but the existing reserved connection is still alive and holding
		// other reasons. Forward that state to the gateway via an ERROR phase
		// response so the gateway keeps tracking the reserved conn; without
		// this, the gateway would see only the gRPC status error and the next
		// statement would fail with "reserved connection not found".
		//
		// Carry the structured PG diagnostic in error_diagnostic so the
		// gateway can re-emit a verbatim ErrorResponse to the client
		// (severity/SQLSTATE/DETAIL/HINT preserved). The text-only `error`
		// field is kept populated for legacy callers / log lines.
		errResp := &multipoolerpb.CopyBidiExecuteResponse{
			Phase:           multipoolerpb.CopyBidiExecuteResponse_ERROR,
			Error:           err.Error(),
			ErrorDiagnostic: mterrors.PgDiagnosticProtoFromError(err),
			ReservedState:   reservedState,
		}
		_ = stream.Send(errResp)
		return mterrors.ToGRPC(err)
	}

	// Convert columnFormats from []int16 to []int32 for protobuf
	columnFormats32 := make([]int32, len(columnFormats))
	for i, f := range columnFormats {
		columnFormats32[i] = int32(f)
	}

	// Send READY response with reserved connection info
	readyResp := &multipoolerpb.CopyBidiExecuteResponse{
		Phase:         multipoolerpb.CopyBidiExecuteResponse_READY,
		ReservedState: reservedState,
		Format:        int32(format),
		ColumnFormats: columnFormats32,
	}
	if err := stream.Send(readyResp); err != nil {
		// Clean up reserved connection on send failure
		copyOptions := &query.ExecuteOptions{
			User:                 req.Options.GetUser(),
			SessionSettings:      req.Options.GetSessionSettings(),
			ReservedConnectionId: reservedState.GetReservedConnectionId(),
		}
		_, _ = exec.CopyAbort(ctx, req.Target, "failed to send READY response", copyOptions)
		return status.Errorf(codes.Internal, "failed to send READY response: %v", err)
	}

	// Build options with reserved connection ID for subsequent calls
	copyOptions := &query.ExecuteOptions{
		User:                 req.Options.GetUser(),
		SessionSettings:      req.Options.GetSessionSettings(),
		ReservedConnectionId: reservedState.GetReservedConnectionId(),
	}
	// Capture target from INITIATE for use in error paths where req may be nil.
	initiateTarget := req.Target

	// Phase 2: Handle DATA/DONE/FAIL messages
	for {
		req, err := stream.Recv()
		if err != nil {
			// Stream closed or error — abort COPY and send best-effort ERROR response
			// so the gateway can update its shard state even if the stream is degraded.
			// Note: req may be nil when Recv fails, so we use initiateTarget.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				abortState, _ := exec.CopyAbort(ctx, initiateTarget, "context canceled", copyOptions)
				_ = stream.Send(&multipoolerpb.CopyBidiExecuteResponse{
					Phase:         multipoolerpb.CopyBidiExecuteResponse_ERROR,
					Error:         fmt.Sprintf("stream canceled: %v", err),
					ReservedState: abortState,
				})
				return status.Errorf(codes.Canceled, "stream canceled: %v", err)
			}
			abortState, _ := exec.CopyAbort(ctx, initiateTarget, "stream receive error", copyOptions)
			_ = stream.Send(&multipoolerpb.CopyBidiExecuteResponse{
				Phase:         multipoolerpb.CopyBidiExecuteResponse_ERROR,
				Error:         fmt.Sprintf("failed to receive message: %v", err),
				ReservedState: abortState,
			})
			return status.Errorf(codes.Internal, "failed to receive message: %v", err)
		}

		switch req.Phase {
		case multipoolerpb.CopyBidiExecuteRequest_DATA:
			// Phase 2a: DATA - Write data chunk to PostgreSQL
			if err := exec.CopySendData(ctx, req.Target, req.Data, copyOptions); err != nil {
				abortState, _ := exec.CopyAbort(ctx, req.Target, "failed to write data", copyOptions)
				// Send ERROR response with reserved state so gateway can update shard state.
				// Attach PgDiagnostic if the error carries one — keeps SQLSTATE/severity intact.
				errorResp := &multipoolerpb.CopyBidiExecuteResponse{
					Phase:           multipoolerpb.CopyBidiExecuteResponse_ERROR,
					Error:           err.Error(),
					ErrorDiagnostic: mterrors.PgDiagnosticProtoFromError(err),
					ReservedState:   abortState,
				}
				_ = stream.Send(errorResp)
				return mterrors.ToGRPC(err)
			}

		case multipoolerpb.CopyBidiExecuteRequest_DONE:
			// Phase 2b: DONE - Finalize COPY operation
			result, reservedState, err := exec.CopyFinalize(ctx, req.Target, req.Data, copyOptions)
			if err != nil {
				// CopyFinalize has already done its own cleanup:
				//   - PG ErrorResponse: ReadyForQuery was drained, the COPY
				//     reason was removed, and the conn was either released
				//     (no other reasons) or kept with the returned state.
				//   - Connection-level failure: conn was released, state is nil.
				// Either way, calling CopyAbort here would either be a no-op
				// (conn already released) or actively poison a clean conn by
				// writing CopyFail on a backend already back in RFQ. Forward
				// the state CopyFinalize returned. Carry the structured PG
				// diagnostic so the gateway re-emits a verbatim ErrorResponse,
				// plus any notices PG delivered before it.
				var errNotices []*query.PgDiagnostic
				if result != nil {
					errNotices = noticesToProto(result.Notices)
				}
				errorResp := &multipoolerpb.CopyBidiExecuteResponse{
					Phase:           multipoolerpb.CopyBidiExecuteResponse_ERROR,
					Error:           err.Error(),
					ErrorDiagnostic: mterrors.PgDiagnosticProtoFromError(err),
					ReservedState:   reservedState,
					Notices:         errNotices,
				}
				_ = stream.Send(errorResp)
				return mterrors.ToGRPC(err)
			}

			// Send RESULT response with final result, notices, and reserved state.
			// COPY bidi has a dedicated Notices field that the gateway folds into the
			// final Result in wire order, so keep the QueryResult notice-free to avoid
			// duplicate NoticeResponse frames after QueryResult grew unary notices.
			protoResult := result.ToProto()
			protoResult.Notices = nil
			resultResp := &multipoolerpb.CopyBidiExecuteResponse{
				Phase:         multipoolerpb.CopyBidiExecuteResponse_RESULT,
				Result:        protoResult,
				ReservedState: reservedState,
				Notices:       noticesToProto(result.Notices),
			}
			if err := stream.Send(resultResp); err != nil {
				return status.Errorf(codes.Internal, "failed to send RESULT: %v", err)
			}

			// Operation completed successfully
			return nil

		case multipoolerpb.CopyBidiExecuteRequest_FAIL:
			// Phase 2c: FAIL - Abort COPY operation
			errorMsg := req.ErrorMessage
			if errorMsg == "" {
				errorMsg = "operation aborted by client"
			}
			abortState, err := exec.CopyAbort(ctx, req.Target, errorMsg, copyOptions)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to abort COPY: %v", err)
			}

			// Send ERROR response with reserved state
			errorResp := &multipoolerpb.CopyBidiExecuteResponse{
				Phase:         multipoolerpb.CopyBidiExecuteResponse_ERROR,
				Error:         errorMsg,
				ReservedState: abortState,
			}
			_ = stream.Send(errorResp)
			return status.Errorf(codes.Aborted, "COPY aborted: %s", errorMsg)

		default:
			abortState, _ := exec.CopyAbort(ctx, req.Target, "unexpected phase", copyOptions)
			// Send ERROR response with reserved state so gateway can update shard state
			errorResp := &multipoolerpb.CopyBidiExecuteResponse{
				Phase:         multipoolerpb.CopyBidiExecuteResponse_ERROR,
				Error:         fmt.Sprintf("unexpected phase: %v", req.Phase),
				ReservedState: abortState,
			}
			_ = stream.Send(errorResp)
			return status.Errorf(codes.InvalidArgument, "unexpected phase: %v", req.Phase)
		}
	}
}

// noticesToProto converts a slice of mterrors notices to their proto form
// for forwarding through CopyBidiExecuteResponse.notices.
func noticesToProto(notices []*mterrors.PgDiagnostic) []*query.PgDiagnostic {
	if len(notices) == 0 {
		return nil
	}
	out := make([]*query.PgDiagnostic, 0, len(notices))
	for _, n := range notices {
		out = append(out, mterrors.PgDiagnosticToProto(n))
	}
	return out
}

// copyBidiExecuteToStdout drives the COPY ... TO STDOUT half of the bidi
// stream. Unlike COPY FROM STDIN, the client doesn't send DATA messages;
// PG pushes CopyData frames (interleaved with NoticeResponse) until
// CopyDone, then CommandComplete + ReadyForQuery. We translate that into
// the bidi protocol as READY → DATA* → RESULT.
//
// During this phase we do not call stream.Recv(); cleanup happens when
// stream.Send starts failing (client gone) or the RPC context is canceled
// by the caller returning up-stack.
func (s *poolerService) copyBidiExecuteToStdout(
	ctx context.Context,
	stream multipoolerpb.MultipoolerService_CopyBidiExecuteServer,
	exec queryservice.QueryService,
	req *multipoolerpb.CopyBidiExecuteRequest,
) error {
	format, columnFormats, initNotices, reservedState, err := exec.CopyOutReady(ctx, req.Target, req.Query, req.Options, req.ReservationOptions)
	if err != nil {
		errResp := &multipoolerpb.CopyBidiExecuteResponse{
			Phase:           multipoolerpb.CopyBidiExecuteResponse_ERROR,
			Error:           err.Error(),
			ErrorDiagnostic: mterrors.PgDiagnosticProtoFromError(err),
			ReservedState:   reservedState,
			Notices:         noticesToProto(initNotices),
		}
		_ = stream.Send(errResp)
		return mterrors.ToGRPC(err)
	}

	columnFormats32 := make([]int32, len(columnFormats))
	for i, f := range columnFormats {
		columnFormats32[i] = int32(f)
	}

	readyResp := &multipoolerpb.CopyBidiExecuteResponse{
		Phase:         multipoolerpb.CopyBidiExecuteResponse_READY,
		ReservedState: reservedState,
		Format:        int32(format),
		ColumnFormats: columnFormats32,
		Notices:       noticesToProto(initNotices),
	}
	if err := stream.Send(readyResp); err != nil {
		copyOptions := &query.ExecuteOptions{
			User:                 req.Options.GetUser(),
			SessionSettings:      req.Options.GetSessionSettings(),
			ReservedConnectionId: reservedState.GetReservedConnectionId(),
		}
		_, _ = exec.CopyAbort(ctx, req.Target, "failed to send READY response", copyOptions)
		return status.Errorf(codes.Internal, "failed to send READY response: %v", err)
	}

	copyOptions := &query.ExecuteOptions{
		User:                 req.Options.GetUser(),
		SessionSettings:      req.Options.GetSessionSettings(),
		ReservedConnectionId: reservedState.GetReservedConnectionId(),
	}

	// Stream CopyData / NoticeResponse to the gateway. The executor reads
	// from the backend and invokes the callback for each message until
	// CopyDone, at which point FinishCopyToStdout drains
	// CommandComplete + ReadyForQuery.
	result, finalState, err := exec.CopyOutStream(ctx, req.Target, copyOptions, func(msg client.CopyOutMessage) error {
		if msg.Notice != nil {
			noticeResp := &multipoolerpb.CopyBidiExecuteResponse{
				Phase:   multipoolerpb.CopyBidiExecuteResponse_DATA,
				Notices: []*query.PgDiagnostic{mterrors.PgDiagnosticToProto(msg.Notice)},
			}
			return stream.Send(noticeResp)
		}
		dataResp := &multipoolerpb.CopyBidiExecuteResponse{
			Phase: multipoolerpb.CopyBidiExecuteResponse_DATA,
			Data:  msg.Data,
		}
		return stream.Send(dataResp)
	})
	if err != nil {
		errResp := &multipoolerpb.CopyBidiExecuteResponse{
			Phase:           multipoolerpb.CopyBidiExecuteResponse_ERROR,
			Error:           err.Error(),
			ErrorDiagnostic: mterrors.PgDiagnosticProtoFromError(err),
			ReservedState:   finalState,
		}
		_ = stream.Send(errResp)
		return mterrors.ToGRPC(err)
	}

	// COPY bidi carries the trailing notices in the dedicated Notices field,
	// which the gateway folds into the final Result in wire order. Clear them
	// from the embedded QueryResult so they are not delivered twice (once from
	// result.ToProto(), once from the Notices field) — matching the CopyFinalize
	// path above.
	protoResult := result.ToProto()
	protoResult.Notices = nil
	resultResp := &multipoolerpb.CopyBidiExecuteResponse{
		Phase:         multipoolerpb.CopyBidiExecuteResponse_RESULT,
		Result:        protoResult,
		ReservedState: finalState,
		Notices:       noticesToProto(result.Notices),
	}
	if err := stream.Send(resultResp); err != nil {
		return status.Errorf(codes.Internal, "failed to send RESULT: %v", err)
	}
	return nil
}

// ConcludeTransaction concludes a transaction on a reserved connection.
// Executes COMMIT or ROLLBACK based on the conclusion. Returns remaining reasons if connection is still reserved.
func (s *poolerService) ConcludeTransaction(ctx context.Context, req *multipoolerpb.ConcludeTransactionRequest) (*multipoolerpb.ConcludeTransactionResponse, error) {
	callerid.AnnotateSpan(ctx, req.GetCallerId())

	// The pooler's query service admits it as an existing reservation.
	executor := s.pooler.QueryService()

	// Conclude the transaction. Forward the per-txn portal-release diff so the
	// executor can drop exactly the cursor pins PG closed for this ROLLBACK
	// (or fall back to ReleaseAllPortals when release_all_portals is true,
	// e.g. for older gateways that don't compute the diff).
	var rollbackSessionSettings map[string]string
	if snap := req.GetRollbackSessionSettings(); snap != nil {
		rollbackSessionSettings = snap.GetVars()
		if rollbackSessionSettings == nil {
			rollbackSessionSettings = map[string]string{}
		}
	}
	result, reservedState, err := executor.ConcludeTransaction(
		ctx, req.Target, req.Options, req.Conclusion,
		req.GetReleasePortalNames(), req.GetReleaseAllPortals(), req.GetChain(),
		rollbackSessionSettings,
	)
	if err != nil {
		return nil, withReservedStateDetail(mterrors.ToGRPC(err), reservedState)
	}

	return &multipoolerpb.ConcludeTransactionResponse{
		Result:        result.ToProto(),
		ReservedState: reservedState,
	}, nil
}

// withReservedStateDetail attaches state as an additional gRPC status detail
// alongside whatever mterrors.ToGRPC already attached (e.g. a PgDiagnostic),
// so a unary RPC that must return an error can still tell the caller the
// authoritative post-failure reservation state — e.g. a temp-table reason
// that survives a COMMIT which failed on a deferred constraint. A unary
// handler that returns an error never sends its response message at all, so
// this is the only way to carry state alongside grpcErr. Falls back to the
// plain error if state is nil or attaching the detail fails.
func withReservedStateDetail(grpcErr error, state *query.ReservedState) error {
	if grpcErr == nil || state == nil {
		return grpcErr
	}
	st, ok := status.FromError(grpcErr)
	if !ok {
		return grpcErr
	}
	stWithState, detailErr := st.WithDetails(state)
	if detailErr != nil {
		return grpcErr
	}
	return stWithState.Err()
}

// DiscardTempTables sends DISCARD TEMP on a reserved connection and removes the temp table reason.
// Returns remaining reasons if connection is still reserved.
func (s *poolerService) DiscardTempTables(ctx context.Context, req *multipoolerpb.DiscardTempTablesRequest) (*multipoolerpb.DiscardTempTablesResponse, error) {
	callerid.AnnotateSpan(ctx, req.GetCallerId())

	// The pooler's query service admits it as an existing reservation.
	executor := s.pooler.QueryService()

	result, reservedState, err := executor.DiscardTempTables(ctx, req.Target, req.Options)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolerpb.DiscardTempTablesResponse{
		Result:        result.ToProto(),
		ReservedState: reservedState,
	}, nil
}

// ReleaseReservedConnection forcefully releases a reserved connection regardless of reason.
func (s *poolerService) ReleaseReservedConnection(ctx context.Context, req *multipoolerpb.ReleaseReservedConnectionRequest) (*multipoolerpb.ReleaseReservedConnectionResponse, error) {
	callerid.AnnotateSpan(ctx, req.GetCallerId())

	// The pooler's query service admits it as an existing reservation.
	reservedState, err := s.pooler.QueryService().ReleaseReservedConnection(ctx, req.Target, req.Options, req.GetKeepStickyReservations())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolerpb.ReleaseReservedConnectionResponse{ReservedState: reservedState}, nil
}

// StreamPoolerHealth streams health updates to the client.
// Sends an initial health state immediately, then updates when state changes.
func (s *poolerService) StreamPoolerHealth(req *multipoolerpb.StreamPoolerHealthRequest, stream multipoolerpb.MultipoolerService_StreamPoolerHealthServer) error {
	ctx := stream.Context()

	// Check if pooler is initialized
	if s.pooler == nil {
		return status.Error(codes.Unavailable, "pooler not initialized")
	}

	// Subscribe to health updates
	initialState, healthChan, err := s.pooler.SubscribeHealth(ctx)
	if err != nil {
		return mterrors.ToGRPC(err)
	}

	// Send initial health state
	if initialState != nil {
		if err := stream.Send(initialState.ToProto()); err != nil {
			return err
		}
	}

	// Stream updates until client disconnects or context is cancelled
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case state, ok := <-healthChan:
			if !ok {
				// Channel closed, stream ended
				return nil
			}
			if err := stream.Send(state.ToProto()); err != nil {
				return err
			}
		}
	}
}

// NotificationStream keeps one ordered notification stream per gateway client
// session. Subscription updates and notification delivery share notifCh, so
// cross-channel notifications preserve PostgreSQL's delivery order.
//
// The listener is looked up per stream, not once at registration: the manager
// replaces it on every connection reopen, and a stale one stays stopped. A
// stream stays bound to the listener it started on; when that listener stops,
// it closes notifCh and the stream ends, and the gateway reopens a stream
// against the current leader. A stopped listener (this pooler is not the
// writable, serving leader) refuses new streams rather than acking subscribes
// it can never deliver on, so a gateway that still routes to a demoted leader
// keeps retrying until the load balancer observes the new one.
func (s *poolerService) NotificationStream(stream multipoolerpb.MultipoolerService_NotificationStreamServer) error {
	err := s.pooler.ServeNotifications(stream.Context(), stream.Recv, stream.Send)
	// The pooler's refusals carry a code for the gRPC status; stream and
	// context errors are returned as they are.
	var coded mterrors.ErrorWithCode
	if errors.As(err, &coded) {
		return mterrors.ToGRPC(err)
	}
	return err
}
