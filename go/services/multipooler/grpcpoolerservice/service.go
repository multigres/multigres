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
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
)

// poolerService is the gRPC wrapper for Multipooler
type poolerService struct {
	multipoolerpb.UnimplementedMultipoolerServiceServer
	pooler *poolerserver.QueryPoolerServer
	pubsub *pubsub.Listener
}

func RegisterPoolerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the pooler starts
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, func(p *poolerserver.QueryPoolerServer) {
		if grpc.CheckServiceMap("pooler", senv) {
			srv := &poolerService{
				pooler: p,
				pubsub: p.PubSubListener(),
			}
			multipoolerpb.RegisterMultipoolerServiceServer(grpc.Server, srv)
		}
	})
}

// admissionKind classifies a query-path request for StartRequest. A non-zero
// reservedConnID means it continues an EXISTING reserved connection; otherwise
// reserves reports whether the request will create a NEW reserved connection —
// if not, it is a single autocommit query. Each handler computes reserves from
// the signal it actually carries, mirroring the executor's own reservation
// decision (reservation reasons for StreamExecute, MaxRows or reasons for
// portals, always for COPY). The graceful drain serves single queries longer
// than new reservations, so the distinction matters during shutdown.
func admissionKind(reservedConnID uint64, reserves bool) poolerserver.RequestKind {
	switch {
	case reservedConnID > 0:
		return poolerserver.RequestExistingReserved
	case reserves:
		return poolerserver.RequestNewReservation
	default:
		return poolerserver.RequestSingleQuery
	}
}

// portalReserves reports whether a PortalStreamExecute will use or create a
// reserved connection, mirroring the executor's reserve decision exactly: a
// suspendable cursor (MaxRows > 0) OR a portal carrying reservation reasons.
// The reasons case is reachable on the extended-query path — a deferred BEGIN
// is folded into the first portal as ReasonTransaction while MaxRows == 0 — so
// checking MaxRows alone would mis-admit such a portal as a single query during
// a graceful drain, and the executor would then open a reserved connection.
func portalReserves(options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) bool {
	return options.GetMaxRows() > 0 || reservationOptions.GetReasons() != 0
}

// annotateCaller records the request's caller identity on the current span so
// pooler-side traces attribute a query to the app that issued it, not just the
// shared database user. No-op when the gateway sent no caller_id or there is no
// recording span.
func annotateCaller(ctx context.Context, caller *mtrpcpb.CallerID) {
	if caller == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attribute.String(callerid.KeyAuthenticatedUser, caller.GetPrincipal()),
		attribute.String(callerid.KeyApplicationName, caller.GetComponent()),
	)
}

// StreamExecute executes a SQL query and streams the results back to the client.
// This is the main execution method used by multigateway.
// When req.ReservationOptions has non-zero reasons, creates or extends a reserved connection.
func (s *poolerService) StreamExecute(req *multipoolerpb.StreamExecuteRequest, stream multipoolerpb.MultipoolerService_StreamExecuteServer) error {
	annotateCaller(stream.Context(), req.GetCallerId())
	// StreamExecute is the only query handler that can create a new reservation
	// (ReservationOptions reasons with no ReservedConnectionId). Classify it so a
	// graceful drain keeps serving single queries while rejecting new transactions.
	if err := s.pooler.StartRequest(req.Target, admissionKind(req.Options.GetReservedConnectionId(), req.GetReservationOptions().GetReasons() != 0)); err != nil {
		return mterrors.ToGRPC(err)
	}

	// Validate reservation reasons at the gRPC trust boundary.
	if reasons := req.GetReservationOptions().GetReasons(); reasons != 0 {
		if err := protoutil.ValidateReasons(reasons); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid reservation reasons: %v", err)
		}
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return mterrors.ToGRPC(err)
	}

	// Execute the query and stream results
	reservedState, err := executor.StreamExecute(stream.Context(), req.Target, req.Query, req.Options, req.GetReservationOptions(), func(ctx context.Context, result *sqltypes.Result) error {
		// Send notices first (if any) as separate diagnostic messages
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
		if len(result.Rows) > 0 || len(result.PassthroughBlock) > 0 || result.CommandTag != "" {
			protoResult := result.ToProto()
			protoResult.Notices = nil
			rowPayload := &query.QueryResultPayload{
				Payload: &query.QueryResultPayload_Result{
					Result: protoResult,
				},
			}
			resp := &multipoolerpb.StreamExecuteResponse{
				Result: rowPayload,
			}
			return stream.Send(resp)
		}
		return nil
	})

	// Send final message with reserved state if on a reserved connection.
	// The send error is intentionally discarded: if the stream is already broken
	// the gateway will clean up via ReleaseReservedConnection on client disconnect.
	if reservedState.GetReservedConnectionId() > 0 {
		_ = stream.Send(&multipoolerpb.StreamExecuteResponse{
			ReservedState: reservedState,
		})
	}

	// Convert errors to gRPC format, preserving PostgreSQL error details
	return mterrors.ToGRPC(err)
}

// ExecuteQuery executes a SQL query and returns the result
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (s *poolerService) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	annotateCaller(ctx, req.GetCallerId())
	// No ReservationOptions: an existing reserved connection, otherwise a single query.
	if err := s.pooler.StartRequest(req.Target, admissionKind(req.Options.GetReservedConnectionId(), false)); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Execute the query
	res, reservedState, err := executor.ExecuteQuery(ctx, req.Target, req.Query, req.Options)
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
	// Validate request.
	if req.Username == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}

	if s.pooler == nil {
		return nil, status.Error(codes.Unavailable, "pooler not initialized")
	}

	// Admin credential fetch (not a query). Treat like a new reservation so it is
	// buffered during any drain — the same behavior as before the two-stage drain.
	if err := s.pooler.StartRequest(nil, poolerserver.RequestNewReservation); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	poolManager := s.pooler.PoolManager()
	if poolManager == nil {
		return nil, status.Error(codes.Unavailable, "pool manager not initialized")
	}

	// Time the full credential-query path (admin acquire + pg_authid
	// lookup + decode) for mg.pooler.auth.credential_query.duration so
	// admin-pool contention shows up before it cascades into
	// gateway-visible auth latency. The duration is recorded on every
	// exit, error or not; the error counter only fires on failures so
	// the success rate is implicit.
	recorder := poolManager.CredentialQueryRecorder()
	start := time.Now()
	var errorType string
	defer func() {
		if recorder != nil {
			recorder.RecordCredentialQuery(ctx, time.Since(start), errorType)
		}
	}()

	// An admin connection:
	// - has permission to read password hashes
	// - also avoids a chicken-egg scenario of needing to create and use a role-specific connection
	//   to figure out if the caller should have access to that role-specific connection.
	conn, err := poolManager.GetAdminConn(ctx)
	if err != nil {
		errorType = connpoolmanager.CredentialQueryErrorPoolAcquireFailed
		return nil, status.Errorf(codes.Unavailable, "failed to get admin connection: %v", err)
	}
	defer conn.Recycle()

	// Get the role auth info (password hash + rolreplication) using the admin
	// connection. This queries pg_authid, which requires superuser access.
	authInfo, err := conn.Conn.GetRolAuthInfo(ctx, req.Username)
	if err != nil {
		switch {
		case errors.Is(err, admin.ErrUserNotFound):
			errorType = connpoolmanager.CredentialQueryErrorUserNotFound
			return nil, status.Errorf(codes.NotFound, "user %q not found", req.Username)
		case errors.Is(err, admin.ErrLoginDisabled):
			// Emit as a PgDiagnostic so the SQLSTATE (28000) is the
			// distinguishing signal at the gateway, not the gRPC code.
			// gRPC auth interceptors use codes.PermissionDenied /
			// codes.Unauthenticated for transport failures; keying on code
			// alone would misclassify an mTLS or authz error as an app-level
			// "role not permitted to log in" rejection to the end user.
			errorType = connpoolmanager.CredentialQueryErrorLoginDisabled
			return nil, mterrors.ToGRPC(mterrors.NewPgError(
				"FATAL", mterrors.PgSSInvalidAuthSpec,
				fmt.Sprintf("role %q is not permitted to log in", req.Username),
				"",
			))
		case errors.Is(err, admin.ErrPasswordExpired):
			// SQLSTATE 28P01 matches PG's opaque "password authentication
			// failed" error for expired passwords. PgDiagnostic detail
			// survives the gRPC round trip and the gateway matches on it.
			errorType = connpoolmanager.CredentialQueryErrorPasswordExpired
			return nil, mterrors.ToGRPC(mterrors.NewPgError(
				"FATAL", mterrors.PgSSAuthFailed,
				fmt.Sprintf("password authentication failed for user %q", req.Username),
				"",
			))
		default:
			// Genuine DB-level failure (SQL error, unexpected result shape,
			// connection drop mid-query). Tagged db_error so operators can
			// alert on it without false positives from the user_not_found
			// baseline.
			errorType = connpoolmanager.CredentialQueryErrorDB
			return nil, status.Errorf(codes.Internal, "failed to get role password: %v", err)
		}
	}

	return &multipoolerpb.GetAuthCredentialsResponse{
		ScramHash:         authInfo.ScramHash,
		IsReplicationRole: authInfo.IsReplicationRole,
	}, nil
}

// Describe returns metadata about a prepared statement or portal.
// Used by multigateway for the Extended Query Protocol.
func (s *poolerService) Describe(ctx context.Context, req *multipoolerpb.DescribeRequest) (*multipoolerpb.DescribeResponse, error) {
	annotateCaller(ctx, req.GetCallerId())
	// No ReservationOptions: an existing reserved connection, otherwise a single query.
	if err := s.pooler.StartRequest(req.Target, admissionKind(req.Options.GetReservedConnectionId(), false)); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Call the executor's Describe method
	desc, err := executor.Describe(ctx, req.Target, req.PreparedStatement, req.Portal, req.Options)
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
	annotateCaller(stream.Context(), req.GetCallerId())
	// A portal reserves when it is a suspendable cursor (MaxRows > 0) or carries
	// reservation reasons (e.g. a deferred BEGIN folded into the first portal),
	// or is already on a reserved connection — this mirrors the executor's own
	// reserve decision. Only a fetch-all portal with no reasons runs on a pooled
	// connection as a single query and may be served during stage 1.
	if err := s.pooler.StartRequest(req.Target, admissionKind(req.Options.GetReservedConnectionId(), portalReserves(req.Options, req.GetReservationOptions()))); err != nil {
		return mterrors.ToGRPC(err)
	}

	// Validate reservation reasons at the gRPC trust boundary (mirrors StreamExecute).
	if reasons := req.GetReservationOptions().GetReasons(); reasons != 0 {
		if err := protoutil.ValidateReasons(reasons); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid reservation reasons: %v", err)
		}
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return mterrors.ToGRPC(err)
	}

	// Execute the portal and stream results
	reservedState, err := executor.PortalStreamExecute(
		stream.Context(),
		req.Target,
		req.PreparedStatement,
		req.Portal,
		req.Options,
		req.PortalOptions,
		req.GetReservationOptions(),
		func(ctx context.Context, result *sqltypes.Result) error {
			// Send notices first (if any) as separate diagnostic messages
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
			if len(result.Rows) > 0 || len(result.PassthroughBlock) > 0 || result.CommandTag != "" {
				protoResult := result.ToProto()
				protoResult.Notices = nil
				rowPayload := &query.QueryResultPayload{
					Payload: &query.QueryResultPayload_Result{
						Result: protoResult,
					},
				}
				response := &multipoolerpb.PortalStreamExecuteResponse{
					Result: rowPayload,
				}
				return stream.Send(response)
			}
			return nil
		},
	)
	if err != nil {
		// A PostgreSQL-level portal error can leave the reserved backend alive
		// (typically in an aborted transaction, awaiting ROLLBACK). Send the
		// authoritative state before returning the gRPC error so the gateway doesn't
		// drift from the multipooler and accidentally route follow-up cleanup to a
		// different backend.
		if reservedState.GetReservedConnectionId() > 0 {
			if sendErr := stream.Send(&multipoolerpb.PortalStreamExecuteResponse{ReservedState: reservedState}); sendErr != nil {
				return mterrors.ToGRPC(sendErr)
			}
		}
		// Convert errors to gRPC format, preserving PostgreSQL error details.
		return mterrors.ToGRPC(err)
	}

	// Send final response with reserved connection ID if one was created
	if reservedState.GetReservedConnectionId() > 0 {
		return stream.Send(&multipoolerpb.PortalStreamExecuteResponse{
			ReservedState: reservedState,
		})
	}

	return nil
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
	annotateCaller(ctx, req.GetCallerId())

	// COPY always pins a connection (the executor adds ReasonCopy internally, so
	// reservation reasons in the request can be 0 for an autocommit COPY that
	// still reserves). So a COPY without an existing reserved connection is always
	// a new reservation, never a single query.
	if err := s.pooler.StartRequest(req.Target, admissionKind(req.Options.GetReservedConnectionId(), true)); err != nil {
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
			ErrorDiagnostic: pgDiagnosticFromError(err),
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
					ErrorDiagnostic: pgDiagnosticFromError(err),
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
					ErrorDiagnostic: pgDiagnosticFromError(err),
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

// pgDiagnosticFromError returns the proto representation of a PG diagnostic
// extracted from err, or nil when err is not a *mterrors.PgDiagnostic (e.g.
// an infrastructure error). Callers attach the result to
// CopyBidiExecuteResponse.error_diagnostic so the gateway can re-emit a
// verbatim ErrorResponse rather than wrapping the message text.
func pgDiagnosticFromError(err error) *query.PgDiagnostic {
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		return mterrors.PgDiagnosticToProto(diag)
	}
	return nil
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
			ErrorDiagnostic: pgDiagnosticFromError(err),
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
			ErrorDiagnostic: pgDiagnosticFromError(err),
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
	annotateCaller(ctx, req.GetCallerId())
	// Always on an existing reserved connection — admitted regardless of drain.
	if err := s.pooler.StartRequest(req.Target, poolerserver.RequestExistingReserved); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Conclude the transaction. Forward the per-txn portal-release diff so the
	// executor can drop exactly the cursor pins PG closed for this ROLLBACK
	// (or fall back to ReleaseAllPortals when release_all_portals is true,
	// e.g. for older gateways that don't compute the diff).
	result, reservedState, err := executor.ConcludeTransaction(
		ctx, req.Target, req.Options, req.Conclusion,
		req.GetReleasePortalNames(), req.GetReleaseAllPortals(), req.GetChain(),
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
	annotateCaller(ctx, req.GetCallerId())
	// Always on an existing reserved connection — admitted regardless of drain.
	if err := s.pooler.StartRequest(req.Target, poolerserver.RequestExistingReserved); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

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
	annotateCaller(ctx, req.GetCallerId())
	// Always on an existing reserved connection — admitted regardless of drain.
	if err := s.pooler.StartRequest(req.Target, poolerserver.RequestExistingReserved); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	if err := executor.ReleaseReservedConnection(ctx, req.Target, req.Options); err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolerpb.ReleaseReservedConnectionResponse{}, nil
}

// StreamPoolerHealth streams health updates to the client.
// Sends an initial health state immediately, then updates when state changes.
func (s *poolerService) StreamPoolerHealth(req *multipoolerpb.StreamPoolerHealthRequest, stream multipoolerpb.MultipoolerService_StreamPoolerHealthServer) error {
	ctx := stream.Context()

	// Check if pooler is initialized
	if s.pooler == nil {
		return status.Error(codes.Unavailable, "pooler not initialized")
	}

	// Get the health provider
	hp := s.pooler.HealthProvider()
	if hp == nil {
		return status.Error(codes.Unavailable, "health provider not initialized")
	}

	// Subscribe to health updates
	initialState, healthChan, err := hp.SubscribeHealth(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to health: %v", err)
	}

	// Send initial health state
	if initialState != nil {
		if err := stream.Send(healthStateToProto(initialState)); err != nil {
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
			if err := stream.Send(healthStateToProto(state)); err != nil {
				return err
			}
		}
	}
}

// healthStateToProto converts internal health state to proto response.
func healthStateToProto(state *poolerserver.HealthState) *multipoolerpb.StreamPoolerHealthResponse {
	resp := &multipoolerpb.StreamPoolerHealthResponse{
		PoolerId:      state.PoolerID,
		ServingStatus: state.ServingStatus,
		RoutingState:  state.RoutingState,
	}

	if state.RecommendedStalenessTimeout > 0 {
		resp.RecommendedStalenessTimeout = durationpb.New(state.RecommendedStalenessTimeout)
	}

	resp.ReplicationLagNs = state.ReplicationLagNs

	return resp
}

// NotificationStream keeps one ordered notification stream per gateway client
// session. Subscription updates and notification delivery share notifCh, so
// cross-channel notifications preserve PostgreSQL's delivery order.
func (s *poolerService) NotificationStream(stream multipoolerpb.MultipoolerService_NotificationStreamServer) error {
	if s.pubsub == nil {
		return errors.New("PubSubListener not initialized")
	}

	notifCh := make(chan *sqltypes.Notification, 256)
	subscribed := make(map[string]bool)
	defer func() {
		for ch := range subscribed {
			s.pubsub.Unsubscribe(ch, notifCh)
		}
	}()

	reqCh := make(chan *multipoolerpb.NotificationStreamRequest)
	errCh := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case reqCh <- req:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-errCh:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case req := <-reqCh:
			if req.GetUnsubscribeAll() {
				for ch := range subscribed {
					s.pubsub.Unsubscribe(ch, notifCh)
					delete(subscribed, ch)
				}
			}
			for _, ch := range req.GetUnsubscribeChannels() {
				if subscribed[ch] {
					s.pubsub.Unsubscribe(ch, notifCh)
					delete(subscribed, ch)
				}
			}
			for _, ch := range req.GetSubscribeChannels() {
				if !subscribed[ch] {
					s.pubsub.SubscribeCh(ch, notifCh)
					subscribed[ch] = true
				}
			}
			if err := stream.Send(&multipoolerpb.NotificationStreamResponse{Ready: true}); err != nil {
				return err
			}
		case notif := <-notifCh:
			if notif == nil {
				return nil
			}
			if err := stream.Send(&multipoolerpb.NotificationStreamResponse{
				Notification: &query.PgNotification{
					Pid:     notif.PID,
					Channel: notif.Channel,
					Payload: notif.Payload,
				},
			}); err != nil {
				return err
			}
		}
	}
}
