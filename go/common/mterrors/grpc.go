// Copyright 2019 The Vitess Authors.
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
// Modifications Copyright 2025 Supabase, Inc.

package mterrors

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// This file contains functions to convert errors to and from gRPC codes.
// Use these methods to return an error through gRPC and still
// retain its code.

// truncateError shortens errors because gRPC has a size restriction on them.
func truncateError(err error) string {
	// For more details see: https://github.com/grpc/grpc-go/issues/443
	// The gRPC spec says "Clients may limit the size of Response-Headers,
	// Trailers, and Trailers-Only, with a default of 8 KiB each suggested."
	// Therefore, we assume 8 KiB minus some headroom.
	GRPCErrorLimit := 8*1024 - 512
	if len(err.Error()) <= GRPCErrorLimit {
		return err.Error()
	}
	truncateInfo := "[...] [remainder of the error is truncated because gRPC has a size limit on errors.]"
	truncatedErr := err.Error()[:GRPCErrorLimit]
	return fmt.Sprintf("%v %v", truncatedErr, truncateInfo)
}

// ToGRPC returns an error as a gRPC error, with the appropriate error code.
// If the error is a *sqltypes.PgDiagnostic, it includes the PgDiagnostic in the gRPC status details
// so that all PostgreSQL error fields are preserved through the RPC.
func ToGRPC(err error) error {
	if err == nil {
		return nil
	}

	// Check if this is a PostgreSQL error
	var diag *sqltypes.PgDiagnostic
	if errors.As(err, &diag) {
		// Create gRPC status with RPCError containing the PgDiagnostic
		st := status.New(codes.Code(Code(err)), truncateError(err))
		rpcErr := &mtrpcpb.RPCError{
			Message:      err.Error(),
			Code:         mtrpcpb.Code_UNKNOWN,
			PgDiagnostic: sqltypes.PgDiagnosticToProto(diag),
		}
		// Attach the RPCError as a detail to the status
		stWithDetails, detailErr := st.WithDetails(rpcErr)
		if detailErr != nil {
			// Log a warning with context about the error being lost.
			// This can happen if the error details are too large for gRPC limits.
			truncatedMsg := diag.Message
			if len(truncatedMsg) > 100 {
				truncatedMsg = truncatedMsg[:100] + "..."
			}
			slog.Warn("failed to attach PgDiagnostic to gRPC status; PostgreSQL error details may be lost",
				slog.String("error", detailErr.Error()),
				slog.String("sqlstate", diag.Code),
				slog.String("severity", diag.Severity),
				slog.String("message", truncatedMsg),
			)
			// Fall back to basic error without PgDiagnostic details
			return st.Err()
		}
		return stWithDetails.Err()
	}

	return status.Errorf(codes.Code(Code(err)), "%v", truncateError(err))
}

// FromGRPC returns a gRPC error as a mterrors error, translating between error codes.
// If the gRPC error contains a PgDiagnostic in its details, it returns a *sqltypes.PgDiagnostic
// to preserve all PostgreSQL error fields.
// However, there are a few errors which are not translated and passed as they
// are. For example, io.EOF since our code base checks for this error to find
// out that a stream has finished.
func FromGRPC(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		// Do not wrap io.EOF because we compare against it for finished streams.
		return err
	}

	st, ok := status.FromError(err)
	if !ok {
		return New(mtrpcpb.Code_UNKNOWN, err.Error())
	}

	// Check for RPCError in status details
	for _, detail := range st.Details() {
		if rpcErr, ok := detail.(*mtrpcpb.RPCError); ok {
			// If PgDiagnostic is present, return it directly
			if rpcErr.GetPgDiagnostic() != nil {
				diag := sqltypes.PgDiagnosticFromProto(rpcErr.GetPgDiagnostic())
				return diag
			}
			// Otherwise use the RPCError message and code
			return New(rpcErr.Code, rpcErr.Message)
		}
	}

	// No RPCError details, fall back to basic conversion
	return New(mtrpcpb.Code(st.Code()), st.Message())
}
