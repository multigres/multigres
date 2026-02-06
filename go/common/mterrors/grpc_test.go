// Copyright 2026 Supabase, Inc.
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

package mterrors

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestToGRPCNil(t *testing.T) {
	got := ToGRPC(nil)
	assert.Nil(t, got)
}

func TestToGRPCRegularError(t *testing.T) {
	err := New(mtrpcpb.Code_INVALID_ARGUMENT, "invalid query")
	grpcErr := ToGRPC(err)
	require.NotNil(t, grpcErr)

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Equal(t, "invalid query", st.Message())
}

func TestToGRPCPgError(t *testing.T) {
	pgDiag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation does not exist",
		Position:    15,
	}

	grpcErr := ToGRPC(pgDiag)
	require.NotNil(t, grpcErr)

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	assert.Equal(t, codes.Unknown, st.Code()) // PgDiagnostic returns UNKNOWN code

	// Check that RPCError with PgDiagnostic is in details
	details := st.Details()
	require.Len(t, details, 1)

	rpcErr, ok := details[0].(*mtrpcpb.RPCError)
	require.True(t, ok)
	assert.Equal(t, "ERROR: relation does not exist", rpcErr.Message)
	assert.Equal(t, mtrpcpb.Code_UNKNOWN, rpcErr.Code)

	require.NotNil(t, rpcErr.PgDiagnostic)
	assert.Equal(t, int32(protocol.MsgErrorResponse), rpcErr.PgDiagnostic.MessageType)
	assert.Equal(t, "ERROR", rpcErr.PgDiagnostic.Severity)
	assert.Equal(t, "42P01", rpcErr.PgDiagnostic.Code)
	assert.Equal(t, "relation does not exist", rpcErr.PgDiagnostic.Message)
	assert.Equal(t, int32(15), rpcErr.PgDiagnostic.Position)
}

func TestFromGRPCNil(t *testing.T) {
	got := FromGRPC(nil)
	assert.Nil(t, got)
}

func TestFromGRPCEOF(t *testing.T) {
	// io.EOF should pass through unchanged
	got := FromGRPC(io.EOF)
	assert.Equal(t, io.EOF, got)
}

func TestFromGRPCRegularError(t *testing.T) {
	grpcErr := status.Error(codes.NotFound, "resource not found")
	got := FromGRPC(grpcErr)

	require.NotNil(t, got)
	assert.Equal(t, "resource not found", got.Error())
	assert.Equal(t, mtrpcpb.Code_NOT_FOUND, Code(got))

	// Should NOT be a PgDiagnostic
	var pgDiag *sqltypes.PgDiagnostic
	assert.False(t, errors.As(got, &pgDiag))
}

func TestFromGRPCWithPgDiagnostic(t *testing.T) {
	// Create a gRPC error with RPCError containing PgDiagnostic
	st := status.New(codes.Unknown, "ERROR: test error")
	rpcErr := &mtrpcpb.RPCError{
		Message: "ERROR: test error",
		Code:    mtrpcpb.Code_UNKNOWN,
		PgDiagnostic: sqltypes.PgDiagnosticToProto(&sqltypes.PgDiagnostic{
			MessageType: protocol.MsgErrorResponse,
			Severity:    "ERROR",
			Code:        "42000",
			Message:     "test error",
			Hint:        "check your query",
		}),
	}
	stWithDetails, err := st.WithDetails(rpcErr)
	require.NoError(t, err)

	got := FromGRPC(stWithDetails.Err())
	require.NotNil(t, got)

	// Should be a PgDiagnostic
	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(got, &diag))

	assert.Equal(t, byte(protocol.MsgErrorResponse), diag.MessageType)
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "42000", diag.Code)
	assert.Equal(t, "test error", diag.Message)
	assert.Equal(t, "check your query", diag.Hint)
}

func TestFromGRPCNonStatusError(t *testing.T) {
	// Non-gRPC errors should still be converted
	got := FromGRPC(errors.New("plain error"))
	require.NotNil(t, got)
	assert.Equal(t, "plain error", got.Error())
	assert.Equal(t, mtrpcpb.Code_UNKNOWN, Code(got))
}

func TestPgErrorGRPCRoundTrip(t *testing.T) {
	// Test that all 14 PostgreSQL fields are preserved through gRPC round-trip
	originalDiag := &sqltypes.PgDiagnostic{
		MessageType:      protocol.MsgErrorResponse,
		Severity:         "ERROR",
		Code:             "23505",
		Message:          "duplicate key value violates unique constraint",
		Detail:           "Key (id)=(1) already exists.",
		Hint:             "Use a different key value.",
		Position:         25,
		InternalPosition: 10,
		InternalQuery:    "SELECT internal_func()",
		Where:            "SQL function \"my_func\" statement 1",
		Schema:           "public",
		Table:            "users",
		Column:           "id",
		DataType:         "integer",
		Constraint:       "users_pkey",
	}

	// Convert to gRPC
	grpcErr := ToGRPC(originalDiag)

	// Convert back from gRPC
	recovered := FromGRPC(grpcErr)
	require.NotNil(t, recovered)

	// Should be a PgDiagnostic
	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(recovered, &diag))

	// Verify all 14 fields are preserved
	assert.Equal(t, byte(protocol.MsgErrorResponse), diag.MessageType)
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "23505", diag.Code)
	assert.Equal(t, "duplicate key value violates unique constraint", diag.Message)
	assert.Equal(t, "Key (id)=(1) already exists.", diag.Detail)
	assert.Equal(t, "Use a different key value.", diag.Hint)
	assert.Equal(t, int32(25), diag.Position)
	assert.Equal(t, int32(10), diag.InternalPosition)
	assert.Equal(t, "SELECT internal_func()", diag.InternalQuery)
	assert.Equal(t, "SQL function \"my_func\" statement 1", diag.Where)
	assert.Equal(t, "public", diag.Schema)
	assert.Equal(t, "users", diag.Table)
	assert.Equal(t, "id", diag.Column)
	assert.Equal(t, "integer", diag.DataType)
	assert.Equal(t, "users_pkey", diag.Constraint)

	// Verify Error() message is preserved
	assert.Equal(t, "ERROR: duplicate key value violates unique constraint", diag.Error())
}

func TestPgErrorGRPCRoundTripMinimalFields(t *testing.T) {
	// Test round-trip with only required fields (severity, code, message)
	originalDiag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "FATAL",
		Code:        "28P01",
		Message:     "password authentication failed",
	}

	grpcErr := ToGRPC(originalDiag)
	recovered := FromGRPC(grpcErr)

	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(recovered, &diag))
	assert.Equal(t, byte(protocol.MsgErrorResponse), diag.MessageType)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "28P01", diag.Code)
	assert.Equal(t, "password authentication failed", diag.Message)
	// Optional fields should be empty/zero
	assert.Empty(t, diag.Detail)
	assert.Empty(t, diag.Hint)
	assert.Equal(t, int32(0), diag.Position)
}

func TestNonPgErrorGRPCRoundTrip(t *testing.T) {
	// Non-PostgreSQL errors should continue working
	originalErr := New(mtrpcpb.Code_UNAVAILABLE, "service unavailable")
	grpcErr := ToGRPC(originalErr)
	recovered := FromGRPC(grpcErr)

	require.NotNil(t, recovered)
	assert.Equal(t, "service unavailable", recovered.Error())
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, Code(recovered))

	// Should NOT be a PgDiagnostic
	var pgDiag *sqltypes.PgDiagnostic
	assert.False(t, errors.As(recovered, &pgDiag))
}

func TestTruncateError(t *testing.T) {
	// Short error should not be truncated
	shortErr := errors.New("short error")
	assert.Equal(t, "short error", truncateError(shortErr))

	// Long error should be truncated
	longMsg := make([]byte, 10000)
	for i := range longMsg {
		longMsg[i] = 'a'
	}
	longErr := errors.New(string(longMsg))
	truncated := truncateError(longErr)
	assert.Less(t, len(truncated), len(longMsg))
	assert.Contains(t, truncated, "truncated")
}

func TestToGRPCPgErrorMessageTruncation(t *testing.T) {
	// Test that large messages are truncated in the warning log.
	// We can't easily test that slog.Warn is called (would require mocking),
	// but we can verify that ToGRPC still returns a valid error even with
	// a very large diagnostic message.
	largeMsg := make([]byte, 10000)
	for i := range largeMsg {
		largeMsg[i] = 'x'
	}

	pgDiag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     string(largeMsg),
		Where:       string(largeMsg), // Large Where field that may exceed gRPC limits
	}

	// ToGRPC should still work, potentially falling back to basic error
	grpcErr := ToGRPC(pgDiag)
	require.NotNil(t, grpcErr)

	// Should be a valid gRPC error
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	assert.Equal(t, codes.Unknown, st.Code())
}
