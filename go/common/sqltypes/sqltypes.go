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

// Package sqltypes provides internal types for query results and PostgreSQL diagnostics.
//
// # Query Result Types
//
// The package provides types that preserve NULL vs empty string distinction:
//   - [Value]: Represents a nullable column value (nil = NULL, []byte{} = empty string)
//   - [Row]: Contains a slice of Values representing a database row
//   - [Result]: Complete query result with fields, rows, command tag, and diagnostics
//
// These types are used throughout the codebase, while the proto types
// (in go/pb/query) are only used for gRPC serialization.
//
// # PostgreSQL Diagnostic System
//
// [PgDiagnostic] is the unified type for PostgreSQL error and notice messages.
// PostgreSQL uses identical wire format for ErrorResponse ('E') and NoticeResponse ('N'),
// so a single type handles both. PgDiagnostic implements the error interface directly,
// so it can be returned as an error without needing a wrapper type.
// The diagnostic flow through Multigres is:
//
//	PostgreSQL ErrorResponse/NoticeResponse
//	        ↓
//	*PgDiagnostic (parse all 14 fields, implements error)
//	        ↓
//	mterrors.PgError (wraps PgDiagnostic for system-level handling)
//	        ↓
//	gRPC: RPCError.pg_diagnostic (proto serialization)
//	        ↓
//	mterrors.PgError (reconstructed from gRPC)
//	        ↓
//	server.writePgDiagnosticResponse (write all 14 fields)
//	        ↓
//	Client sees native PostgreSQL error format
//
// # MessageType vs Severity
//
// MessageType and Severity serve different purposes:
//
//   - MessageType (byte): Protocol-level message type from PostgreSQL wire protocol.
//     'E' (0x45) = ErrorResponse, 'N' (0x4E) = NoticeResponse.
//     Use [PgDiagnostic.IsError] and [PgDiagnostic.IsNotice] to check.
//
//   - Severity (string): Application-level severity from the Severity field.
//     Errors: "ERROR", "FATAL", "PANIC"
//     Notices: "WARNING", "NOTICE", "DEBUG", "INFO", "LOG"
//     Use [PgDiagnostic.IsFatal] to check for fatal severities.
//
// The MessageType determines how to handle the message (error vs notice),
// while Severity provides additional context about the message's importance.
//
// # PostgreSQL Protocol Fields
//
// PgDiagnostic captures all 14 PostgreSQL diagnostic fields:
//
//	Field             Code  Description
//	─────────────────────────────────────────────────────────
//	Severity          S     ERROR, FATAL, PANIC, WARNING, etc.
//	Code              C     SQLSTATE error code (e.g., "42P01")
//	Message           M     Primary human-readable message
//	Detail            D     Optional detailed explanation
//	Hint              H     Optional suggestion for fixing
//	Position          P     Cursor position in original query
//	InternalPosition  p     Position in internal query
//	InternalQuery     q     Text of internal query
//	Where             W     Call stack for PL/pgSQL errors
//	Schema            s     Schema name (constraint errors)
//	Table             t     Table name (constraint errors)
//	Column            c     Column name (constraint errors)
//	DataType          d     Data type name
//	Constraint        n     Constraint name
//
// See: https://www.postgresql.org/docs/current/protocol-error-fields.html
package sqltypes

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/query"
)

// Value represents a nullable column value.
// nil means NULL, []byte{} means empty string.
type Value []byte

// IsNull returns true if the value is NULL.
func (v Value) IsNull() bool {
	return v == nil
}

// Row represents a row with nullable column values.
type Row struct {
	// Values contains the column values. nil entry means NULL.
	Values []Value
}

// Result represents a query result with nullable values.
type Result struct {
	// Fields describes the columns in the result set.
	Fields []*query.Field

	// RowsAffected is the number of rows affected (INSERT, UPDATE, DELETE, etc.)
	RowsAffected uint64

	// Rows contains the actual data rows.
	Rows []*Row

	// CommandTag is the PostgreSQL command tag for this result set.
	// Examples: "SELECT 42", "INSERT 0 5", "UPDATE 10", "DELETE 3"
	CommandTag string

	// Notices contains any PostgreSQL diagnostic messages received during query execution.
	// These are typically non-fatal messages like warnings or informational notices.
	Notices []*mterrors.PgDiagnostic
}

// ToProto converts Result to proto format for gRPC serialization.
func (r *Result) ToProto() *query.QueryResult {
	if r == nil {
		return nil
	}
	protoRows := make([]*query.Row, len(r.Rows))
	for i, row := range r.Rows {
		protoRows[i] = row.ToProto()
	}
	return &query.QueryResult{
		Fields:       r.Fields,
		RowsAffected: r.RowsAffected,
		Rows:         protoRows,
		CommandTag:   r.CommandTag,
	}
}

// ResultFromProto converts proto QueryResult to sqltypes Result.
func ResultFromProto(pr *query.QueryResult) *Result {
	if pr == nil {
		return nil
	}
	rows := make([]*Row, len(pr.Rows))
	for i, row := range pr.Rows {
		rows[i] = RowFromProto(row)
	}
	return &Result{
		Fields:       pr.Fields,
		RowsAffected: pr.RowsAffected,
		Rows:         rows,
		CommandTag:   pr.CommandTag,
	}
}

// ToProto converts Row to proto format (lengths+values) for gRPC serialization.
// Encoding: -1 = NULL, 0 = empty string, >0 = actual length.
func (r *Row) ToProto() *query.Row {
	if r == nil {
		return nil
	}

	lengths := make([]int64, len(r.Values))
	var totalLen int
	for i, v := range r.Values {
		if v == nil {
			lengths[i] = -1
		} else {
			lengths[i] = int64(len(v))
			totalLen += len(v)
		}
	}

	values := make([]byte, 0, totalLen)
	for _, v := range r.Values {
		if v != nil {
			values = append(values, v...)
		}
	}

	return &query.Row{
		Lengths: lengths,
		Values:  values,
	}
}

// RowFromProto converts proto Row (lengths+values) to sqltypes Row.
// Decoding: -1 = NULL, 0 = empty string, >0 = actual length.
func RowFromProto(pr *query.Row) *Row {
	if pr == nil {
		return nil
	}

	values := make([]Value, len(pr.Lengths))
	offset := 0
	for i, length := range pr.Lengths {
		switch length {
		case -1:
			values[i] = nil // NULL
		case 0:
			values[i] = []byte{} // empty string, not NULL
		default:
			values[i] = pr.Values[offset : offset+int(length)]
			offset += int(length)
		}
	}

	return &Row{Values: values}
}

// MakeRow creates a new Row from a slice of byte slices.
// nil entries represent NULL values.
func MakeRow(values [][]byte) *Row {
	row := &Row{
		Values: make([]Value, len(values)),
	}
	for i, v := range values {
		if v == nil {
			row.Values[i] = nil
		} else {
			row.Values[i] = Value(v)
		}
	}
	return row
}
