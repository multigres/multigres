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
	"errors"
	"fmt"
	"strings"

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

// PgDiagnostic represents a PostgreSQL diagnostic message (error or notice).
// PostgreSQL uses the same wire format for both ErrorResponse ('E') and NoticeResponse ('N'),
// differentiated by the MessageType field.
type PgDiagnostic struct {
	// MessageType is the PostgreSQL protocol message type byte.
	// 'E' (0x45 = 69) for ErrorResponse, 'N' (0x4E = 78) for NoticeResponse.
	MessageType      byte
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	Schema           string
	Table            string
	Column           string
	DataType         string
	Constraint       string
}

// IsError returns true if this diagnostic represents an error (MessageType == 'E').
func (d *PgDiagnostic) IsError() bool {
	return d.MessageType == 'E'
}

// IsNotice returns true if this diagnostic represents a notice (MessageType == 'N').
func (d *PgDiagnostic) IsNotice() bool {
	return d.MessageType == 'N'
}

// SQLSTATE returns the PostgreSQL SQLSTATE error code.
// This is an alias for the Code field, provided for clarity.
//
// SQLSTATE codes are 5-character strings where:
//   - First 2 characters = class (e.g., "42" = syntax/access error)
//   - Last 3 characters = specific condition
//
// Example: "42P01" means undefined table (class 42 = syntax error or access rule violation).
//
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
func (d *PgDiagnostic) SQLSTATE() string {
	return d.Code
}

// SQLSTATEClass returns the first 2 characters of the SQLSTATE code,
// which identifies the error class.
//
// Common classes:
//   - "00" = Successful completion
//   - "22" = Data exception
//   - "23" = Integrity constraint violation
//   - "42" = Syntax error or access rule violation
//   - "XX" = Internal error
//
// Returns empty string if Code is empty or less than 2 characters.
//
// Example:
//
//	diag := &PgDiagnostic{Code: "42P01"}
//	diag.SQLSTATEClass() // returns "42"
func (d *PgDiagnostic) SQLSTATEClass() string {
	if len(d.Code) < 2 {
		return ""
	}
	return d.Code[:2]
}

// IsClass returns true if the SQLSTATE code belongs to the specified class.
// The class is the first 2 characters of the SQLSTATE code.
//
// Example:
//
//	diag := &PgDiagnostic{Code: "42P01"} // undefined table
//	diag.IsClass("42") // returns true (syntax/access error class)
//	diag.IsClass("23") // returns false
func (d *PgDiagnostic) IsClass(class string) bool {
	return d.SQLSTATEClass() == class
}

// IsFatal returns true if the severity indicates a fatal condition.
// Fatal conditions include FATAL and PANIC severities.
//
// Per PostgreSQL protocol:
//   - FATAL: The session is terminated
//   - PANIC: All database sessions are terminated (server restart required)
//
// ERROR severity is not considered fatal - the session can continue.
func (d *PgDiagnostic) IsFatal() bool {
	return d.Severity == "FATAL" || d.Severity == "PANIC"
}

// Error implements the error interface.
// Returns PostgreSQL-native format: "SEVERITY: message".
// This matches the primary error line format that PostgreSQL displays.
// Use [PgDiagnostic.FullError] to include the SQLSTATE code for debugging.
func (d *PgDiagnostic) Error() string {
	if d == nil {
		return "ERROR: unknown error"
	}
	return d.Severity + ": " + d.Message
}

// FullError returns the error with SQLSTATE code for debugging purposes.
// Format: "SEVERITY: message (SQLSTATE code)"
func (d *PgDiagnostic) FullError() string {
	if d == nil {
		return "ERROR: unknown error (SQLSTATE 00000)"
	}
	return d.Severity + ": " + d.Message + " (SQLSTATE " + d.Code + ")"
}

// Validate checks that required PostgreSQL diagnostic fields are present.
// This is a lenient validation - it returns an error describing what's missing
// but callers should typically log a warning rather than fail.
//
// Required fields per PostgreSQL protocol:
//   - MessageType must be 'E' (ErrorResponse) or 'N' (NoticeResponse)
//   - Severity must not be empty
//   - Code (SQLSTATE) must not be empty
//   - Message must not be empty
func (d *PgDiagnostic) Validate() error {
	if d == nil {
		return errors.New("diagnostic is nil")
	}

	var issues []string

	if d.MessageType != 'E' && d.MessageType != 'N' {
		if d.MessageType == 0 {
			issues = append(issues, "MessageType is unset (0x00): must be 'E' or 'N'")
		} else {
			issues = append(issues, fmt.Sprintf("invalid MessageType '%c' (0x%02x): must be 'E' or 'N'", d.MessageType, d.MessageType))
		}
	}

	if d.Severity == "" {
		issues = append(issues, "Severity is empty")
	}

	if d.Code == "" {
		issues = append(issues, "Code (SQLSTATE) is empty")
	}

	if d.Message == "" {
		issues = append(issues, "Message is empty")
	}

	if len(issues) > 0 {
		return fmt.Errorf("invalid PgDiagnostic: %s", strings.Join(issues, "; "))
	}

	return nil
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
	Notices []*PgDiagnostic
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
	protoNotices := make([]*query.PgDiagnostic, len(r.Notices))
	for i, notice := range r.Notices {
		protoNotices[i] = PgDiagnosticToProto(notice)
	}
	return &query.QueryResult{
		Fields:       r.Fields,
		RowsAffected: r.RowsAffected,
		Rows:         protoRows,
		CommandTag:   r.CommandTag,
		Notices:      protoNotices,
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
	notices := make([]*PgDiagnostic, len(pr.Notices))
	for i, notice := range pr.Notices {
		notices[i] = PgDiagnosticFromProto(notice)
	}
	return &Result{
		Fields:       pr.Fields,
		RowsAffected: pr.RowsAffected,
		Rows:         rows,
		CommandTag:   pr.CommandTag,
		Notices:      notices,
	}
}

// PgDiagnosticToProto converts sqltypes PgDiagnostic to proto format for gRPC serialization.
func PgDiagnosticToProto(d *PgDiagnostic) *query.PgDiagnostic {
	if d == nil {
		return nil
	}
	return &query.PgDiagnostic{
		MessageType:      int32(d.MessageType),
		Severity:         d.Severity,
		Code:             d.Code,
		Message:          d.Message,
		Detail:           d.Detail,
		Hint:             d.Hint,
		Position:         d.Position,
		InternalPosition: d.InternalPosition,
		InternalQuery:    d.InternalQuery,
		Where:            d.Where,
		SchemaName:       d.Schema,
		TableName:        d.Table,
		ColumnName:       d.Column,
		DataTypeName:     d.DataType,
		ConstraintName:   d.Constraint,
	}
}

// PgDiagnosticFromProto converts proto PgDiagnostic to sqltypes PgDiagnostic.
func PgDiagnosticFromProto(pd *query.PgDiagnostic) *PgDiagnostic {
	if pd == nil {
		return nil
	}
	return &PgDiagnostic{
		MessageType:      byte(pd.MessageType),
		Severity:         pd.Severity,
		Code:             pd.Code,
		Message:          pd.Message,
		Detail:           pd.Detail,
		Hint:             pd.Hint,
		Position:         pd.Position,
		InternalPosition: pd.InternalPosition,
		InternalQuery:    pd.InternalQuery,
		Where:            pd.Where,
		Schema:           pd.SchemaName,
		Table:            pd.TableName,
		Column:           pd.ColumnName,
		DataType:         pd.DataTypeName,
		Constraint:       pd.ConstraintName,
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
