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
	"strings"

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

// ParseBool parses PostgreSQL's text input format for boolean values.
// It mirrors boolin()/parse_bool_with_len: true/false, yes/no, on/off, 1/0,
// plus unique prefixes. The prefix "o" is invalid because it is ambiguous
// between "on" and "off".
func ParseBool(s string) (bool, bool) {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return false, false
	}
	switch {
	case v == "1", strings.HasPrefix("true", v), strings.HasPrefix("yes", v), v == "on":
		return true, true
	case v == "0", strings.HasPrefix("false", v), strings.HasPrefix("no", v), len(v) > 1 && strings.HasPrefix("off", v):
		return false, true
	default:
		return false, false
	}
}

// IsTrue interprets the value as PostgreSQL's text encoding of a boolean and
// reports whether it is true. NULL and any unrecognized encoding are false.
func (v Value) IsTrue() bool {
	if v.IsNull() {
		return false
	}
	b, ok := ParseBool(string(v))
	return ok && b
}

// SQLLiteral renders the value as a SQL literal: a single-quoted, escaped
// string literal for a non-NULL value (embedded single quotes doubled, the
// standard_conforming_strings form), or the keyword NULL. The value is treated
// as text — a numeric or boolean value is rendered as a quoted string, which
// PostgreSQL coerces at the call site.
func (v Value) SQLLiteral() string {
	if v.IsNull() {
		return "NULL"
	}
	return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
}

// Row represents a row with nullable column values.
type Row struct {
	// Values contains the column values. nil entry means NULL.
	Values []Value
}

// Notification represents a PostgreSQL asynchronous notification (NotificationResponse).
type Notification struct {
	// PID is the process ID of the notifying backend.
	PID int32
	// Channel is the notification channel name.
	Channel string
	// Payload is the notification payload string.
	Payload string
}

// Result represents a query result with nullable values.
type Result struct {
	// Fields describes the columns in the result set.
	Fields []*query.Field

	// RowsAffected is the number of rows affected (INSERT, UPDATE, DELETE, etc.)
	RowsAffected uint64

	// Rows contains the actual data rows.
	Rows []*Row

	// RawData, when non-nil, holds the opaque row-passthrough payload: the
	// concatenated raw PostgreSQL DataRow frames for this batch, exactly as read
	// from the backend. When set, Rows is empty and the rows are never parsed
	// into columns. The multigateway writes RawData straight to the client. See
	// QueryResult.raw_data_block.
	RawData []byte

	// RawRowCount is the number of DataRow frames packed into RawData. Preserves
	// the row count for metrics and row-limit accounting, since Rows is empty in
	// passthrough mode.
	RawRowCount int

	// CommandTag is the PostgreSQL command tag for this result set.
	// Examples: "SELECT 42", "INSERT 0 5", "UPDATE 10", "DELETE 3"
	CommandTag string

	// Notices contains any PostgreSQL diagnostic messages received during query execution.
	// These are typically non-fatal messages like warnings or informational notices.
	Notices []*mterrors.PgDiagnostic

	// Notifications contains asynchronous notifications received during query execution.
	// These are delivered via NotificationResponse ('A') messages from PostgreSQL.
	Notifications []*Notification
}

// RowCount returns the number of data rows in the result, accounting for opaque
// passthrough mode where the rows live in RawData rather than Rows.
func (r *Result) RowCount() int {
	if r == nil {
		return 0
	}
	if len(r.RawData) > 0 {
		return r.RawRowCount
	}
	return len(r.Rows)
}

// StructuredRows returns the result's parsed Rows and must be used by any
// consumer that reads column values itself (rather than streaming the result to
// a client). It panics if the result carries an opaque passthrough block
// (RawData set), because that means an opaque result reached a reader that
// expects structured columns — the Rows slice would be empty and the reader
// would silently see zero rows.
//
// Opaque passthrough is opt-in per query (ExecuteOptions.raw_rows), set only on
// the gateway's client-streaming path; internal query consumers never enable
// it, so this guard should never fire in practice. It converts a would-be
// silent "zero rows" bug into an immediate, loud failure if opaque is ever
// wired onto a structured-reading path by mistake. Callers that forward results
// to a client must not use it — they pass RawData through verbatim.
func (r *Result) StructuredRows() []*Row {
	if r == nil {
		return nil
	}
	if len(r.RawData) > 0 {
		panic("sqltypes: StructuredRows called on an opaque passthrough result (RawData set); this reader requires structured rows — run the query with raw_rows disabled")
	}
	return r.Rows
}

// ToProto converts Result to proto format for gRPC serialization.
func (r *Result) ToProto() *query.QueryResult {
	if r == nil {
		return nil
	}
	protoNotices := make([]*query.PgDiagnostic, len(r.Notices))
	for i, notice := range r.Notices {
		protoNotices[i] = mterrors.PgDiagnosticToProto(notice)
	}
	// Opaque row passthrough: carry the raw DataRow block instead of parsing
	// each row into a proto Row. Rows is empty in this mode.
	if len(r.RawData) > 0 {
		return &query.QueryResult{
			Fields:       r.Fields,
			HasFields:    r.Fields != nil,
			RowsAffected: r.RowsAffected,
			RawDataBlock: r.RawData,
			RawRowCount:  uint32(r.RawRowCount),
			CommandTag:   r.CommandTag,
			Notices:      protoNotices,
		}
	}
	protoRows := make([]*query.Row, len(r.Rows))
	for i, row := range r.Rows {
		protoRows[i] = row.ToProto()
	}
	return &query.QueryResult{
		Fields:       r.Fields,
		HasFields:    r.Fields != nil,
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
	// Restore nil vs empty Fields distinction lost in protobuf serialization.
	// Protobuf encodes both nil and empty repeated fields identically (as absent),
	// so we use HasFields to distinguish "no result set" from "zero-column result".
	fields := pr.Fields
	if pr.HasFields && fields == nil {
		fields = []*query.Field{}
	}
	notices := make([]*mterrors.PgDiagnostic, len(pr.Notices))
	for i, notice := range pr.Notices {
		notices[i] = mterrors.PgDiagnosticFromProto(notice)
	}
	// Opaque row passthrough: keep the raw DataRow block unparsed. Rows stays
	// nil; the multigateway writes RawData straight to the client.
	if pr.RawDataBlock != nil {
		return &Result{
			Fields:       fields,
			RowsAffected: pr.RowsAffected,
			RawData:      pr.RawDataBlock,
			RawRowCount:  int(pr.RawRowCount),
			CommandTag:   pr.CommandTag,
			Notices:      notices,
		}
	}
	rows := make([]*Row, len(pr.Rows))
	for i, row := range pr.Rows {
		rows[i] = RowFromProto(row)
	}
	return &Result{
		Fields:       fields,
		RowsAffected: pr.RowsAffected,
		Rows:         rows,
		CommandTag:   pr.CommandTag,
		Notices:      notices,
	}
}

// SetStatementDescriptionHasFields records, on desc.HasFields, whether desc.Fields
// was non-nil before crossing a gRPC boundary. Protobuf encodes both nil and empty
// repeated fields identically (as absent), so this flag preserves the
// RowDescription(0 fields) vs NoData distinction. Mirrors Result.ToProto.
func SetStatementDescriptionHasFields(desc *query.StatementDescription) {
	if desc != nil {
		desc.HasFields = desc.GetFields() != nil
	}
}

// RestoreStatementDescriptionFields restores the non-nil empty Fields slice that
// protobuf collapsed to nil on the wire, using HasFields to distinguish "no result
// set" from "zero-column result". Mirrors ResultFromProto.
func RestoreStatementDescriptionFields(desc *query.StatementDescription) {
	if desc != nil && desc.GetHasFields() && desc.GetFields() == nil {
		desc.Fields = []*query.Field{}
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
