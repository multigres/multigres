// Copyright 2022 The Vitess Authors.
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
	"slices"
)

// PostgreSQL SQLSTATE codes used by Multigres when spoofing native PG errors.
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	PgSSSuccessfulCompletion      = "00000" // successful_completion (used by NOTICE messages)
	PgSSProtocolViolation         = "08P01" // protocol_violation
	PgSSConnectionFailure         = "08006" // connection_failure
	PgSSFeatureNotSupported       = "0A000" // feature_not_supported
	PgSSInvalidTextRepresentation = "22P02" // invalid_text_representation
	PgSSInvalidParameterValue     = "22023" // invalid_parameter_value
	PgSSUndefinedFunction         = "42883" // undefined_function
	PgSSActiveTransaction         = "25001" // active_sql_transaction
	PgSSNoActiveTransaction       = "25P01" // no_active_sql_transaction
	PgSSInFailedTransaction       = "25P02" // in_failed_sql_transaction
	PgSSInvalidSQLStatementName   = "26000" // invalid_sql_statement_name
	PgSSAuthFailed                = "28P01" // invalid_password
	PgSSInvalidAuthSpec           = "28000" // invalid_authorization_specification
	PgSSInvalidCursorName         = "34000" // invalid_cursor_name
	PgSSDuplicatePreparedStmt     = "42P05" // duplicate_prepared_statement
	PgSSInsufficientPrivilege     = "42501" // insufficient_privilege
	PgSSSyntaxError               = "42601" // syntax_error
	PgSSUndefinedObject           = "42704" // undefined_object
	PgSSQueryCanceled             = "57014" // query_canceled
	PgSSAdminShutdown             = "57P01" // admin_shutdown
	PgSSCannotConnectNow          = "57P03" // cannot_connect_now
	PgSSIdleSessionTimeout        = "57P05" // idle_session_timeout
	PgSSInternalError             = "XX000" // internal_error
	PgSSReadOnlyTransaction       = "25006" // read_only_sql_transaction
	PgSSSerializationFailure      = "40001" // serialization_failure
)

// PostgreSQL data exceptions raised by scalar evaluation.
const (
	PgSSNumericValueOutOfRange          = "22003" // numeric_value_out_of_range
	PgSSDivisionByZero                  = "22012" // division_by_zero
	PgSSInvalidPrecedingOrFollowingSize = "22013" // invalid_preceding_or_following_size
)

// SQLSTATEs raised by expression analysis (postgres src/backend/parser).
const (
	PgSSIndeterminateDatatype = "42P18" // indeterminate_datatype
	PgSSUndefinedParameter    = "42P02" // undefined_parameter
	PgSSAmbiguousParameter    = "42P08" // ambiguous_parameter
	PgSSAmbiguousFunction     = "42725" // ambiguous_function
	PgSSDatatypeMismatch      = "42804" // datatype_mismatch
	PgSSWrongObjectType       = "42809" // wrong_object_type
	PgSSCannotCoerce          = "42846" // cannot_coerce
	PgSSTooManyArguments      = "54023" // too_many_arguments
)

// NewQueryCanceled creates a PgDiagnostic for an explicit cancel request
// (e.g. CancelRequest). SQLSTATE 57014 (query_canceled).
func NewQueryCanceled() *PgDiagnostic {
	return NewPgError("ERROR", PgSSQueryCanceled,
		"canceling statement due to user request", "")
}

// NewStatementTimeout creates a PgDiagnostic for a statement timeout expiry.
// SQLSTATE 57014 (query_canceled).
func NewStatementTimeout() *PgDiagnostic {
	return NewPgError("ERROR", PgSSQueryCanceled,
		"canceling statement due to statement timeout", "")
}

// NewIdleSessionTimeout creates a PgDiagnostic for an idle_session_timeout
// expiry. SQLSTATE 57P05, severity FATAL — matches PostgreSQL's behavior of
// terminating the client session after emitting the error.
func NewIdleSessionTimeout() *PgDiagnostic {
	return NewPgError("FATAL", PgSSIdleSessionTimeout,
		"terminating connection due to idle-session timeout", "")
}

// NewReservedConnectionTerminated creates a PgDiagnostic for a reserved
// connection that was terminated before its in-flight work could continue or
// be concluded. This fires for several distinct, unrelated reasons — most
// commonly the reserved pool's own idle-connection reaper (see
// reserved.Pool's InactivityTimeout), but also an explicit kill, an ordinary
// release racing a lookup, or a planned-failover drain exceeding its grace
// period. The message deliberately does not name a specific cause: it isn't
// determinable at this call site, and naming the wrong one (e.g. blaming
// "planned failover" for what's actually a routine idle timeout) misleads
// whoever reads it during an incident.
//
// The multipooler raises this for any lost reservation: once the connection is
// gone it no longer knows what the reservation held. The multigateway does, and
// passes this error to the client only when the reservation held nothing but a
// transaction. SQLSTATE 40001 (serialization_failure) so clients and ORMs retry
// the transaction, which was rolled back with the connection. When the
// reservation also held session state a retry cannot restore, the multigateway
// ends the session with NewSessionStateLost instead.
//
// This is deliberately NOT MTF01: the connection is gone, so there is nothing
// to buffer or auto-retry inside the cluster — only the client can replay it.
func NewReservedConnectionTerminated(reservedConnID uint64) *PgDiagnostic {
	return NewPgError("ERROR", PgSSSerializationFailure,
		reservedConnectionTerminatedMessage,
		fmt.Sprintf("reserved connection %d was terminated", reservedConnID))
}

// NewSessionStateLost creates the PgDiagnostic that ends a client session whose
// reserved connection was lost while it held session state the client relies on
// (temp tables, session advisory locks, a seeded PRNG, WITH HOLD cursors, ...).
// The next statement would otherwise run on a fresh backend that has none of it,
// silently: a lock another session can now take, a temp table that no longer
// shadows a permanent one. A retry cannot bring that state back, so the session
// ends and the client reconnects. lostState names what was lost.
//
// SQLSTATE 57P01 (admin_shutdown), severity FATAL: what PostgreSQL sends when it
// terminates a backend, which drivers and poolers treat as a lost connection.
func NewSessionStateLost(lostState string) *PgDiagnostic {
	return NewPgError("FATAL", PgSSAdminShutdown,
		"terminating connection because the backend holding its session state was lost",
		"Lost session state: "+lostState+".")
}

const reservedConnectionTerminatedMessage = "reserved connection terminated; please retry"

// IsReservedConnectionTerminated reports whether err is the diagnostic built by
// NewReservedConnectionTerminated, i.e. the multipooler no longer holds the
// reserved connection the request named. It matches the SQLSTATE together with
// the message (which Multigres generates, so it is not localized): a bare 40001
// is also what PostgreSQL itself raises for a real serialization failure, and
// that one leaves the reservation intact.
func IsReservedConnectionTerminated(err error) bool {
	var diag *PgDiagnostic
	return errors.As(err, &diag) &&
		diag.Code == PgSSSerializationFailure &&
		diag.Message == reservedConnectionTerminatedMessage
}

// NewAuthenticationTimeout creates a PgDiagnostic for an authentication_timeout
// expiry during the startup phase (SSLRequest, StartupMessage, or SCRAM
// exchange). SQLSTATE 08006 (connection_failure), severity FATAL — matches
// the way native PostgreSQL closes the connection when the timeout fires.
func NewAuthenticationTimeout() *PgDiagnostic {
	return NewPgError("FATAL", PgSSConnectionFailure,
		"canceling authentication due to timeout", "")
}

// MTError defines a Multigres-specific error code for conditions that have no
// PostgreSQL equivalent. Each instance is a template that produces a
// *PgDiagnostic via its New method. The ID is a 5-character code (e.g.
// "MTD01") placed in the SQLSTATE field, making MT errors directly
// identifiable by clients. For errors that have a real PostgreSQL SQLSTATE
// equivalent, use NewPgError instead.
type MTError struct {
	ID          string // e.g. "MTD01" — 5-char code used as the SQLSTATE
	Description string // long description, used as the Detail field
	Severity    string
	Format      string // fmt format string for message
}

// New builds a *PgDiagnostic from this error definition.
// The MT ID is placed in the SQLSTATE Code field and the Description
// is placed in the Detail field. If args are provided, the Format
// string is passed through fmt.Sprintf.
func (e *MTError) New(args ...any) *PgDiagnostic {
	msg := e.Format
	if len(args) != 0 {
		msg = fmt.Sprintf(e.Format, args...)
	}
	return &PgDiagnostic{
		MessageType: 'E',
		Severity:    e.Severity,
		Code:        e.ID,
		Message:     msg,
		Detail:      e.Description,
	}
}

// NewWithDetail builds a *PgDiagnostic from this error definition, using the
// provided detail string instead of the Description. This is useful for wrapper
// errors where the underlying error message should appear as the Detail field.
func (e *MTError) NewWithDetail(detail string, args ...any) *PgDiagnostic {
	d := e.New(args...)
	d.Detail = detail
	return d
}

var (
	MTD01 = &MTError{
		ID: "MTD01", Severity: "ERROR",
		Format:      "[BUG] %s",
		Description: "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/multigres/multigres/issues/new/choose.",
	}

	MTD02 = &MTError{
		ID: "MTD02", Severity: "ERROR",
		Format:      "pooler type mismatch: topology says %s but PostgreSQL is %s",
		Description: "The pooler type in the topology does not match the actual PostgreSQL role. This indicates the pooler is in an inconsistent state and requires intervention.",
	}

	MTD03 = &MTError{
		ID: "MTD03", Severity: "ERROR",
		Format:      "internal error",
		Description: "Internal proxy error.",
	}

	MTD04 = &MTError{
		ID: "MTD04", Severity: "ERROR",
		Format:      "parse failed",
		Description: "Extended query Parse could not be processed.",
	}

	MTD05 = &MTError{
		ID: "MTD05", Severity: "ERROR",
		Format:      "bind failed",
		Description: "Extended query Bind could not be processed.",
	}

	MTD06 = &MTError{
		ID: "MTD06", Severity: "ERROR",
		Format:      "describe failed",
		Description: "Extended query Describe could not be processed.",
	}

	MTD07 = &MTError{
		ID: "MTD07", Severity: "ERROR",
		Format:      "close failed",
		Description: "Extended query Close could not be processed.",
	}

	MTD08 = &MTError{
		ID: "MTD08", Severity: "ERROR",
		Format:      "sync failed",
		Description: "Extended query Sync could not be processed.",
	}

	MTE01 = &MTError{
		ID: "MTE01", Severity: "FATAL",
		Format:      "connection startup failed",
		Description: "Invalid startup message.",
	}

	MTB01 = &MTError{
		ID: "MTB01", Severity: "ERROR",
		Format:      "failover buffer full",
		Description: "The request was evicted because the failover buffer is at capacity. Retry the query.",
	}

	MTB02 = &MTError{
		ID: "MTB02", Severity: "ERROR",
		Format:      "failover buffer timeout",
		Description: "The request was evicted because the failover did not complete within the buffer window. Retry the query.",
	}

	MTB03 = &MTError{
		ID: "MTB03", Severity: "ERROR",
		Format:      "failover buffer shutting down",
		Description: "The request was evicted because the gateway is shutting down.",
	}

	// MTF01 is returned by multipooler when a planned failover is in progress
	// (servingStatus == SERVING_RDONLY). The gateway's classifyError uses this
	// code to trigger failover buffering for PRIMARY queries.
	MTF01 = &MTError{
		ID: "MTF01", Severity: "ERROR",
		Format:      "planned failover in progress",
		Description: "The pooler is transitioning during a planned failover. The query will be retried automatically.",
	}
)

// NewPgError creates a *PgDiagnostic with a real PostgreSQL SQLSTATE code.
// Use this for errors that should present as native PostgreSQL errors to clients
// (e.g., authentication failures, protocol violations, aborted transactions).
func NewPgError(severity, sqlState, message, detail string) *PgDiagnostic {
	return &PgDiagnostic{
		MessageType: 'E',
		Severity:    severity,
		Code:        sqlState,
		Message:     message,
		Detail:      detail,
	}
}

// NewParseError returns a *PgDiagnostic for a SQL parse/syntax failure, carrying
// SQLSTATE 42601 (syntax_error) — PostgreSQL's class for parse-stage errors. The
// parser produces these so a malformed statement surfaces to clients with the
// same SQLSTATE PostgreSQL itself would send, rather than an opaque internal
// error. This is the single source of truth for the parse-error SQLSTATE.
func NewParseError(message string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSSyntaxError, message, "")
}

// NewParseErrorAt is NewParseError with a cursor position and an optional
// SQLSTATE. position is the 1-based character offset PostgreSQL reports in the
// ErrorResponse "P" field so clients (psql, ORMs) can render the caret under the
// offending token. A position of 0 is omitted from the wire message, matching
// NewParseError.
//
// sqlState overrides NewParseError's 42601 for the parse-stage errors
// PostgreSQL raises with an errcode of their own rather than through yyerror —
// e.g. 22025 (invalid_escape_sequence) for a malformed E'...' Unicode escape.
// Empty means the statement failed to parse in the ordinary way and keeps 42601.
func NewParseErrorAt(message string, position int32, sqlState string) *PgDiagnostic {
	d := NewParseError(message)
	d.Position = position
	if sqlState != "" {
		d.Code = sqlState
	}
	return d
}

// NewPgNotice creates a *PgDiagnostic that will be sent as a NoticeResponse
// ('N') rather than an ErrorResponse ('E'). Use this for non-fatal diagnostics
// (WARNING, NOTICE, INFO, LOG, DEBUG) that PostgreSQL surfaces alongside a
// successful CommandComplete — e.g., the WARNING emitted for `SET LOCAL`
// outside a transaction block.
func NewPgNotice(severity, sqlState, message, detail string) *PgDiagnostic {
	return &PgDiagnostic{
		MessageType: 'N',
		Severity:    severity,
		Code:        sqlState,
		Message:     message,
		Detail:      detail,
	}
}

// NewUnrecognizedParameter creates a PgDiagnostic for an unrecognized configuration
// parameter (SQLSTATE 42704 undefined_object). This matches PostgreSQL's error for
// SHOW/SET/RESET of unknown GUC parameters.
func NewUnrecognizedParameter(name string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSUndefinedObject,
		fmt.Sprintf("unrecognized configuration parameter %q", name), "")
}

// NewInvalidPreparedStatementError creates a PgDiagnostic for a reference to
// a nonexistent prepared statement. SQLSTATE 26000 (invalid_sql_statement_name).
func NewInvalidPreparedStatementError(name string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSInvalidSQLStatementName,
		fmt.Sprintf("prepared statement \"%s\" does not exist", name), "")
}

// NewInvalidPortalError creates a PgDiagnostic for a reference to
// a nonexistent portal. SQLSTATE 34000 (invalid_cursor_name).
func NewInvalidPortalError(name string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSInvalidCursorName,
		fmt.Sprintf("portal \"%s\" does not exist", name), "")
}

// NewDuplicatePreparedStatementError creates a PgDiagnostic for a PREPARE
// that reuses an existing statement name. SQLSTATE 42P05.
func NewDuplicatePreparedStatementError(name string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSDuplicatePreparedStmt,
		fmt.Sprintf("prepared statement \"%s\" already exists", name), "")
}

// IsErrorCode checks whether err (or a wrapped cause) is a *PgDiagnostic
// whose SQLSTATE Code matches any of the provided codes.
func IsErrorCode(err error, codes ...string) bool {
	if err == nil {
		return false
	}
	var diag *PgDiagnostic
	if errors.As(err, &diag) {
		return slices.Contains(codes, diag.Code)
	}
	return false
}
