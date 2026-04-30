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
	PgSSProtocolViolation       = "08P01" // protocol_violation
	PgSSFeatureNotSupported     = "0A000" // feature_not_supported
	PgSSInvalidParameterValue   = "22023" // invalid_parameter_value
	PgSSActiveTransaction       = "25001" // active_sql_transaction
	PgSSInFailedTransaction     = "25P02" // in_failed_sql_transaction
	PgSSInvalidSQLStatementName = "26000" // invalid_sql_statement_name
	PgSSAuthFailed              = "28P01" // invalid_password
	PgSSInvalidAuthSpec         = "28000" // invalid_authorization_specification
	PgSSInvalidCursorName       = "34000" // invalid_cursor_name
	PgSSSyntaxError             = "42601" // syntax_error
	PgSSUndefinedObject         = "42704" // undefined_object
	PgSSQueryCanceled           = "57014" // query_canceled
	PgSSInternalError           = "XX000" // internal_error
	PgSSReadOnlyTransaction     = "25006" // read_only_sql_transaction
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
