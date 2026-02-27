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
)

// PostgreSQL SQLSTATE codes used by Multigres when spoofing native PG errors.
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	PgSSProtocolViolation     = "08P01" // protocol_violation
	PgSSFeatureNotSupported   = "0A000" // feature_not_supported
	PgSSInvalidParameterValue = "22023" // invalid_parameter_value
	PgSSActiveTransaction     = "25001" // active_sql_transaction
	PgSSInFailedTransaction   = "25P02" // in_failed_sql_transaction
	PgSSAuthFailed            = "28P01" // invalid_authorization_specification
	PgSSInvalidCursorName     = "34000" // invalid_cursor_name
	PgSSSyntaxError           = "42601" // syntax_error
	PgSSUndefinedObject       = "42704" // undefined_object
	PgSSQueryCanceled         = "57014" // query_canceled
	PgSSInternalError         = "XX000" // internal_error
)

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

// NewUnrecognizedParameter creates a PgDiagnostic for an unrecognized configuration
// parameter (SQLSTATE 42704 undefined_object). This matches PostgreSQL's error for
// SHOW/SET/RESET of unknown GUC parameters.
func NewUnrecognizedParameter(name string) *PgDiagnostic {
	return NewPgError("ERROR", PgSSUndefinedObject,
		fmt.Sprintf("unrecognized configuration parameter %q", name), "")
}

// IsError checks whether err (or a wrapped cause) is an MT error matching code.
// For *PgDiagnostic errors it compares the SQLSTATE Code field directly;
// otherwise it falls back to substring matching on the error string.
func IsError(err error, code string) bool {
	if err == nil {
		return false
	}
	var diag *PgDiagnostic
	if errors.As(err, &diag) {
		return diag.Code == code
	}
	return false
}
