// Copyright 2026 Supabase, Inc.
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

package mterrors

import (
	"errors"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/pb/query"
)

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

// PgDiagnosticToProto converts mterrors PgDiagnostic to proto format for gRPC serialization.
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

// PgDiagnosticFromProto converts proto PgDiagnostic to mterrors PgDiagnostic.
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
