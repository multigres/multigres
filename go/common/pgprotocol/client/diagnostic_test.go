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

package client

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// TestParseDiagnosticFieldsMissingRequiredFields tests parsing diagnostics with missing required fields.
func TestParseDiagnosticFieldsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name             string
		fields           map[byte]string
		expectedSeverity string
		expectedCode     string
		expectedMessage  string
	}{
		{
			name:             "missing severity",
			fields:           map[byte]string{protocol.FieldCode: "42P01", protocol.FieldMessage: "relation does not exist"},
			expectedSeverity: "",
			expectedCode:     "42P01",
			expectedMessage:  "relation does not exist",
		},
		{
			name:             "missing code",
			fields:           map[byte]string{protocol.FieldSeverity: "ERROR", protocol.FieldMessage: "relation does not exist"},
			expectedSeverity: "ERROR",
			expectedCode:     "",
			expectedMessage:  "relation does not exist",
		},
		{
			name:             "missing message",
			fields:           map[byte]string{protocol.FieldSeverity: "ERROR", protocol.FieldCode: "42P01"},
			expectedSeverity: "ERROR",
			expectedCode:     "42P01",
			expectedMessage:  "",
		},
		{
			name:             "all required fields missing",
			fields:           map[byte]string{protocol.FieldDetail: "Some detail"},
			expectedSeverity: "",
			expectedCode:     "",
			expectedMessage:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build wire format message body
			w := NewMessageWriter()
			for fieldType, value := range tc.fields {
				w.WriteByte(fieldType)
				w.WriteString(value)
			}
			w.WriteByte(0) // Terminator

			// Parse as error (warning should be logged)
			diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
			require.NotNil(t, diag)

			// Verify fields are parsed (even if incomplete)
			assert.Equal(t, tc.expectedSeverity, diag.Severity)
			assert.Equal(t, tc.expectedCode, diag.Code)
			assert.Equal(t, tc.expectedMessage, diag.Message)

			// Validate should return error for incomplete diagnostics
			err := diag.Validate()
			assert.Error(t, err, "expected validation error for incomplete diagnostic")
		})
	}
}

// TestParseDiagnosticFieldsAllFieldsPopulated tests parsing diagnostics with all 14 fields.
func TestParseDiagnosticFieldsAllFieldsPopulated(t *testing.T) {
	// Build wire format message with all 14 fields
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("23505")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("duplicate key value violates unique constraint")
	w.WriteByte(protocol.FieldDetail)
	w.WriteString("Key (id)=(1) already exists.")
	w.WriteByte(protocol.FieldHint)
	w.WriteString("Use a different key value.")
	w.WriteByte(protocol.FieldPosition)
	w.WriteString("42")
	w.WriteByte(protocol.FieldInternalPosition)
	w.WriteString("15")
	w.WriteByte(protocol.FieldInternalQuery)
	w.WriteString("SELECT * FROM internal_table")
	w.WriteByte(protocol.FieldWhere)
	w.WriteString("PL/pgSQL function check_email() line 5")
	w.WriteByte(protocol.FieldSchema)
	w.WriteString("public")
	w.WriteByte(protocol.FieldTable)
	w.WriteString("users")
	w.WriteByte(protocol.FieldColumn)
	w.WriteString("email")
	w.WriteByte(protocol.FieldDataType)
	w.WriteString("text")
	w.WriteByte(protocol.FieldConstraint)
	w.WriteString("users_email_key")
	w.WriteByte(0) // Terminator

	diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
	require.NotNil(t, diag)

	// Verify all 14 fields
	assert.Equal(t, byte(protocol.MsgErrorResponse), diag.MessageType)
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "23505", diag.Code)
	assert.Equal(t, "duplicate key value violates unique constraint", diag.Message)
	assert.Equal(t, "Key (id)=(1) already exists.", diag.Detail)
	assert.Equal(t, "Use a different key value.", diag.Hint)
	assert.Equal(t, int32(42), diag.Position)
	assert.Equal(t, int32(15), diag.InternalPosition)
	assert.Equal(t, "SELECT * FROM internal_table", diag.InternalQuery)
	assert.Equal(t, "PL/pgSQL function check_email() line 5", diag.Where)
	assert.Equal(t, "public", diag.Schema)
	assert.Equal(t, "users", diag.Table)
	assert.Equal(t, "email", diag.Column)
	assert.Equal(t, "text", diag.DataType)
	assert.Equal(t, "users_email_key", diag.Constraint)

	// Validate should pass
	err := diag.Validate()
	assert.NoError(t, err)
}

// TestParseDiagnosticFieldsOnlyRequiredFields tests parsing diagnostics with only required fields.
func TestParseDiagnosticFieldsOnlyRequiredFields(t *testing.T) {
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("FATAL")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("28P01")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("password authentication failed")
	w.WriteByte(0) // Terminator

	diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
	require.NotNil(t, diag)

	// Verify required fields
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "28P01", diag.Code)
	assert.Equal(t, "password authentication failed", diag.Message)

	// Verify optional fields are empty/zero
	assert.Empty(t, diag.Detail)
	assert.Empty(t, diag.Hint)
	assert.Equal(t, int32(0), diag.Position)
	assert.Equal(t, int32(0), diag.InternalPosition)
	assert.Empty(t, diag.InternalQuery)
	assert.Empty(t, diag.Where)
	assert.Empty(t, diag.Schema)
	assert.Empty(t, diag.Table)
	assert.Empty(t, diag.Column)
	assert.Empty(t, diag.DataType)
	assert.Empty(t, diag.Constraint)

	// Validate should pass
	err := diag.Validate()
	assert.NoError(t, err)
}

// TestParseDiagnosticFieldsUnknownFieldTypes tests that unknown field types are ignored.
func TestParseDiagnosticFieldsUnknownFieldTypes(t *testing.T) {
	w := NewMessageWriter()
	// Known fields
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	// Unknown field type 'Z'
	w.WriteByte('Z')
	w.WriteString("unknown value 1")
	// Known field
	w.WriteByte(protocol.FieldCode)
	w.WriteString("42601")
	// Another unknown field type '!'
	w.WriteByte('!')
	w.WriteString("unknown value 2")
	// Known field
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("syntax error")
	// Unknown field type that looks like it could be a field
	w.WriteByte(0x7F)
	w.WriteString("high byte value")
	w.WriteByte(0) // Terminator

	diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
	require.NotNil(t, diag)

	// Verify known fields are parsed correctly
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "42601", diag.Code)
	assert.Equal(t, "syntax error", diag.Message)

	// Unknown fields should not affect parsing
	// Validate should pass since required fields are present
	err := diag.Validate()
	assert.NoError(t, err)
}

// TestParseDiagnosticFieldsNoticeMessageType tests parsing notices (not errors).
func TestParseDiagnosticFieldsNoticeMessageType(t *testing.T) {
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("WARNING")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("01000")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("this is a warning")
	w.WriteByte(0) // Terminator

	diag := parseDiagnosticFields(protocol.MsgNoticeResponse, w.Bytes())
	require.NotNil(t, diag)

	// Verify message type
	assert.Equal(t, byte(protocol.MsgNoticeResponse), diag.MessageType)
	assert.True(t, diag.IsNotice())
	assert.False(t, diag.IsError())

	// Verify fields
	assert.Equal(t, "WARNING", diag.Severity)
	assert.Equal(t, "01000", diag.Code)
	assert.Equal(t, "this is a warning", diag.Message)
}

// TestDiagnosticRoundTripParseProtoWriteParse tests the full round-trip:
// parse → PgDiagnostic → proto → PgDiagnostic → write → parse
func TestDiagnosticRoundTripParseProtoWriteParse(t *testing.T) {
	tests := []struct {
		name string
		diag *sqltypes.PgDiagnostic
	}{
		{
			name: "error with all fields",
			diag: &sqltypes.PgDiagnostic{
				MessageType:      protocol.MsgErrorResponse,
				Severity:         "ERROR",
				Code:             "23505",
				Message:          "duplicate key value violates unique constraint",
				Detail:           "Key (id)=(1) already exists.",
				Hint:             "Use a different key value.",
				Position:         42,
				InternalPosition: 15,
				InternalQuery:    "SELECT * FROM internal_table",
				Where:            "PL/pgSQL function check_email() line 5",
				Schema:           "public",
				Table:            "users",
				Column:           "email",
				DataType:         "text",
				Constraint:       "users_email_key",
			},
		},
		{
			name: "error with minimal fields",
			diag: &sqltypes.PgDiagnostic{
				MessageType: protocol.MsgErrorResponse,
				Severity:    "FATAL",
				Code:        "28P01",
				Message:     "password authentication failed",
			},
		},
		{
			name: "notice with warning",
			diag: &sqltypes.PgDiagnostic{
				MessageType: protocol.MsgNoticeResponse,
				Severity:    "WARNING",
				Code:        "01000",
				Message:     "implicit zero-padding warning",
				Detail:      "The value was truncated",
			},
		},
		{
			name: "error with position only",
			diag: &sqltypes.PgDiagnostic{
				MessageType: protocol.MsgErrorResponse,
				Severity:    "ERROR",
				Code:        "42601",
				Message:     "syntax error at or near \"FORM\"",
				Position:    15,
			},
		},
		{
			name: "error with schema context only",
			diag: &sqltypes.PgDiagnostic{
				MessageType: protocol.MsgErrorResponse,
				Severity:    "ERROR",
				Code:        "23503",
				Message:     "insert or update on table violates foreign key constraint",
				Schema:      "myschema",
				Table:       "orders",
				Constraint:  "orders_customer_id_fkey",
			},
		},
		{
			name: "unicode characters",
			diag: &sqltypes.PgDiagnostic{
				MessageType: protocol.MsgErrorResponse,
				Severity:    "ERROR",
				Code:        "22P02",
				Message:     "invalid input syntax: 日本語 émoji 🎉",
				Detail:      "详细信息 with unicode",
				Hint:        "Подсказка на русском",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: PgDiagnostic → proto
			protoDiag := sqltypes.PgDiagnosticToProto(tc.diag)
			require.NotNil(t, protoDiag)

			// Step 2: proto → PgDiagnostic
			recoveredDiag := sqltypes.PgDiagnosticFromProto(protoDiag)
			require.NotNil(t, recoveredDiag)

			// Verify proto round-trip preserves all fields
			assertDiagnosticsEqual(t, tc.diag, recoveredDiag)

			// Step 3: Build wire format message from recovered diagnostic
			wireBytes := buildDiagnosticWireFormat(recoveredDiag)

			// Step 4: Parse wire format back to PgDiagnostic
			finalDiag := parseDiagnosticFields(tc.diag.MessageType, wireBytes)
			require.NotNil(t, finalDiag)

			// Verify full round-trip preserves all fields
			assertDiagnosticsEqual(t, tc.diag, finalDiag)
		})
	}
}

// TestDiagnosticIsFatalAllSeverities tests IsFatal() with all known severity values.
func TestDiagnosticIsFatalAllSeverities(t *testing.T) {
	severities := []struct {
		severity string
		isFatal  bool
	}{
		// Fatal severities
		{"FATAL", true},
		{"PANIC", true},

		// Non-fatal error severities
		{"ERROR", false},

		// Notice severities (never fatal)
		{"WARNING", false},
		{"NOTICE", false},
		{"INFO", false},
		{"LOG", false},
		{"DEBUG", false},

		// Edge cases
		{"", false},
		{"fatal", false}, // Case sensitive
		{"Fatal", false},
		{"UNKNOWN", false},
	}

	for _, tc := range severities {
		t.Run("severity_"+tc.severity, func(t *testing.T) {
			diag := &sqltypes.PgDiagnostic{Severity: tc.severity}
			assert.Equal(t, tc.isFatal, diag.IsFatal(),
				"IsFatal() for severity %q should be %v", tc.severity, tc.isFatal)
		})
	}
}

// TestDiagnosticIsClassAllCategories tests IsClass() with various SQLSTATE codes.
func TestDiagnosticIsClassAllCategories(t *testing.T) {
	// Common SQLSTATE classes and example codes
	tests := []struct {
		code  string
		class string
		match bool
	}{
		// Class 00 - Successful completion
		{"00000", "00", true},
		{"00000", "42", false},

		// Class 01 - Warning
		{"01000", "01", true},
		{"01003", "01", true},
		{"01000", "00", false},

		// Class 02 - No data
		{"02000", "02", true},
		{"02001", "02", true},

		// Class 08 - Connection exception
		{"08000", "08", true},
		{"08003", "08", true},
		{"08006", "08", true},

		// Class 22 - Data exception
		{"22P02", "22", true}, // invalid_text_representation
		{"22000", "22", true},
		{"22001", "22", true}, // string_data_right_truncation
		{"22P02", "23", false},

		// Class 23 - Integrity constraint violation
		{"23000", "23", true},
		{"23505", "23", true}, // unique_violation
		{"23503", "23", true}, // foreign_key_violation
		{"23502", "23", true}, // not_null_violation
		{"23505", "22", false},

		// Class 25 - Invalid transaction state
		{"25000", "25", true},
		{"25001", "25", true},
		{"25P02", "25", true}, // in_failed_sql_transaction

		// Class 42 - Syntax error or access rule violation
		{"42601", "42", true}, // syntax_error
		{"42P01", "42", true}, // undefined_table
		{"42703", "42", true}, // undefined_column
		{"42P01", "23", false},

		// Class 53 - Insufficient resources
		{"53000", "53", true},
		{"53100", "53", true}, // disk_full
		{"53200", "53", true}, // out_of_memory

		// Class XX - Internal error
		{"XX000", "XX", true},
		{"XX001", "XX", true},
		{"XX000", "00", false},

		// Edge cases
		{"", "00", false},
		{"4", "42", false},   // Too short
		{"42", "42", true},   // Exact match
		{"X", "XX", false},   // Single char
		{"", "", true},       // Both empty (empty class matches empty code)
		{"42601", "", false}, // Empty class
	}

	for _, tc := range tests {
		t.Run(tc.code+"_in_"+tc.class, func(t *testing.T) {
			diag := &sqltypes.PgDiagnostic{Code: tc.code}
			assert.Equal(t, tc.match, diag.IsClass(tc.class),
				"IsClass(%q) for code %q should be %v", tc.class, tc.code, tc.match)
		})
	}
}

// TestDiagnosticValidationCatchesAllInvalidStates tests that validation catches all invalid states.
func TestDiagnosticValidationCatchesAllInvalidStates(t *testing.T) {
	tests := []struct {
		name          string
		diag          *sqltypes.PgDiagnostic
		expectError   bool
		errorContains []string
	}{
		{
			name:          "nil diagnostic",
			diag:          nil,
			expectError:   true,
			errorContains: []string{"diagnostic is nil"},
		},
		{
			name: "valid error",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "relation does not exist",
			},
			expectError: false,
		},
		{
			name: "valid notice",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'N',
				Severity:    "NOTICE",
				Code:        "00000",
				Message:     "this is a notice",
			},
			expectError: false,
		},
		{
			name: "invalid message type X",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'X',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "test",
			},
			expectError:   true,
			errorContains: []string{"invalid MessageType 'X' (0x58)"},
		},
		{
			name: "invalid message type zero",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 0,
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "test",
			},
			expectError:   true,
			errorContains: []string{"MessageType is unset (0x00)"},
		},
		{
			name: "empty severity",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'E',
				Severity:    "",
				Code:        "42P01",
				Message:     "test",
			},
			expectError:   true,
			errorContains: []string{"Severity is empty"},
		},
		{
			name: "empty code",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "",
				Message:     "test",
			},
			expectError:   true,
			errorContains: []string{"Code (SQLSTATE) is empty"},
		},
		{
			name: "empty message",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "",
			},
			expectError:   true,
			errorContains: []string{"Message is empty"},
		},
		{
			name: "all fields invalid",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'Z',
				Severity:    "",
				Code:        "",
				Message:     "",
			},
			expectError:   true,
			errorContains: []string{"invalid MessageType", "Severity is empty", "Code (SQLSTATE) is empty", "Message is empty"},
		},
		{
			name: "lowercase severity accepted (validation lenient)",
			diag: &sqltypes.PgDiagnostic{
				MessageType: 'E',
				Severity:    "error", // Lowercase is unusual but not invalid per validation
				Code:        "42P01",
				Message:     "test",
			},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.diag.Validate()
			if tc.expectError {
				require.Error(t, err)
				for _, contains := range tc.errorContains {
					assert.Contains(t, err.Error(), contains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestPgDiagnosticErrorWrapping tests that *sqltypes.PgDiagnostic works correctly with errors.As.
func TestPgDiagnosticErrorWrapping(t *testing.T) {
	// Create a PgDiagnostic error
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("42P01")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("relation does not exist")
	w.WriteByte(0)

	conn := &Conn{}
	err := conn.parseError(w.Bytes())
	require.Error(t, err)

	// Test errors.As - now directly to *sqltypes.PgDiagnostic
	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "42P01", diag.Code)

	// Test Error() format
	assert.Equal(t, "ERROR: relation does not exist", err.Error())

	// Test FullError()
	assert.Equal(t, "ERROR: relation does not exist (SQLSTATE 42P01)", diag.FullError())
}

// Helper function to assert two PgDiagnostics are equal
func assertDiagnosticsEqual(t *testing.T, expected, actual *sqltypes.PgDiagnostic) {
	t.Helper()
	assert.Equal(t, expected.MessageType, actual.MessageType, "MessageType mismatch")
	assert.Equal(t, expected.Severity, actual.Severity, "Severity mismatch")
	assert.Equal(t, expected.Code, actual.Code, "Code mismatch")
	assert.Equal(t, expected.Message, actual.Message, "Message mismatch")
	assert.Equal(t, expected.Detail, actual.Detail, "Detail mismatch")
	assert.Equal(t, expected.Hint, actual.Hint, "Hint mismatch")
	assert.Equal(t, expected.Position, actual.Position, "Position mismatch")
	assert.Equal(t, expected.InternalPosition, actual.InternalPosition, "InternalPosition mismatch")
	assert.Equal(t, expected.InternalQuery, actual.InternalQuery, "InternalQuery mismatch")
	assert.Equal(t, expected.Where, actual.Where, "Where mismatch")
	assert.Equal(t, expected.Schema, actual.Schema, "Schema mismatch")
	assert.Equal(t, expected.Table, actual.Table, "Table mismatch")
	assert.Equal(t, expected.Column, actual.Column, "Column mismatch")
	assert.Equal(t, expected.DataType, actual.DataType, "DataType mismatch")
	assert.Equal(t, expected.Constraint, actual.Constraint, "Constraint mismatch")
}

// Helper function to build wire format from PgDiagnostic
func buildDiagnosticWireFormat(diag *sqltypes.PgDiagnostic) []byte {
	w := NewMessageWriter()

	if diag.Severity != "" {
		w.WriteByte(protocol.FieldSeverity)
		w.WriteString(diag.Severity)
	}
	if diag.Code != "" {
		w.WriteByte(protocol.FieldCode)
		w.WriteString(diag.Code)
	}
	if diag.Message != "" {
		w.WriteByte(protocol.FieldMessage)
		w.WriteString(diag.Message)
	}
	if diag.Detail != "" {
		w.WriteByte(protocol.FieldDetail)
		w.WriteString(diag.Detail)
	}
	if diag.Hint != "" {
		w.WriteByte(protocol.FieldHint)
		w.WriteString(diag.Hint)
	}
	if diag.Position != 0 {
		w.WriteByte(protocol.FieldPosition)
		w.WriteString(positionToString(diag.Position))
	}
	if diag.InternalPosition != 0 {
		w.WriteByte(protocol.FieldInternalPosition)
		w.WriteString(positionToString(diag.InternalPosition))
	}
	if diag.InternalQuery != "" {
		w.WriteByte(protocol.FieldInternalQuery)
		w.WriteString(diag.InternalQuery)
	}
	if diag.Where != "" {
		w.WriteByte(protocol.FieldWhere)
		w.WriteString(diag.Where)
	}
	if diag.Schema != "" {
		w.WriteByte(protocol.FieldSchema)
		w.WriteString(diag.Schema)
	}
	if diag.Table != "" {
		w.WriteByte(protocol.FieldTable)
		w.WriteString(diag.Table)
	}
	if diag.Column != "" {
		w.WriteByte(protocol.FieldColumn)
		w.WriteString(diag.Column)
	}
	if diag.DataType != "" {
		w.WriteByte(protocol.FieldDataType)
		w.WriteString(diag.DataType)
	}
	if diag.Constraint != "" {
		w.WriteByte(protocol.FieldConstraint)
		w.WriteString(diag.Constraint)
	}

	w.WriteByte(0) // Terminator
	return w.Bytes()
}

func positionToString(pos int32) string {
	return strconv.FormatInt(int64(pos), 10)
}

// TestParseDiagnosticFieldsSeverityVOnly tests parsing when only FieldSeverityV ('V') is present.
func TestParseDiagnosticFieldsSeverityVOnly(t *testing.T) {
	// Build wire format message with only FieldSeverityV (non-localized severity)
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverityV)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("42P01")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("relation does not exist")
	w.WriteByte(0) // Terminator

	diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
	require.NotNil(t, diag)

	// Severity should be set from FieldSeverityV when FieldSeverity is absent
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "42P01", diag.Code)
	assert.Equal(t, "relation does not exist", diag.Message)

	// Validation should pass since all required fields are present
	err := diag.Validate()
	assert.NoError(t, err)
}

// TestParseDiagnosticFieldsBothSeverityFields tests parsing when both FieldSeverity and FieldSeverityV are present.
func TestParseDiagnosticFieldsBothSeverityFields(t *testing.T) {
	// Test case 1: FieldSeverity ('S') comes before FieldSeverityV ('V')
	// Expected: Use FieldSeverity value
	t.Run("S_before_V", func(t *testing.T) {
		w := NewMessageWriter()
		w.WriteByte(protocol.FieldSeverity)
		w.WriteString("ERREUR") // Localized (e.g., French)
		w.WriteByte(protocol.FieldSeverityV)
		w.WriteString("ERROR") // Non-localized
		w.WriteByte(protocol.FieldCode)
		w.WriteString("42P01")
		w.WriteByte(protocol.FieldMessage)
		w.WriteString("relation does not exist")
		w.WriteByte(0)

		diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
		require.NotNil(t, diag)

		// Should use FieldSeverity ('S') value since it was set first
		assert.Equal(t, "ERREUR", diag.Severity)
	})

	// Test case 2: FieldSeverityV ('V') comes before FieldSeverity ('S')
	// Expected: Use FieldSeverity value (overrides V)
	t.Run("V_before_S", func(t *testing.T) {
		w := NewMessageWriter()
		w.WriteByte(protocol.FieldSeverityV)
		w.WriteString("ERROR") // Non-localized (set first)
		w.WriteByte(protocol.FieldSeverity)
		w.WriteString("ERREUR") // Localized (overrides)
		w.WriteByte(protocol.FieldCode)
		w.WriteString("42P01")
		w.WriteByte(protocol.FieldMessage)
		w.WriteString("relation does not exist")
		w.WriteByte(0)

		diag := parseDiagnosticFields(protocol.MsgErrorResponse, w.Bytes())
		require.NotNil(t, diag)

		// Should use FieldSeverity ('S') value since it unconditionally overwrites
		assert.Equal(t, "ERREUR", diag.Severity)
	})
}
