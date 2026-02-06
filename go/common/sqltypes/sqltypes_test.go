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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/query"
)

func TestValueIsNull(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected bool
	}{
		{
			name:     "nil is null",
			value:    nil,
			expected: true,
		},
		{
			name:     "empty is not null",
			value:    Value{},
			expected: false,
		},
		{
			name:     "non-empty is not null",
			value:    Value("hello"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.value.IsNull())
		})
	}
}

func TestRowToProtoAndBack(t *testing.T) {
	tests := []struct {
		name   string
		values []Value
	}{
		{
			name:   "all nulls",
			values: []Value{nil, nil, nil},
		},
		{
			name:   "all empty strings",
			values: []Value{{}, {}, {}},
		},
		{
			name:   "all non-empty",
			values: []Value{Value("a"), Value("bc"), Value("def")},
		},
		{
			name:   "mixed null and empty and values",
			values: []Value{nil, {}, Value("hello"), nil, Value("world"), {}},
		},
		{
			name:   "empty row",
			values: []Value{},
		},
		{
			name:   "single null",
			values: []Value{nil},
		},
		{
			name:   "single empty string",
			values: []Value{{}},
		},
		{
			name:   "single value",
			values: []Value{Value("test")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := &Row{Values: tc.values}

			// Convert to proto
			protoRow := original.ToProto()

			// Verify proto encoding
			require.Len(t, protoRow.Lengths, len(tc.values))
			for i, v := range tc.values {
				if v == nil {
					assert.Equal(t, int64(-1), protoRow.Lengths[i], "null should encode as -1")
				} else {
					assert.Equal(t, int64(len(v)), protoRow.Lengths[i], "length should match")
				}
			}

			// Convert back
			recovered := RowFromProto(protoRow)

			// Verify roundtrip
			require.Len(t, recovered.Values, len(tc.values))
			for i, v := range tc.values {
				if v == nil {
					assert.Nil(t, recovered.Values[i], "null should remain null at index %d", i)
				} else {
					assert.NotNil(t, recovered.Values[i], "non-null should remain non-null at index %d", i)
					assert.Equal(t, []byte(v), []byte(recovered.Values[i]), "value should match at index %d", i)
				}
			}
		})
	}
}

func TestRowFromProtoNil(t *testing.T) {
	assert.Nil(t, RowFromProto(nil))
}

func TestRowToProtoNil(t *testing.T) {
	var r *Row
	assert.Nil(t, r.ToProto())
}

func TestResultToProtoAndBack(t *testing.T) {
	original := &Result{
		Fields: []*query.Field{
			{Name: "col1", DataTypeOid: 23},
			{Name: "col2", DataTypeOid: 25},
		},
		RowsAffected: 42,
		Rows: []*Row{
			{Values: []Value{nil, Value("hello")}},
			{Values: []Value{{}, Value("world")}},
			{Values: []Value{Value("test"), nil}},
		},
		CommandTag: "SELECT 3",
	}

	// Convert to proto
	protoResult := original.ToProto()
	require.NotNil(t, protoResult)
	assert.Equal(t, original.RowsAffected, protoResult.RowsAffected)
	assert.Equal(t, original.CommandTag, protoResult.CommandTag)
	assert.Len(t, protoResult.Fields, 2)
	assert.Len(t, protoResult.Rows, 3)

	// Convert back
	recovered := ResultFromProto(protoResult)
	require.NotNil(t, recovered)
	assert.Equal(t, original.RowsAffected, recovered.RowsAffected)
	assert.Equal(t, original.CommandTag, recovered.CommandTag)
	assert.Len(t, recovered.Fields, 2)
	assert.Len(t, recovered.Rows, 3)

	// Verify rows preserved NULL vs empty string
	// Row 0: [NULL, "hello"]
	assert.Nil(t, recovered.Rows[0].Values[0], "row 0 col 0 should be NULL")
	assert.Equal(t, "hello", string(recovered.Rows[0].Values[1]))

	// Row 1: ["", "world"]
	assert.NotNil(t, recovered.Rows[1].Values[0], "row 1 col 0 should be empty string not NULL")
	assert.Equal(t, "", string(recovered.Rows[1].Values[0]))
	assert.Equal(t, "world", string(recovered.Rows[1].Values[1]))

	// Row 2: ["test", NULL]
	assert.Equal(t, "test", string(recovered.Rows[2].Values[0]))
	assert.Nil(t, recovered.Rows[2].Values[1], "row 2 col 1 should be NULL")
}

func TestResultFromProtoNil(t *testing.T) {
	assert.Nil(t, ResultFromProto(nil))
}

func TestResultToProtoNil(t *testing.T) {
	var r *Result
	assert.Nil(t, r.ToProto())
}

func TestParamsToProtoAndBack(t *testing.T) {
	tests := []struct {
		name   string
		params [][]byte
	}{
		{
			name:   "all nulls",
			params: [][]byte{nil, nil, nil},
		},
		{
			name:   "all empty",
			params: [][]byte{{}, {}, {}},
		},
		{
			name:   "all values",
			params: [][]byte{[]byte("a"), []byte("bc"), []byte("def")},
		},
		{
			name:   "mixed",
			params: [][]byte{nil, {}, []byte("hello"), nil, []byte("world"), {}},
		},
		{
			name:   "empty params",
			params: [][]byte{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Convert to proto format
			lengths, values := ParamsToProto(tc.params)

			// Verify encoding
			require.Len(t, lengths, len(tc.params))
			for i, p := range tc.params {
				if p == nil {
					assert.Equal(t, int64(-1), lengths[i])
				} else {
					assert.Equal(t, int64(len(p)), lengths[i])
				}
			}

			// Convert back
			recovered := ParamsFromProto(lengths, values)

			// Verify roundtrip
			require.Len(t, recovered, len(tc.params))
			for i, p := range tc.params {
				if p == nil {
					assert.Nil(t, recovered[i], "null param should remain null at index %d", i)
				} else {
					assert.NotNil(t, recovered[i], "non-null param should remain non-null at index %d", i)
					assert.Equal(t, p, recovered[i], "param should match at index %d", i)
				}
			}
		})
	}
}

func TestMakeRow(t *testing.T) {
	input := [][]byte{nil, {}, []byte("hello")}
	row := MakeRow(input)

	require.Len(t, row.Values, 3)
	assert.Nil(t, row.Values[0])
	assert.NotNil(t, row.Values[1])
	assert.Equal(t, "", string(row.Values[1]))
	assert.Equal(t, "hello", string(row.Values[2]))
}

func TestPgDiagnosticToProtoAndBack(t *testing.T) {
	tests := []struct {
		name       string
		diagnostic *PgDiagnostic
	}{
		{
			name: "all fields populated",
			diagnostic: &PgDiagnostic{
				MessageType:      'N',
				Severity:         "WARNING",
				Code:             "01000",
				Message:          "This is a test notice",
				Detail:           "Additional detail about the notice",
				Hint:             "Consider doing something",
				Position:         42,
				InternalPosition: 10,
				InternalQuery:    "SELECT internal_func()",
				Where:            "PL/pgSQL function test_func() line 5",
				Schema:           "public",
				Table:            "users",
				Column:           "email",
				DataType:         "character varying",
				Constraint:       "users_email_key",
			},
		},
		{
			name: "only required fields",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "NOTICE",
				Code:        "00000",
				Message:     "Simple notice",
				Detail:      "",
				Hint:        "",
			},
		},
		{
			name: "empty strings for optional fields",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "INFO",
				Code:        "00000",
				Message:     "Notice with empty optional fields",
				Detail:      "",
				Hint:        "",
			},
		},
		{
			name: "unicode characters in message",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "WARNING",
				Code:        "01000",
				Message:     "Unicode test: 日本語 🎉 émoji café",
				Detail:      "详细信息 with unicode",
				Hint:        "Подсказка на русском",
			},
		},
		{
			name: "error with special characters",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "relation \"users\" does not exist",
				Detail:      "Line 1: SELECT * FROM \"users\"\n        ^",
				Hint:        "Check if table name is quoted correctly",
			},
		},
		{
			name: "empty message",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "NOTICE",
				Code:        "00000",
				Message:     "",
				Detail:      "",
				Hint:        "",
			},
		},
		{
			name: "context fields only",
			diagnostic: &PgDiagnostic{
				MessageType:      'N',
				Severity:         "NOTICE",
				Code:             "00000",
				Message:          "Notice with context",
				Position:         100,
				InternalPosition: 50,
				InternalQuery:    "SELECT * FROM internal_table",
				Where:            "SQL function \"my_func\"",
			},
		},
		{
			name: "position fields only",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "WARNING",
				Code:        "01000",
				Message:     "Position test",
				Position:    1,
			},
		},
		{
			name: "where field with newlines",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "NOTICE",
				Code:        "00000",
				Message:     "Call stack test",
				Where:       "PL/pgSQL function outer_func() line 3\nPL/pgSQL function inner_func() line 1",
			},
		},
		{
			name: "error with schema object fields",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "23505",
				Message:     "duplicate key value violates unique constraint",
				Schema:      "myschema",
				Table:       "mytable",
				Column:      "mycolumn",
				DataType:    "integer",
				Constraint:  "mytable_pkey",
			},
		},
		{
			name: "partial schema object fields",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "WARNING",
				Code:        "01000",
				Message:     "Partial schema info",
				Schema:      "public",
				Table:       "orders",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Convert to proto
			protoDiag := PgDiagnosticToProto(tc.diagnostic)
			require.NotNil(t, protoDiag)

			// Verify proto has correct values
			assert.Equal(t, int32(tc.diagnostic.MessageType), protoDiag.MessageType)
			assert.Equal(t, tc.diagnostic.Severity, protoDiag.Severity)
			assert.Equal(t, tc.diagnostic.Code, protoDiag.Code)
			assert.Equal(t, tc.diagnostic.Message, protoDiag.Message)
			assert.Equal(t, tc.diagnostic.Detail, protoDiag.Detail)
			assert.Equal(t, tc.diagnostic.Hint, protoDiag.Hint)
			assert.Equal(t, tc.diagnostic.Position, protoDiag.Position)
			assert.Equal(t, tc.diagnostic.InternalPosition, protoDiag.InternalPosition)
			assert.Equal(t, tc.diagnostic.InternalQuery, protoDiag.InternalQuery)
			assert.Equal(t, tc.diagnostic.Where, protoDiag.Where)
			assert.Equal(t, tc.diagnostic.Schema, protoDiag.SchemaName)
			assert.Equal(t, tc.diagnostic.Table, protoDiag.TableName)
			assert.Equal(t, tc.diagnostic.Column, protoDiag.ColumnName)
			assert.Equal(t, tc.diagnostic.DataType, protoDiag.DataTypeName)
			assert.Equal(t, tc.diagnostic.Constraint, protoDiag.ConstraintName)

			// Convert back
			recovered := PgDiagnosticFromProto(protoDiag)
			require.NotNil(t, recovered)

			// Verify roundtrip preserves all fields
			assert.Equal(t, tc.diagnostic.MessageType, recovered.MessageType)
			assert.Equal(t, tc.diagnostic.Severity, recovered.Severity)
			assert.Equal(t, tc.diagnostic.Code, recovered.Code)
			assert.Equal(t, tc.diagnostic.Message, recovered.Message)
			assert.Equal(t, tc.diagnostic.Detail, recovered.Detail)
			assert.Equal(t, tc.diagnostic.Hint, recovered.Hint)
			assert.Equal(t, tc.diagnostic.Position, recovered.Position)
			assert.Equal(t, tc.diagnostic.InternalPosition, recovered.InternalPosition)
			assert.Equal(t, tc.diagnostic.InternalQuery, recovered.InternalQuery)
			assert.Equal(t, tc.diagnostic.Where, recovered.Where)
			assert.Equal(t, tc.diagnostic.Schema, recovered.Schema)
			assert.Equal(t, tc.diagnostic.Table, recovered.Table)
			assert.Equal(t, tc.diagnostic.Column, recovered.Column)
			assert.Equal(t, tc.diagnostic.DataType, recovered.DataType)
			assert.Equal(t, tc.diagnostic.Constraint, recovered.Constraint)
		})
	}
}

func TestPgDiagnosticFromProtoNil(t *testing.T) {
	assert.Nil(t, PgDiagnosticFromProto(nil))
}

func TestPgDiagnosticToProtoNil(t *testing.T) {
	assert.Nil(t, PgDiagnosticToProto(nil))
}

func TestPgDiagnosticIsError(t *testing.T) {
	errDiag := &PgDiagnostic{MessageType: 'E', Severity: "ERROR"}
	noticeDiag := &PgDiagnostic{MessageType: 'N', Severity: "NOTICE"}

	assert.True(t, errDiag.IsError())
	assert.False(t, errDiag.IsNotice())
	assert.False(t, noticeDiag.IsError())
	assert.True(t, noticeDiag.IsNotice())
}

func TestPgDiagnosticValidate(t *testing.T) {
	tests := []struct {
		name        string
		diagnostic  *PgDiagnostic
		expectError bool
		errorSubstr string
	}{
		{
			name: "valid error diagnostic",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "relation does not exist",
			},
			expectError: false,
		},
		{
			name: "valid notice diagnostic",
			diagnostic: &PgDiagnostic{
				MessageType: 'N',
				Severity:    "NOTICE",
				Code:        "00000",
				Message:     "this is a notice",
			},
			expectError: false,
		},
		{
			name: "valid error with all fields",
			diagnostic: &PgDiagnostic{
				MessageType:      'E',
				Severity:         "ERROR",
				Code:             "23505",
				Message:          "duplicate key value",
				Detail:           "Key already exists",
				Hint:             "Use ON CONFLICT",
				Position:         10,
				InternalPosition: 5,
				InternalQuery:    "SELECT ...",
				Where:            "trigger",
				Schema:           "public",
				Table:            "users",
				Column:           "id",
				DataType:         "integer",
				Constraint:       "users_pkey",
			},
			expectError: false,
		},
		{
			name:        "nil diagnostic",
			diagnostic:  nil,
			expectError: true,
			errorSubstr: "diagnostic is nil",
		},
		{
			name: "invalid MessageType",
			diagnostic: &PgDiagnostic{
				MessageType: 'X',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "test error",
			},
			expectError: true,
			errorSubstr: "invalid MessageType 'X' (0x58)",
		},
		{
			name: "zero MessageType",
			diagnostic: &PgDiagnostic{
				MessageType: 0,
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "test error",
			},
			expectError: true,
			errorSubstr: "MessageType is unset (0x00)",
		},
		{
			name: "empty Severity",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "",
				Code:        "42P01",
				Message:     "test error",
			},
			expectError: true,
			errorSubstr: "Severity is empty",
		},
		{
			name: "empty Code",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "",
				Message:     "test error",
			},
			expectError: true,
			errorSubstr: "Code (SQLSTATE) is empty",
		},
		{
			name: "empty Message",
			diagnostic: &PgDiagnostic{
				MessageType: 'E',
				Severity:    "ERROR",
				Code:        "42P01",
				Message:     "",
			},
			expectError: true,
			errorSubstr: "Message is empty",
		},
		{
			name: "multiple validation failures",
			diagnostic: &PgDiagnostic{
				MessageType: 'X',
				Severity:    "",
				Code:        "",
				Message:     "",
			},
			expectError: true,
			errorSubstr: "invalid MessageType",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.diagnostic.Validate()
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorSubstr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPgDiagnosticValidateMultipleFailures(t *testing.T) {
	// Test that multiple failures are all reported
	diag := &PgDiagnostic{
		MessageType: 'X',
		Severity:    "",
		Code:        "",
		Message:     "",
	}

	err := diag.Validate()
	require.Error(t, err)

	errStr := err.Error()
	assert.Contains(t, errStr, "invalid MessageType")
	assert.Contains(t, errStr, "Severity is empty")
	assert.Contains(t, errStr, "Code (SQLSTATE) is empty")
	assert.Contains(t, errStr, "Message is empty")
}

func TestPgDiagnosticSQLSTATE(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		expected string
	}{
		{
			name:     "syntax error",
			code:     "42601",
			expected: "42601",
		},
		{
			name:     "undefined table",
			code:     "42P01",
			expected: "42P01",
		},
		{
			name:     "successful completion",
			code:     "00000",
			expected: "00000",
		},
		{
			name:     "data exception",
			code:     "22P02",
			expected: "22P02",
		},
		{
			name:     "internal error",
			code:     "XX000",
			expected: "XX000",
		},
		{
			name:     "empty code",
			code:     "",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			diag := &PgDiagnostic{Code: tc.code}
			assert.Equal(t, tc.expected, diag.SQLSTATE())
		})
	}
}

func TestPgDiagnosticSQLSTATEClass(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		expected string
	}{
		{
			name:     "syntax error class",
			code:     "42601",
			expected: "42",
		},
		{
			name:     "undefined table",
			code:     "42P01",
			expected: "42",
		},
		{
			name:     "successful completion",
			code:     "00000",
			expected: "00",
		},
		{
			name:     "data exception",
			code:     "22P02",
			expected: "22",
		},
		{
			name:     "integrity constraint violation",
			code:     "23505",
			expected: "23",
		},
		{
			name:     "internal error",
			code:     "XX000",
			expected: "XX",
		},
		{
			name:     "empty code",
			code:     "",
			expected: "",
		},
		{
			name:     "single character code",
			code:     "X",
			expected: "",
		},
		{
			name:     "two character code",
			code:     "42",
			expected: "42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			diag := &PgDiagnostic{Code: tc.code}
			assert.Equal(t, tc.expected, diag.SQLSTATEClass())
		})
	}
}

func TestPgDiagnosticIsClass(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		class    string
		expected bool
	}{
		{
			name:     "syntax error in class 42",
			code:     "42601",
			class:    "42",
			expected: true,
		},
		{
			name:     "syntax error not in class 22",
			code:     "42601",
			class:    "22",
			expected: false,
		},
		{
			name:     "undefined table in class 42",
			code:     "42P01",
			class:    "42",
			expected: true,
		},
		{
			name:     "data exception in class 22",
			code:     "22P02",
			class:    "22",
			expected: true,
		},
		{
			name:     "integrity constraint in class 23",
			code:     "23505",
			class:    "23",
			expected: true,
		},
		{
			name:     "internal error in class XX",
			code:     "XX000",
			class:    "XX",
			expected: true,
		},
		{
			name:     "empty code is not in any class",
			code:     "",
			class:    "42",
			expected: false,
		},
		{
			name:     "short code is not in any class",
			code:     "X",
			class:    "XX",
			expected: false,
		},
		{
			name:     "empty class comparison",
			code:     "42601",
			class:    "",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			diag := &PgDiagnostic{Code: tc.code}
			assert.Equal(t, tc.expected, diag.IsClass(tc.class))
		})
	}
}

func TestPgDiagnosticIsFatal(t *testing.T) {
	tests := []struct {
		name     string
		severity string
		expected bool
	}{
		{
			name:     "ERROR is not fatal",
			severity: "ERROR",
			expected: false,
		},
		{
			name:     "FATAL is fatal",
			severity: "FATAL",
			expected: true,
		},
		{
			name:     "PANIC is fatal",
			severity: "PANIC",
			expected: true,
		},
		{
			name:     "WARNING is not fatal",
			severity: "WARNING",
			expected: false,
		},
		{
			name:     "NOTICE is not fatal",
			severity: "NOTICE",
			expected: false,
		},
		{
			name:     "INFO is not fatal",
			severity: "INFO",
			expected: false,
		},
		{
			name:     "LOG is not fatal",
			severity: "LOG",
			expected: false,
		},
		{
			name:     "DEBUG is not fatal",
			severity: "DEBUG",
			expected: false,
		},
		{
			name:     "empty severity is not fatal",
			severity: "",
			expected: false,
		},
		{
			name:     "lowercase fatal is not fatal (strict matching)",
			severity: "fatal",
			expected: false,
		},
		{
			name:     "lowercase panic is not fatal (strict matching)",
			severity: "panic",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			diag := &PgDiagnostic{Severity: tc.severity}
			assert.Equal(t, tc.expected, diag.IsFatal())
		})
	}
}
