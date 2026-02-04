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

func TestNoticeToProtoAndBack(t *testing.T) {
	tests := []struct {
		name   string
		notice *Notice
	}{
		{
			name: "all fields populated",
			notice: &Notice{
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
			notice: &Notice{
				Severity: "NOTICE",
				Code:     "00000",
				Message:  "Simple notice",
				Detail:   "",
				Hint:     "",
			},
		},
		{
			name: "empty strings for optional fields",
			notice: &Notice{
				Severity: "INFO",
				Code:     "00000",
				Message:  "Notice with empty optional fields",
				Detail:   "",
				Hint:     "",
			},
		},
		{
			name: "unicode characters in message",
			notice: &Notice{
				Severity: "WARNING",
				Code:     "01000",
				Message:  "Unicode test: 日本語 🎉 émoji café",
				Detail:   "详细信息 with unicode",
				Hint:     "Подсказка на русском",
			},
		},
		{
			name: "special characters",
			notice: &Notice{
				Severity: "ERROR",
				Code:     "42P01",
				Message:  "relation \"users\" does not exist",
				Detail:   "Line 1: SELECT * FROM \"users\"\n        ^",
				Hint:     "Check if table name is quoted correctly",
			},
		},
		{
			name: "empty message",
			notice: &Notice{
				Severity: "NOTICE",
				Code:     "00000",
				Message:  "",
				Detail:   "",
				Hint:     "",
			},
		},
		{
			name: "context fields only",
			notice: &Notice{
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
			notice: &Notice{
				Severity: "WARNING",
				Code:     "01000",
				Message:  "Position test",
				Position: 1,
			},
		},
		{
			name: "where field with newlines",
			notice: &Notice{
				Severity: "NOTICE",
				Code:     "00000",
				Message:  "Call stack test",
				Where:    "PL/pgSQL function outer_func() line 3\nPL/pgSQL function inner_func() line 1",
			},
		},
		{
			name: "schema object fields only",
			notice: &Notice{
				Severity:   "ERROR",
				Code:       "23505",
				Message:    "duplicate key value violates unique constraint",
				Schema:     "myschema",
				Table:      "mytable",
				Column:     "mycolumn",
				DataType:   "integer",
				Constraint: "mytable_pkey",
			},
		},
		{
			name: "partial schema object fields",
			notice: &Notice{
				Severity: "WARNING",
				Code:     "01000",
				Message:  "Partial schema info",
				Schema:   "public",
				Table:    "orders",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Convert to proto
			protoNotice := NoticeToProto(tc.notice)
			require.NotNil(t, protoNotice)

			// Verify proto has correct values
			assert.Equal(t, tc.notice.Severity, protoNotice.Severity)
			assert.Equal(t, tc.notice.Code, protoNotice.Code)
			assert.Equal(t, tc.notice.Message, protoNotice.Message)
			assert.Equal(t, tc.notice.Detail, protoNotice.Detail)
			assert.Equal(t, tc.notice.Hint, protoNotice.Hint)
			assert.Equal(t, tc.notice.Position, protoNotice.Position)
			assert.Equal(t, tc.notice.InternalPosition, protoNotice.InternalPosition)
			assert.Equal(t, tc.notice.InternalQuery, protoNotice.InternalQuery)
			assert.Equal(t, tc.notice.Where, protoNotice.Where)
			assert.Equal(t, tc.notice.Schema, protoNotice.SchemaName)
			assert.Equal(t, tc.notice.Table, protoNotice.TableName)
			assert.Equal(t, tc.notice.Column, protoNotice.ColumnName)
			assert.Equal(t, tc.notice.DataType, protoNotice.DataTypeName)
			assert.Equal(t, tc.notice.Constraint, protoNotice.ConstraintName)

			// Convert back
			recovered := NoticeFromProto(protoNotice)
			require.NotNil(t, recovered)

			// Verify roundtrip preserves all fields
			assert.Equal(t, tc.notice.Severity, recovered.Severity)
			assert.Equal(t, tc.notice.Code, recovered.Code)
			assert.Equal(t, tc.notice.Message, recovered.Message)
			assert.Equal(t, tc.notice.Detail, recovered.Detail)
			assert.Equal(t, tc.notice.Hint, recovered.Hint)
			assert.Equal(t, tc.notice.Position, recovered.Position)
			assert.Equal(t, tc.notice.InternalPosition, recovered.InternalPosition)
			assert.Equal(t, tc.notice.InternalQuery, recovered.InternalQuery)
			assert.Equal(t, tc.notice.Where, recovered.Where)
			assert.Equal(t, tc.notice.Schema, recovered.Schema)
			assert.Equal(t, tc.notice.Table, recovered.Table)
			assert.Equal(t, tc.notice.Column, recovered.Column)
			assert.Equal(t, tc.notice.DataType, recovered.DataType)
			assert.Equal(t, tc.notice.Constraint, recovered.Constraint)
		})
	}
}

func TestNoticeFromProtoNil(t *testing.T) {
	assert.Nil(t, NoticeFromProto(nil))
}

func TestNoticeToProtoNil(t *testing.T) {
	assert.Nil(t, NoticeToProto(nil))
}
