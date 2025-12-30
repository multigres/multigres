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

package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
)

// makeRow creates a sqltypes.Row from a slice of values (nil values become NULL).
func makeRow(values ...any) *sqltypes.Row {
	row := &sqltypes.Row{Values: make([]sqltypes.Value, len(values))}
	for i, v := range values {
		if v == nil {
			row.Values[i] = nil
		} else if b, ok := v.([]byte); ok {
			row.Values[i] = b
		} else {
			row.Values[i] = []byte(v.(string))
		}
	}
	return row
}

// makeResult creates a sqltypes.Result with the given rows.
func makeResult(rows ...*sqltypes.Row) *sqltypes.Result {
	return &sqltypes.Result{Rows: rows}
}

func TestScanRow(t *testing.T) {
	t.Run("nil row returns error", func(t *testing.T) {
		var s string
		err := ScanRow(nil, &s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "row is nil")
	})

	t.Run("not enough columns returns error", func(t *testing.T) {
		row := makeRow("value1")
		var s1, s2 string
		err := ScanRow(row, &s1, &s2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not enough columns")
	})

	t.Run("scan multiple values successfully", func(t *testing.T) {
		row := makeRow("hello", "42", "t")
		var s string
		var i int
		var b bool
		err := ScanRow(row, &s, &i, &b)
		require.NoError(t, err)
		assert.Equal(t, "hello", s)
		assert.Equal(t, 42, i)
		assert.True(t, b)
	})

	t.Run("extra columns are ignored", func(t *testing.T) {
		row := makeRow("hello", "world", "extra")
		var s1, s2 string
		err := ScanRow(row, &s1, &s2)
		require.NoError(t, err)
		assert.Equal(t, "hello", s1)
		assert.Equal(t, "world", s2)
	})
}

func TestScanSingleRow(t *testing.T) {
	t.Run("nil result returns error", func(t *testing.T) {
		var s string
		err := ScanSingleRow(nil, &s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no rows")
	})

	t.Run("empty result returns error", func(t *testing.T) {
		result := &sqltypes.Result{Rows: []*sqltypes.Row{}}
		var s string
		err := ScanSingleRow(result, &s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no rows")
	})

	t.Run("scans first row successfully", func(t *testing.T) {
		result := makeResult(makeRow("first"), makeRow("second"))
		var s string
		err := ScanSingleRow(result, &s)
		require.NoError(t, err)
		assert.Equal(t, "first", s)
	})
}

func TestScanValue_Bool(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    bool
		expectError string // empty means no error expected
	}{
		{"t", "t", true, ""},
		{"T", "T", true, ""},
		{"true", "true", true, ""},
		{"TRUE", "TRUE", true, ""},
		{"1", "1", true, ""},
		{"f", "f", false, ""},
		{"F", "F", false, ""},
		{"false", "false", false, ""},
		{"FALSE", "FALSE", false, ""},
		{"0", "0", false, ""},
		{"invalid", "invalid", false, `cannot parse "invalid" as bool`},
		{"empty", "", false, `cannot parse "" as bool`},
		{"yes", "yes", false, `cannot parse "yes" as bool`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var b bool
			err := ScanRow(row, &b)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, b)
			}
		})
	}
}

func TestScanValue_String(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple string", "hello", "hello"},
		{"empty string", "", ""},
		{"string with spaces", "hello world", "hello world"},
		{"string with special chars", "hello\nworld\ttab", "hello\nworld\ttab"},
		{"unicode", "hello 世界", "hello 世界"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var s string
			err := ScanRow(row, &s)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, s)
		})
	}
}

func TestScanValue_Int(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int
		expectError string // empty means no error expected
	}{
		{"zero", "0", 0, ""},
		{"positive", "42", 42, ""},
		{"negative", "-42", -42, ""},
		{"large positive", "2147483647", 2147483647, ""},
		{"large negative", "-2147483648", -2147483648, ""},
		{"float", "42.5", 0, `cannot parse "42.5" as int`},
		{"not a number", "abc", 0, `cannot parse "abc" as int`},
		{"empty", "", 0, `cannot parse "" as int`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var i int
			err := ScanRow(row, &i)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, i)
			}
		})
	}
}

func TestScanValue_Int32(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int32
		expectError string // empty means no error expected
	}{
		{"zero", "0", 0, ""},
		{"positive", "42", 42, ""},
		{"negative", "-42", -42, ""},
		{"max int32", "2147483647", 2147483647, ""},
		{"min int32", "-2147483648", -2147483648, ""},
		{"overflow", "2147483648", 0, `cannot parse "2147483648" as int32`},
		{"underflow", "-2147483649", 0, `cannot parse "-2147483649" as int32`},
		{"not a number", "abc", 0, `cannot parse "abc" as int32`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var i int32
			err := ScanRow(row, &i)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, i)
			}
		})
	}
}

func TestScanValue_Int64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int64
		expectError string // empty means no error expected
	}{
		{"zero", "0", 0, ""},
		{"positive", "42", 42, ""},
		{"negative", "-42", -42, ""},
		{"large positive", "9223372036854775807", 9223372036854775807, ""},
		{"large negative", "-9223372036854775808", -9223372036854775808, ""},
		{"overflow", "9223372036854775808", 0, `cannot parse "9223372036854775808" as int64`},
		{"not a number", "abc", 0, `cannot parse "abc" as int64`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var i int64
			err := ScanRow(row, &i)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, i)
			}
		})
	}
}

func TestScanValue_Float64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    float64
		expectError string // empty means no error expected
	}{
		{"zero", "0", 0.0, ""},
		{"positive", "42.5", 42.5, ""},
		{"negative", "-42.5", -42.5, ""},
		{"integer", "42", 42.0, ""},
		{"scientific notation", "1.5e10", 1.5e10, ""},
		{"negative exponent", "1.5e-10", 1.5e-10, ""},
		{"not a number", "abc", 0, `cannot parse "abc" as float64`},
		{"empty", "", 0, `cannot parse "" as float64`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var f float64
			err := ScanRow(row, &f)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.expected, f, 1e-15)
			}
		})
	}
}

func TestScanValue_Time(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    time.Time
		expectError string // empty means no error expected
	}{
		{
			name:     "timestamp with timezone offset",
			input:    "2024-01-15 10:30:45.123456-07",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.FixedZone("", -7*3600)),
		},
		{
			name:     "timestamp with positive offset",
			input:    "2024-01-15 10:30:45.123456+05",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.FixedZone("", 5*3600)),
		},
		{
			name:     "timestamp without timezone (microseconds)",
			input:    "2024-01-15 10:30:45.123456",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC),
		},
		{
			name:     "timestamp without timezone (no microseconds)",
			input:    "2024-01-15 10:30:45",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:     "RFC3339 format",
			input:    "2024-01-15T10:30:45Z",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:     "RFC3339 with offset",
			input:    "2024-01-15T10:30:45+05:30",
			expected: time.Date(2024, 1, 15, 10, 30, 45, 0, time.FixedZone("", 5*3600+30*60)),
		},
		{
			name:        "invalid format",
			input:       "not a timestamp",
			expectError: `cannot parse "not a timestamp" as time.Time`,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: `cannot parse "" as time.Time`,
		},
		{
			name:        "date only",
			input:       "2024-01-15",
			expectError: `cannot parse "2024-01-15" as time.Time`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := makeRow(tt.input)
			var tm time.Time
			err := ScanRow(row, &tm)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				require.NoError(t, err)
				assert.True(t, tt.expected.Equal(tm), "expected %v, got %v", tt.expected, tm)
			}
		})
	}
}

func TestScanValue_Bytes(t *testing.T) {
	t.Run("scan bytes", func(t *testing.T) {
		row := makeRow([]byte("binary data"))
		var b []byte
		err := ScanRow(row, &b)
		require.NoError(t, err)
		assert.Equal(t, []byte("binary data"), b)
	})

	t.Run("scan empty bytes", func(t *testing.T) {
		row := makeRow([]byte{})
		var b []byte
		err := ScanRow(row, &b)
		require.NoError(t, err)
		assert.Equal(t, []byte{}, b)
	})
}

func TestScanValue_NULL(t *testing.T) {
	t.Run("NULL leaves bool unchanged", func(t *testing.T) {
		row := makeRow(nil)
		b := true // set a non-zero value
		err := ScanRow(row, &b)
		require.NoError(t, err)
		assert.True(t, b) // should remain unchanged
	})

	t.Run("NULL leaves string unchanged", func(t *testing.T) {
		row := makeRow(nil)
		s := "original"
		err := ScanRow(row, &s)
		require.NoError(t, err)
		assert.Equal(t, "original", s)
	})

	t.Run("NULL leaves int unchanged", func(t *testing.T) {
		row := makeRow(nil)
		i := 42
		err := ScanRow(row, &i)
		require.NoError(t, err)
		assert.Equal(t, 42, i)
	})
}

func TestScanValue_UnsupportedType(t *testing.T) {
	t.Run("unsupported type returns error", func(t *testing.T) {
		row := makeRow("42")
		var u uint
		err := ScanRow(row, &u)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported destination type")
	})

	t.Run("non-pointer returns error", func(t *testing.T) {
		row := makeRow("hello")
		s := "hello"
		err := ScanRow(row, s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported destination type")
	})
}

func TestGetString(t *testing.T) {
	t.Run("get string value", func(t *testing.T) {
		row := makeRow("hello", "world")
		s, err := GetString(row, 0)
		require.NoError(t, err)
		assert.Equal(t, "hello", s)

		s, err = GetString(row, 1)
		require.NoError(t, err)
		assert.Equal(t, "world", s)
	})

	t.Run("nil row returns error", func(t *testing.T) {
		_, err := GetString(nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "row is nil")
	})

	t.Run("out of range returns error", func(t *testing.T) {
		row := makeRow("hello")
		_, err := GetString(row, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})
}

func TestGetBool(t *testing.T) {
	t.Run("get true value", func(t *testing.T) {
		row := makeRow("t")
		b, err := GetBool(row, 0)
		require.NoError(t, err)
		assert.True(t, b)
	})

	t.Run("get false value", func(t *testing.T) {
		row := makeRow("f")
		b, err := GetBool(row, 0)
		require.NoError(t, err)
		assert.False(t, b)
	})
}

func TestGetInt(t *testing.T) {
	t.Run("get int value", func(t *testing.T) {
		row := makeRow("42")
		i, err := GetInt(row, 0)
		require.NoError(t, err)
		assert.Equal(t, 42, i)
	})

	t.Run("parse error", func(t *testing.T) {
		row := makeRow("not a number")
		_, err := GetInt(row, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `cannot parse "not a number" as int`)
	})
}

func TestGetInt32(t *testing.T) {
	t.Run("get int32 value", func(t *testing.T) {
		row := makeRow("42")
		i, err := GetInt32(row, 0)
		require.NoError(t, err)
		assert.Equal(t, int32(42), i)
	})
}

func TestGetInt64(t *testing.T) {
	t.Run("get int64 value", func(t *testing.T) {
		row := makeRow("9223372036854775807")
		i, err := GetInt64(row, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), i)
	})
}

func TestParseFloat64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    float64
		expectError bool
	}{
		{"zero", "0", 0.0, false},
		{"positive", "42.5", 42.5, false},
		{"negative", "-42.5", -42.5, false},
		{"scientific", "1e10", 1e10, false},
		{"invalid", "abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := ParseFloat64(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.expected, f, 1e-15)
			}
		})
	}
}

func TestScanRow_ErrorInMiddle(t *testing.T) {
	t.Run("error scanning third column", func(t *testing.T) {
		row := makeRow("hello", "42", "not-a-bool")
		var s string
		var i int
		var b bool
		err := ScanRow(row, &s, &i, &b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 2")
		// Values scanned before the error should still be set
		assert.Equal(t, "hello", s)
		assert.Equal(t, 42, i)
	})
}

func TestScanRowAllowNils(t *testing.T) {
	t.Run("nullable string with nil value", func(t *testing.T) {
		row := makeRow("hello", nil, "world")
		var s *string
		var ns *string
		var w *string
		err := ScanRow(row, &s, &ns, &w)
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, "hello", *s)
		assert.Nil(t, ns) // NULL value
		require.NotNil(t, w)
		assert.Equal(t, "world", *w)
	})

	t.Run("nullable int with nil value", func(t *testing.T) {
		row := makeRow("42", nil, "100")
		var a *int
		var b *int
		var c *int
		err := ScanRow(row, &a, &b, &c)
		require.NoError(t, err)
		require.NotNil(t, a)
		assert.Equal(t, 42, *a)
		assert.Nil(t, b) // NULL value
		require.NotNil(t, c)
		assert.Equal(t, 100, *c)
	})

	t.Run("nullable bool with nil value", func(t *testing.T) {
		row := makeRow("t", nil)
		var a *bool
		var b *bool
		err := ScanRow(row, &a, &b)
		require.NoError(t, err)
		require.NotNil(t, a)
		assert.True(t, *a)
		assert.Nil(t, b) // NULL value
	})

	t.Run("mix of nullable and non-nullable", func(t *testing.T) {
		row := makeRow("hello", nil, "42")
		var s *string  // nullable
		var ns *string // nullable, will be nil
		var i int      // non-nullable
		err := ScanRow(row, &s, &ns, &i)
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, "hello", *s)
		assert.Nil(t, ns)
		assert.Equal(t, 42, i)
	})
}
