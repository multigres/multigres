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

package ast_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/multigres/multigres/go/parser"
	"github.com/multigres/multigres/go/parser/ast"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubstituteParameters(t *testing.T) {
	t.Run("no parameters", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT 1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		result, err := ast.SubstituteParameters(stmts[0], nil, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 1", result.SqlString())
	})

	t.Run("text parameter - integer", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameter: $1 = "42" (text format, int4 type)
		params := [][]byte{[]byte("42")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 42", result.SqlString())
	})

	t.Run("text parameter - string", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameter: $1 = "hello" (text format, text type)
		params := [][]byte{[]byte("hello")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 'hello'", result.SqlString())
	})

	t.Run("null parameter", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameter: $1 = NULL
		params := [][]byte{nil}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT NULL", result.SqlString())
	})

	t.Run("multiple parameters", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1, $2")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameters: $1 = "100", $2 = "hello"
		params := [][]byte{[]byte("100"), []byte("hello")}
		formats := []int16{ast.TextFormat, ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID), uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 100, 'hello'", result.SqlString())
	})

	t.Run("boolean parameter", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameter: $1 = "true" (text format, bool type)
		params := [][]byte{[]byte("true")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.BOOLOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT TRUE", result.SqlString())
	})

	t.Run("where clause with parameter", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT * FROM users WHERE id = $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameter: $1 = "42"
		params := [][]byte{[]byte("42")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM users WHERE id = 42", result.SqlString())
	})

	t.Run("multiple parameters in WHERE", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT * FROM users WHERE id = $1 AND name = $2")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Parameters: $1 = "42", $2 = "Alice"
		params := [][]byte{[]byte("42"), []byte("Alice")}
		formats := []int16{ast.TextFormat, ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID), uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM users WHERE id = 42 AND name = 'Alice'", result.SqlString())
	})

	t.Run("binary int4", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode 42 as 4-byte big-endian integer
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 42)

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.INT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 42", result.SqlString())
	})

	t.Run("binary int8", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode large number as 8-byte big-endian integer
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 9223372036854775807) // Max int64

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.INT8OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 9223372036854775807", result.SqlString())
	})

	t.Run("binary negative int4", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode -42 as 4-byte big-endian integer (two's complement)
		data := make([]byte, 4)
		var negValue int32 = -42
		binary.BigEndian.PutUint32(data, uint32(negValue))

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.INT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT -42", result.SqlString())
	})

	t.Run("binary float4", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode 3.14 as 4-byte float
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, math.Float32bits(3.14))

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.FLOAT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 3.14", result.SqlString())
	})

	t.Run("binary float8", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode π as 8-byte float
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, math.Float64bits(math.Pi))

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.FLOAT8OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 3.141592653589793", result.SqlString())
	})

	t.Run("binary bool true", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Boolean true is 1 byte with non-zero value
		params := [][]byte{{1}}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.BOOLOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT TRUE", result.SqlString())
	})

	t.Run("binary bool false", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Boolean false is 1 byte with zero value
		params := [][]byte{{0}}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.BOOLOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT FALSE", result.SqlString())
	})

	t.Run("binary text", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Text in binary format is just UTF-8 bytes
		params := [][]byte{[]byte("hello world")}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 'hello world'", result.SqlString())
	})

	t.Run("binary timestamp", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// PostgreSQL timestamp: microseconds since 2000-01-01 00:00:00 UTC
		// Let's encode 2000-01-01 00:00:01 (1 second = 1,000,000 microseconds)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 1000000) // 1 second in microseconds

		params := [][]byte{data}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.TIMESTAMPOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT '2000-01-01 00:00:01'", result.SqlString())
	})

	t.Run("binary bytea", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Binary data encoded as hex
		params := [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}}
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.BYTEAOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT '\\xdeadbeef'", result.SqlString())
	})

	t.Run("mixed text and binary formats", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1, $2, $3")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// First param: text format integer
		// Second param: binary format integer
		// Third param: text format string
		binaryInt := make([]byte, 4)
		binary.BigEndian.PutUint32(binaryInt, 200)

		params := [][]byte{
			[]byte("100"),  // text
			binaryInt,      // binary
			[]byte("test"), // text
		}
		formats := []int16{ast.TextFormat, ast.BinaryFormat, ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID), uint32(ast.INT4OID), uint32(ast.TEXTOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 100, 200, 'test'", result.SqlString())
	})

	t.Run("binary format errors", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// INT4 requires exactly 4 bytes
		params := [][]byte{{1, 2, 3}} // Only 3 bytes
		formats := []int16{ast.BinaryFormat}
		types := []uint32{uint32(ast.INT4OID)}

		_, err = ast.SubstituteParameters(stmts[0], params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected 4 bytes")
	})

	t.Run("binary format complex query", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT * FROM users WHERE id = $1 AND age > $2 AND active = $3")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Encode parameters in binary format
		idData := make([]byte, 4)
		binary.BigEndian.PutUint32(idData, 42)

		ageData := make([]byte, 4)
		binary.BigEndian.PutUint32(ageData, 25)

		params := [][]byte{idData, ageData, {1}} // id, age, active
		formats := []int16{ast.BinaryFormat}     // All binary
		types := []uint32{uint32(ast.INT4OID), uint32(ast.INT4OID), uint32(ast.BOOLOID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM users WHERE id = 42 AND age > 25 AND active = TRUE", result.SqlString())
	})
}

func TestParameterErrors(t *testing.T) {
	t.Run("invalid integer", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		params := [][]byte{[]byte("not a number")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.INT4OID)}

		_, err = ast.SubstituteParameters(stmts[0], params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid integer")
	})

	t.Run("invalid boolean", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		params := [][]byte{[]byte("maybe")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.BOOLOID)}

		_, err = ast.SubstituteParameters(stmts[0], params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid boolean")
	})

	t.Run("invalid float", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		params := [][]byte{[]byte("not a float")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.FLOAT8OID)}

		_, err = ast.SubstituteParameters(stmts[0], params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid float")
	})
}

func TestParameterFormats(t *testing.T) {
	t.Run("single format applies to all parameters", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1, $2")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Single format code should apply to all parameters
		params := [][]byte{[]byte("42"), []byte("100")}
		formats := []int16{ast.TextFormat} // Single format for all
		types := []uint32{uint32(ast.INT4OID), uint32(ast.INT4OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 42, 100", result.SqlString())
	})

	t.Run("float parameter", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT $1")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		params := [][]byte{[]byte("3.14159")}
		formats := []int16{ast.TextFormat}
		types := []uint32{uint32(ast.FLOAT8OID)}

		result, err := ast.SubstituteParameters(stmts[0], params, formats, types)
		require.NoError(t, err)
		assert.Equal(t, "SELECT 3.14159", result.SqlString())
	})
}
