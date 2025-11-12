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

package ast

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Format codes for parameter encoding
const (
	TextFormat   = 0
	BinaryFormat = 1
)

// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// SubstituteParameters walks through the AST and replaces all ParamRef nodes
// with A_Const nodes containing the actual parameter values.
// This function creates a modified copy of the AST with parameters substituted.
//
// Parameters:
//   - stmt: The AST statement to process
//   - params: The parameter values as byte arrays (nil for NULL)
//   - paramFormats: Format codes for each parameter (0=text, 1=binary)
//   - paramTypes: PostgreSQL type OIDs for each parameter
//
// Returns the modified statement with parameters substituted, or an error if
// parameter parsing fails.
func SubstituteParameters(stmt Stmt, params [][]byte, paramFormats []int16, paramTypes []uint32) (Stmt, error) {
	if len(params) == 0 {
		return stmt, nil
	}

	// Create parameter value cache to convert params to A_Const nodes
	paramValues, err := createParameterValues(params, paramFormats, paramTypes)
	if err != nil {
		return nil, err
	}

	// Use Rewrite to walk the AST and replace ParamRef nodes
	rewritten := Rewrite(stmt, func(cursor *Cursor) bool {
		if paramRef, ok := cursor.Node().(*ParamRef); ok {
			// Parameter numbers are 1-based
			paramIndex := paramRef.Number - 1
			if paramIndex >= 0 && paramIndex < len(paramValues) {
				cursor.Replace(paramValues[paramIndex])
			}
			// If parameter index is out of bounds, leave ParamRef unchanged
			return false // No need to traverse children of ParamRef
		}
		return true // Continue traversal for other nodes
	}, nil)

	return rewritten.(Stmt), nil
}

// createParameterValues converts parameter bytes to A_Const nodes
func createParameterValues(params [][]byte, formats []int16, paramTypes []uint32) ([]*A_Const, error) {
	paramValues := make([]*A_Const, len(params))

	// Determine format for each parameter
	getFormat := func(i int) int16 {
		if len(formats) == 0 {
			return TextFormat
		}
		if len(formats) == 1 {
			return formats[0]
		}
		if i < len(formats) {
			return formats[i]
		}
		return TextFormat
	}

	for i := range len(params) {
		format := getFormat(i)
		paramData := params[i]

		// Handle NULL
		if paramData == nil {
			paramValues[i] = NewA_ConstNull(0)
			continue
		}

		// Get type OID if available
		var typeOID uint32
		if i < len(paramTypes) && paramTypes[i] != 0 {
			typeOID = paramTypes[i]
		}

		// Parse parameter based on format
		var value *A_Const
		var err error

		if format == BinaryFormat {
			value, err = parseBinaryParameter(paramData, typeOID)
		} else {
			value, err = parseTextParameter(paramData, typeOID)
		}

		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", i+1, err)
		}

		paramValues[i] = value
	}

	return paramValues, nil
}

// parseTextParameter parses a text format parameter into an A_Const node
func parseTextParameter(data []byte, typeOID uint32) (*A_Const, error) {
	value := string(data)

	switch typeOID {
	case uint32(INT4OID), uint32(INT8OID):
		// Validate and parse as integer
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %q", value)
		}
		return NewA_Const(NewInteger(int(intVal)), 0), nil

	case uint32(FLOAT4OID), uint32(FLOAT8OID):
		// Validate it's a float
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return nil, fmt.Errorf("invalid float: %q", value)
		}
		return NewA_Const(NewFloat(value), 0), nil

	case uint32(BOOLOID):
		// Parse boolean
		lower := strings.ToLower(value)
		switch lower {
		case "t", "true", "y", "yes", "on", "1":
			return NewA_Const(NewBoolean(true), 0), nil
		case "f", "false", "n", "no", "off", "0":
			return NewA_Const(NewBoolean(false), 0), nil
		default:
			return nil, fmt.Errorf("invalid boolean: %q", value)
		}

	case uint32(TEXTOID), uint32(VARCHAROID), 0: // 0 = unknown type, treat as text
		return NewA_Const(NewString(value), 0), nil

	case uint32(BYTEAOID):
		// Bytea in text format
		return NewA_Const(NewString(value), 0), nil

	default:
		// Unknown type, treat as text
		return NewA_Const(NewString(value), 0), nil
	}
}

// parseBinaryParameter parses a binary format parameter into an A_Const node
func parseBinaryParameter(data []byte, typeOID uint32) (*A_Const, error) {
	switch typeOID {
	case uint32(INT4OID):
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid int4 binary data: expected 4 bytes, got %d", len(data))
		}
		value := int32(binary.BigEndian.Uint32(data))
		return NewA_Const(NewInteger(int(value)), 0), nil

	case uint32(INT8OID):
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid int8 binary data: expected 8 bytes, got %d", len(data))
		}
		value := int64(binary.BigEndian.Uint64(data))
		return NewA_Const(NewInteger(int(value)), 0), nil

	case uint32(FLOAT4OID):
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid float4 binary data: expected 4 bytes, got %d", len(data))
		}
		bits := binary.BigEndian.Uint32(data)
		value := math.Float32frombits(bits)
		return NewA_Const(NewFloat(fmt.Sprintf("%g", value)), 0), nil

	case uint32(FLOAT8OID):
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid float8 binary data: expected 8 bytes, got %d", len(data))
		}
		bits := binary.BigEndian.Uint64(data)
		value := math.Float64frombits(bits)
		return NewA_Const(NewFloat(fmt.Sprintf("%g", value)), 0), nil

	case uint32(BOOLOID):
		if len(data) != 1 {
			return nil, fmt.Errorf("invalid bool binary data: expected 1 byte, got %d", len(data))
		}
		return NewA_Const(NewBoolean(data[0] != 0), 0), nil

	case uint32(TEXTOID), uint32(VARCHAROID):
		// Text in binary format is just UTF-8 bytes
		return NewA_Const(NewString(string(data)), 0), nil

	case uint32(TIMESTAMPOID):
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid timestamp binary data: expected 8 bytes, got %d", len(data))
		}
		// PostgreSQL timestamp: microseconds since 2000-01-01 00:00:00 UTC
		microseconds := int64(binary.BigEndian.Uint64(data))
		timestamp := pgEpoch.Add(time.Duration(microseconds) * time.Microsecond)
		// Return as string value that will be properly quoted
		return NewA_Const(NewString(timestamp.Format("2006-01-02 15:04:05.999999")), 0), nil

	case uint32(BYTEAOID):
		// Bytea in binary format: raw bytes, encode as hex string
		hexStr := fmt.Sprintf("\\x%x", data)
		return NewA_Const(NewString(hexStr), 0), nil

	default:
		// Unknown binary type - try to interpret as text
		return NewA_Const(NewString(string(data)), 0), nil
	}
}
