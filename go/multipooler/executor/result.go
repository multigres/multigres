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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/pb/query"
)

// ScanRow scans values from a query result row into the provided destinations.
// Each destination should be a pointer to a supported type (bool, string, int, int32, int64, float64, time.Time).
func ScanRow(row *query.Row, dests ...any) error {
	if row == nil {
		return fmt.Errorf("row is nil")
	}
	if len(row.Values) < len(dests) {
		return fmt.Errorf("not enough columns: got %d, want %d", len(row.Values), len(dests))
	}

	for i, dest := range dests {
		val := row.Values[i]
		if err := scanValue(val, dest); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

// ScanSingleRow is a convenience function that scans the first row of a result.
// Returns an error if the result has no rows.
func ScanSingleRow(result *query.QueryResult, dests ...any) error {
	if result == nil || len(result.Rows) == 0 {
		return fmt.Errorf("no rows in result")
	}
	return ScanRow(result.Rows[0], dests...)
}

// scanValue scans a single value into the destination.
func scanValue(val []byte, dest any) error {
	if val == nil {
		// Handle NULL values - for now, leave the destination unchanged
		// (similar to sql.Scanner behavior with default values)
		return nil
	}

	s := string(val)

	switch d := dest.(type) {
	case *bool:
		// PostgreSQL returns "t" or "f" for boolean values
		switch strings.ToLower(s) {
		case "t", "true", "1":
			*d = true
		case "f", "false", "0":
			*d = false
		default:
			return fmt.Errorf("cannot parse %q as bool", s)
		}
	case *string:
		*d = s
	case *int:
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("cannot parse %q as int: %w", s, err)
		}
		*d = v
	case *int32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return fmt.Errorf("cannot parse %q as int32: %w", s, err)
		}
		*d = int32(v)
	case *int64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as int64: %w", s, err)
		}
		*d = v
	case *float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as float64: %w", s, err)
		}
		*d = v
	case *time.Time:
		// PostgreSQL timestamp formats from the PostgreSQL documentation:
		// https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
		// We support the ISO 8601 style formats commonly returned by PostgreSQL.
		for _, format := range []string{
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			time.RFC3339,
		} {
			if t, err := time.Parse(format, s); err == nil {
				*d = t
				return nil
			}
		}
		return fmt.Errorf("cannot parse %q as time.Time", s)
	case *[]byte:
		*d = val
	default:
		return fmt.Errorf("unsupported destination type: %T", dest)
	}
	return nil
}

// getValue is a helper that validates the row and column, then scans the value into dest.
func getValue[T any](row *query.Row, col int) (T, error) {
	var result T
	if row == nil {
		return result, fmt.Errorf("row is nil")
	}
	if col >= len(row.Values) {
		return result, fmt.Errorf("column index %d out of range", col)
	}
	if err := scanValue(row.Values[col], &result); err != nil {
		return result, err
	}
	return result, nil
}

// GetString extracts a string value from a row at the given column index.
func GetString(row *query.Row, col int) (string, error) {
	return getValue[string](row, col)
}

// GetBool extracts a boolean value from a row at the given column index.
func GetBool(row *query.Row, col int) (bool, error) {
	return getValue[bool](row, col)
}

// GetInt extracts an integer value from a row at the given column index.
func GetInt(row *query.Row, col int) (int, error) {
	return getValue[int](row, col)
}

// GetInt32 extracts an int32 value from a row at the given column index.
func GetInt32(row *query.Row, col int) (int32, error) {
	return getValue[int32](row, col)
}

// GetInt64 extracts an int64 value from a row at the given column index.
func GetInt64(row *query.Row, col int) (int64, error) {
	return getValue[int64](row, col)
}

// ParseFloat64 parses a string as a float64.
// This is useful for parsing numeric values that were cast to text in SQL.
func ParseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
