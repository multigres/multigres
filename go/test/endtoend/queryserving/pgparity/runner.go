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

package pgparity

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Target identifies a PostgreSQL-compatible endpoint under test.
type Target struct {
	Name string // "postgres" or "multigateway"
	Host string
	Port int
	User string
	Pass string
	DB   string
}

// DSN returns a libpq-style connection string for this target.
func (t Target) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		t.Host, t.Port, t.User, t.Pass, t.DB)
}

// RecordResult holds the outcome of a single parsed record.
type RecordResult struct {
	LineNo int
	Kind   string // "statement" or "query"
	Pass   bool
	Reason string // failure reason, empty on pass
}

// FileResult aggregates per-record results for one test file on one target.
type FileResult struct {
	Total   int
	Passed  int
	Failed  int
	Records []RecordResult
}

// RunFile executes a parsed test file against a target and reports per-record
// results. A fresh database schema is used (public); callers are responsible
// for ensuring target state is clean before the run.
//
// Connection errors for individual records are reported as failures rather
// than aborting the file: this makes it easy to see how much of a file a
// target can handle. A hard connection failure at the start aborts the run.
func RunFile(ctx context.Context, tf *TestFile, target Target) (*FileResult, error) {
	conn, err := pgx.Connect(ctx, target.DSN())
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", target.Name, err)
	}
	defer conn.Close(ctx)

	fr := &FileResult{Total: len(tf.Records)}
	for _, rec := range tf.Records {
		res := runRecord(ctx, conn, rec)
		if res.Pass {
			fr.Passed++
		} else {
			fr.Failed++
		}
		fr.Records = append(fr.Records, res)
	}
	return fr, nil
}

// runRecord executes one parsed record and returns its result.
func runRecord(ctx context.Context, conn *pgx.Conn, rec Record) RecordResult {
	out := RecordResult{LineNo: rec.LineNo}

	switch rec.Kind {
	case DirectiveStatement:
		out.Kind = "statement"
		_, err := conn.Exec(ctx, rec.SQL)
		switch {
		case err == nil && rec.ExpectError:
			out.Reason = "expected error, got success"
		case err != nil && !rec.ExpectError:
			out.Reason = fmt.Sprintf("unexpected error: %v", err)
		case err != nil && rec.ExpectError && rec.ErrorPattern != "":
			// Optional regex match against error message.
			re, reErr := regexp.Compile(rec.ErrorPattern)
			if reErr != nil {
				out.Reason = fmt.Sprintf("invalid error pattern %q: %v", rec.ErrorPattern, reErr)
			} else if !re.MatchString(err.Error()) {
				out.Reason = fmt.Sprintf("error %q did not match pattern %q", err.Error(), rec.ErrorPattern)
			} else {
				out.Pass = true
			}
		default:
			out.Pass = true
		}
		return out

	case DirectiveQuery:
		out.Kind = "query"
		values, err := executeQuery(ctx, conn, rec)
		if err != nil {
			out.Reason = fmt.Sprintf("query error: %v", err)
			return out
		}
		if reason := compareQueryResult(values, rec); reason != "" {
			out.Reason = reason
			return out
		}
		out.Pass = true
		return out
	}

	out.Reason = "unknown directive kind"
	return out
}

// executeQuery runs a query record against the connection and returns the
// result set flattened into one value per cell, normalized by the type string
// specified in the record (I=int, R=real, T=text).
func executeQuery(ctx context.Context, conn *pgx.Conn, rec Record) ([]string, error) {
	rows, err := conn.Query(ctx, rec.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// nCols tracks the actual column count from the database, not the
	// declared type string length. Using the type string length would
	// silently mis-reshape the row grid for rowsort if a test author wrote a
	// too-short type string (e.g. `query II` for a 3-column SELECT) — rowsort
	// would sort across row boundaries without any error.
	nCols := 0
	var flat []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		nCols = len(vals)
		for i, v := range vals {
			var typeChar byte
			if i < len(rec.TypeString) {
				typeChar = rec.TypeString[i]
			} else {
				typeChar = 'T'
			}
			flat = append(flat, formatValue(v, typeChar))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply sort mode: rowsort reorders whole rows before comparison, used
	// when the SQL has no ORDER BY and the test doesn't care about order.
	if rec.SortMode == "rowsort" {
		flat = rowsort(flat, nCols)
	}
	return flat, nil
}

// formatValue converts a pgx-decoded value into its canonical string form
// based on the declared type char:
//
//	I — integer (NULL becomes "NULL")
//	R — real/float, formatted with three decimal places
//	T — text ("" becomes "(empty)", NULL becomes "NULL")
//
// Unknown type chars fall back to generic string formatting.
func formatValue(v any, typeChar byte) string {
	if v == nil {
		return "NULL"
	}
	switch typeChar {
	case 'I':
		switch x := v.(type) {
		case int, int32, int64, int16, int8:
			return fmt.Sprintf("%d", x)
		case float64:
			return strconv.FormatInt(int64(x), 10)
		case bool:
			if x {
				return "1"
			}
			return "0"
		case *big.Int:
			return x.String()
		case pgtype.Numeric:
			// PostgreSQL NUMERIC type comes back as this struct. Convert via
			// Float64Value and truncate to an integer for 'I' columns.
			if f, err := x.Float64Value(); err == nil && f.Valid {
				return strconv.FormatInt(int64(f.Float64), 10)
			}
			return fmt.Sprintf("%v", x)
		default:
			return fmt.Sprintf("%v", x)
		}
	case 'R':
		switch x := v.(type) {
		case float32:
			// bitSize=32 so rounding matches the original float32 precision;
			// PG `real` / `float4` columns come back as float32 via pgx.
			return strconv.FormatFloat(float64(x), 'f', 3, 32)
		case float64:
			return strconv.FormatFloat(x, 'f', 3, 64)
		case int, int32, int64, int16, int8:
			return strconv.FormatFloat(toFloat64(x), 'f', 3, 64)
		case pgtype.Numeric:
			// Division and aggregates like avg() return NUMERIC in pgx. Render
			// with the same 3-decimal convention as floats for comparability.
			if f, err := x.Float64Value(); err == nil && f.Valid {
				return strconv.FormatFloat(f.Float64, 'f', 3, 64)
			}
			return fmt.Sprintf("%v", x)
		default:
			return fmt.Sprintf("%v", x)
		}
	case 'T':
		s := fmt.Sprintf("%v", v)
		if s == "" {
			return "(empty)"
		}
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

// rowsort sorts a flat slice of values as if it were a 2D grid of nCols
// columns per row, preserving per-row order within the result.
func rowsort(flat []string, nCols int) []string {
	if nCols <= 1 {
		out := make([]string, len(flat))
		copy(out, flat)
		sort.Strings(out)
		return out
	}
	if len(flat)%nCols != 0 {
		// Should not happen for well-formed queries, but don't panic in tests.
		return flat
	}
	rowCount := len(flat) / nCols
	rows := make([][]string, rowCount)
	for i := range rows {
		rows[i] = flat[i*nCols : (i+1)*nCols]
	}
	sort.Slice(rows, func(i, j int) bool {
		for k := range nCols {
			if rows[i][k] != rows[j][k] {
				return rows[i][k] < rows[j][k]
			}
		}
		return false
	})
	out := make([]string, 0, len(flat))
	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}

// compareQueryResult returns the empty string when values match the expected
// rows, or a human-readable failure reason otherwise.
//
// Expected rows may themselves be tab-split during parsing, so both sides are
// flat value-per-slot lists.
func compareQueryResult(values []string, rec Record) string {
	// An empty expected section (no `----` block) means the test only
	// required the query to execute without error.
	if rec.ExpectedCount == 0 {
		return ""
	}
	if len(values) != rec.ExpectedCount {
		return fmt.Sprintf("got %d values, expected %d\ngot: %s\nwant: %s",
			len(values), rec.ExpectedCount,
			truncateList(values), truncateList(rec.ExpectedRows))
	}
	for i, got := range values {
		if got != rec.ExpectedRows[i] {
			return fmt.Sprintf("value %d mismatch: got %q, expected %q\nfull got:  %s\nfull want: %s",
				i, got, rec.ExpectedRows[i],
				truncateList(values), truncateList(rec.ExpectedRows))
		}
	}
	return ""
}

// truncateList renders a slice for error messages, cutting off after a few
// elements so we don't dump megabytes into test logs.
func truncateList(xs []string) string {
	const max = 20
	if len(xs) <= max {
		return "[" + strings.Join(xs, " | ") + "]"
	}
	return "[" + strings.Join(xs[:max], " | ") + fmt.Sprintf(" ...+%d more]", len(xs)-max)
}
