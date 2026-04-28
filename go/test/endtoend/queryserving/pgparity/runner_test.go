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
	"math/big"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// TestFormatValue covers the type-char driven formatting that shapes how
// query output is compared against expected rows.
func TestFormatValue(t *testing.T) {
	cases := []struct {
		name string
		v    any
		typ  byte
		want string
	}{
		{"int as I", int64(42), 'I', "42"},
		{"float truncated to int under I", float64(3.7), 'I', "3"},
		{"bool true as I", true, 'I', "1"},
		{"bool false as I", false, 'I', "0"},
		{"nil is NULL", nil, 'I', "NULL"},
		{"float as R formats with 3 decimals", float64(3.14159), 'R', "3.142"},
		{"int as R", int64(7), 'R', "7.000"},
		{"text as T", "hello", 'T', "hello"},
		{"empty text as T", "", 'T', "(empty)"},
		{"nil as T is NULL", nil, 'T', "NULL"},
		// pgx returns PostgreSQL NUMERIC (e.g. from division, avg) as
		// pgtype.Numeric. Must decode through Float64Value, not fall through
		// to %v which would print the struct literal.
		{"numeric 3.5 as R", pgtype.Numeric{Int: big.NewInt(35000000000000000), Exp: -16, Valid: true}, 'R', "3.500"},
		{"numeric 2 as I", pgtype.Numeric{Int: big.NewInt(2), Exp: 0, Valid: true}, 'I', "2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatValue(tc.v, tc.typ); got != tc.want {
				t.Errorf("formatValue(%v, %q) = %q, want %q", tc.v, tc.typ, got, tc.want)
			}
		})
	}
}

// TestRowsort verifies the row-level sort used by `query ... rowsort`, which
// must order whole rows (multi-column) rather than individual cells.
func TestRowsort(t *testing.T) {
	// 2-column grid: (2,b) (1,a) (1,c) → sorted should be (1,a) (1,c) (2,b)
	in := []string{"2", "b", "1", "a", "1", "c"}
	want := []string{"1", "a", "1", "c", "2", "b"}
	got := rowsort(in, 2)
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("idx %d: got %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestCompareQueryResult verifies the success/failure classification for
// query result comparison.
func TestCompareQueryResult(t *testing.T) {
	rec := Record{
		TypeString:    "II",
		ExpectedRows:  []string{"1", "one", "2", "two"},
		ExpectedCount: 4,
	}
	// Exact match.
	if reason := compareQueryResult([]string{"1", "one", "2", "two"}, rec); reason != "" {
		t.Errorf("expected match, got reason: %s", reason)
	}
	// Length mismatch.
	if reason := compareQueryResult([]string{"1", "one"}, rec); reason == "" {
		t.Error("expected length-mismatch failure, got empty reason")
	}
	// Value mismatch.
	if reason := compareQueryResult([]string{"1", "one", "2", "three"}, rec); reason == "" {
		t.Error("expected value-mismatch failure, got empty reason")
	}
	// Empty expected section means "any result OK".
	open := Record{TypeString: "I", ExpectedCount: 0}
	if reason := compareQueryResult([]string{"anything"}, open); reason != "" {
		t.Errorf("empty expected should accept any result, got: %s", reason)
	}
}
