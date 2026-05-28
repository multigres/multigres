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

package queryserving

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestKeywordFunctionNames verifies that functions whose names are SQL keywords
// (left, right, overlaps, current_schema, ...) can be called both unquoted and
// double-quoted, and that the two spellings return identical results. This guards
// the deparser's identifier quoting: it quotes keyword function names (matching
// PostgreSQL's quote_identifier, which quotes every non-unreserved keyword), so
// `left(...)` round-trips through multigateway as `"left"(...)`. A quoted keyword
// folds to the same lowercase name as the unquoted form, so both must resolve to
// the same built-in.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure the
// proxy behavior matches native PostgreSQL exactly.
func TestKeywordFunctionNames(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping keyword function name test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping keyword function name tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// Each case is a function whose name is a keyword. unquoted and quoted are two
	// spellings of the same call that must behave identically. The expression is
	// always cast to text so heterogeneous return types (text, bool, name) scan
	// uniformly. want is the expected text result; an empty want means the result
	// is environment dependent (e.g. current_schema) and we only assert the two
	// spellings agree.
	cases := []struct {
		name     string
		unquoted string
		quoted   string
		want     string
	}{
		{
			name:     "left",
			unquoted: "left('hello world', 5)",
			quoted:   `"left"('hello world', 5)`,
			want:     "hello",
		},
		{
			name:     "right",
			unquoted: "right('hello world', 5)",
			quoted:   `"right"('hello world', 5)`,
			want:     "world",
		},
		{
			name:     "overlaps",
			unquoted: "overlaps(DATE '2000-01-01', DATE '2000-02-01', DATE '2000-01-15', DATE '2000-03-01')",
			quoted:   `"overlaps"(DATE '2000-01-01', DATE '2000-02-01', DATE '2000-01-15', DATE '2000-03-01')`,
			want:     "true",
		},
		{
			name:     "current_schema",
			unquoted: "current_schema()",
			quoted:   `"current_schema"()`,
			// Environment dependent: assert only that both spellings agree.
			want: "",
		},
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var unquoted, quoted string
					err := conn.QueryRow(ctx, "SELECT ("+tc.unquoted+")::text").Scan(&unquoted)
					require.NoError(t, err, "unquoted call failed: %s", tc.unquoted)

					err = conn.QueryRow(ctx, "SELECT ("+tc.quoted+")::text").Scan(&quoted)
					require.NoError(t, err, "quoted call failed: %s", tc.quoted)

					assert.Equal(t, unquoted, quoted,
						"quoted and unquoted spellings returned different results")

					if tc.want != "" {
						assert.Equal(t, tc.want, unquoted, "unexpected result for %s", tc.name)
					}
				})
			}
		})
	}
}
