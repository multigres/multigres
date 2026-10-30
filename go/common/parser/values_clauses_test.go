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

package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValuesClausesRoundTrip(t *testing.T) {
	for _, sql := range []string{
		"VALUES (2), (1) ORDER BY 1",
		"VALUES (2), (1) LIMIT 0",
		"VALUES (2), (1) LIMIT ALL",
		"VALUES (2), (1) OFFSET 1",
		"VALUES (2), (1) ORDER BY 1 LIMIT 1 OFFSET 1",
		"VALUES (2), (1), (1) ORDER BY 1 FETCH FIRST ROW WITH TIES",
		"VALUES (2), (1), (1) ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES",
		"VALUES ($1), ($2) ORDER BY 1 LIMIT $3 OFFSET $4",
		"WITH x AS (VALUES (1)) VALUES (2), (1) ORDER BY 1 LIMIT 0",
		"SELECT * FROM (VALUES (2), (1) ORDER BY 1 LIMIT 1) AS v",
		"DELETE FROM orders WHERE id IN (VALUES (1), (2) LIMIT 0)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := ParseSQL(sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			deparsed := stmts[0].SqlString()
			require.Equal(t, sql, deparsed)
			_, err = ParseSQL(deparsed)
			require.NoError(t, err)
		})
	}
}
