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

package planner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

// Tablegroup DDL parses but has no implementation yet: the gateway must
// reject it with feature_not_supported instead of forwarding it to postgres,
// where it would fail with a syntax error.
func TestAnalyzeStatementRejectsTableGroupDDL(t *testing.T) {
	rejected := []string{
		"CREATE TABLEGROUP commerce",
		"CREATE TABLEGROUP commerce SHARDS ('0-80', '80-inf')",
		"ALTER TABLEGROUP commerce RENAME TO shop",
		"ALTER TABLEGROUP commerce RESHARD ('0-inf') INTO ('0-80', '80-inf')",
		"DROP TABLEGROUP commerce CASCADE",
	}
	for _, sql := range rejected {
		for _, unsafeConn := range []bool{false, true} {
			t.Run(sql, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), unsafeConn, false)
				require.Error(t, err)
				var diag *mterrors.PgDiagnostic
				require.True(t, errors.As(err, &diag))
				assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
				assert.Contains(t, diag.Message, "tablegroup DDL")
			})
		}
	}

	// The new keywords stay ordinary identifiers in plain postgres DDL.
	allowed := []string{
		"CREATE TABLE t (tablegroup text, shards int, reshard int)",
		"ALTER TABLE t DROP shards",
		"DROP TABLE tablegroup",
	}
	for _, sql := range allowed {
		t.Run(sql, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false, false)
			require.NoError(t, err)
		})
	}
}
