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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
)

func TestReadOnlyOverride(t *testing.T) {
	tests := []struct {
		sql      string
		override bool
	}{
		// Overrides the read-only default.
		{"SET transaction_read_only = off", true},
		{"SET transaction_read_only = false", true},
		{"SET TRANSACTION_READ_ONLY TO 'no'", true},
		{"SET SESSION transaction_read_only = 0", true},
		{"SET default_transaction_read_only = off", true},
		{"SET LOCAL default_transaction_read_only = off", true},
		{"RESET transaction_read_only", true},
		{"RESET default_transaction_read_only", true},
		{"SET default_transaction_read_only TO DEFAULT", true},
		{"SET TRANSACTION READ WRITE", true},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE", true},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE", true},
		{"BEGIN READ WRITE", true},
		{"BEGIN ISOLATION LEVEL REPEATABLE READ, READ WRITE", true},
		{"START TRANSACTION READ WRITE", true},

		// Keeps or does not touch the read-only default.
		{"SET transaction_read_only = on", false},
		{"SET default_transaction_read_only = 'yes'", false},
		{"SET TRANSACTION READ ONLY", false},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", false},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", false},
		{"BEGIN", false},
		{"BEGIN READ ONLY", false},
		{"BEGIN ISOLATION LEVEL SERIALIZABLE", false},
		{"COMMIT", false},
		{"SET search_path = public", false},
		{"RESET ALL", false},
		{"INSERT INTO t VALUES (1)", false},
		{"SELECT set_config('transaction_read_only', 'off', false)", false},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, err := parser.ParseSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Equal(t, tc.override, ReadOnlyOverride(stmts[0]))
		})
	}
}

func TestNonAtomicProcedure(t *testing.T) {
	for sql, want := range map[string]bool{
		"CALL p()":                      true,
		"DO $$ BEGIN PERFORM 1; END $$": true,
		"SELECT f()":                    false,
		"EXECUTE s":                     false,
		"BEGIN":                         false,
		"INSERT INTO t VALUES (1)":      false,
	} {
		stmts, err := parser.ParseSQL(sql)
		require.NoError(t, err, sql)
		require.Len(t, stmts, 1, sql)
		require.Equal(t, want, NonAtomicProcedure(stmts[0]), sql)
	}
}
