// Copyright 2026 Supabase, Inc.
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

package queryserving

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_MultigresVersion verifies both version surfaces:
//
//   - SHOW multigres.server_version   → short release version (like server_version),
//     answered locally by the gateway.
//   - SELECT multigres.version() → full build string (like version()), folded to
//     a literal before it reaches PostgreSQL, so it works in any expression
//     position and in both the simple and extended query protocols.
func TestMultigateway_MultigresVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigres.server_version test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping multigres.server_version test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 30*time.Second)
	require.NoError(t, db.PingContext(ctx), "failed to ping database - multigateway may not be ready")

	// simpleQuery runs sql via the simple query protocol; preparedQuery runs it
	// via the extended protocol (Parse/Bind/Execute).
	simpleQuery := func(t *testing.T, sql string) string {
		t.Helper()
		var out string
		require.NoError(t, db.QueryRowContext(ctx, sql).Scan(&out), "simple query %q failed", sql)
		return out
	}
	preparedQuery := func(t *testing.T, sql string) string {
		t.Helper()
		stmt, err := db.PrepareContext(ctx, sql)
		require.NoError(t, err, "failed to prepare %q", sql)
		defer stmt.Close()
		var out string
		require.NoError(t, stmt.QueryRowContext(ctx).Scan(&out), "prepared query %q failed", sql)
		return out
	}
	bothProtocols := map[string]func(*testing.T, string) string{"simple": simpleQuery, "extended": preparedQuery}

	t.Run("SHOW multigres.server_version returns the short version", func(t *testing.T) {
		for _, run := range bothProtocols {
			version := run(t, "SHOW multigres.server_version")
			assert.NotEmpty(t, version, "version should not be empty")
			assert.NotContains(t, version, "built with", "SHOW should return the short version, got %q", version)
		}
	})

	t.Run("SELECT multigres.version() returns the full build string", func(t *testing.T) {
		for _, run := range bothProtocols {
			version := run(t, "SELECT multigres.version()")
			assert.Contains(t, version, "Multigres", "function should return the full build string, got %q", version)
		}
	})

	// The function is folded to a literal before reaching PostgreSQL, so it works
	// inside a larger expression that PostgreSQL then evaluates — proving it is
	// not limited to a standalone target.
	t.Run("multigres.version() works inside an expression", func(t *testing.T) {
		for _, run := range bothProtocols {
			out := run(t, "SELECT length(multigres.version()) > 0")
			assert.Equal(t, "true", out, "expected the folded literal to be evaluated by PostgreSQL")
		}
	})

	// A bare version() must still route to PostgreSQL and report the backend
	// version — the gateway function is namespaced so it does not shadow this.
	t.Run("bare version() still reports PostgreSQL", func(t *testing.T) {
		version := simpleQuery(t, "SELECT version()")
		assert.True(t, strings.Contains(version, "PostgreSQL"), "bare version() should report PostgreSQL, got %q", version)
	})
}
