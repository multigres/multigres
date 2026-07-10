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
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_ShowMultigresVersion verifies that `SHOW multigres_version`
// is answered locally by the multigateway (postgres has no such GUC) and
// returns the gateway's build identity over the wire in both the simple and
// extended query protocols.
func TestMultigateway_ShowMultigresVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigres_version test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping multigres_version test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 30*time.Second)
	require.NoError(t, db.PingContext(ctx), "failed to ping database - multigateway may not be ready")

	// Simple query protocol.
	t.Run("simple protocol", func(t *testing.T) {
		var version string
		err := db.QueryRowContext(ctx, "SHOW multigres_version").Scan(&version)
		require.NoError(t, err, "failed to execute SHOW multigres_version")
		assert.Contains(t, version, "Multigres", "version string should identify Multigres, got %q", version)
	})

	// Extended query protocol: a prepared statement drives Parse/Bind/Execute,
	// exercising the primitive's PortalStreamExecute path.
	t.Run("extended protocol", func(t *testing.T) {
		stmt, err := db.PrepareContext(ctx, "SHOW multigres_version")
		require.NoError(t, err, "failed to prepare SHOW multigres_version")
		defer stmt.Close()

		var version string
		err = stmt.QueryRowContext(ctx).Scan(&version)
		require.NoError(t, err, "failed to execute prepared SHOW multigres_version")
		assert.Contains(t, version, "Multigres", "version string should identify Multigres, got %q", version)
	})
}
