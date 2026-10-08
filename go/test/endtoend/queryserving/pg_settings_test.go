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

// TestUpdatePgSettingsRejected checks that UPDATE pg_settings, which
// PostgreSQL applies like SET, is refused rather than changing a pooled
// backend that later clients borrow.
func TestUpdatePgSettingsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)
	connStr := shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable")

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, sql := range []string{
		"UPDATE pg_settings SET setting = 'off' WHERE name = 'synchronous_commit'",
		"UPDATE pg_settings SET setting = 'pg_temp, public' WHERE name = 'search_path'",
	} {
		_, err = conn.Exec(ctx, sql)
		pgErr := utils.RequirePgError(t, err, "0A000")
		assert.Contains(t, pgErr.Message, "UPDATE pg_settings is not supported")
	}

	other, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer other.Close(ctx)

	var syncCommit string
	require.NoError(t, other.QueryRow(ctx, "SELECT current_setting('synchronous_commit')").Scan(&syncCommit))
	assert.Equal(t, "on", syncCommit)
}
