// Copyright 2025 Supabase, Inc.
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
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestMultiGateway_Authentication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping authentication test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping authentication tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Connect as postgres (superuser) to create test users
	adminConnStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	adminDB, err := sql.Open("postgres", adminConnStr)
	require.NoError(t, err)
	defer adminDB.Close()

	// Create test users with different passwords
	_, err = adminDB.Exec("CREATE USER testuser1 WITH PASSWORD 'password1'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = adminDB.Exec("DROP USER IF EXISTS testuser1")
	})

	_, err = adminDB.Exec("CREATE USER testuser2 WITH PASSWORD 'password2'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = adminDB.Exec("DROP USER IF EXISTS testuser2")
	})

	t.Run("correct password authentication", func(t *testing.T) {
		connStr := fmt.Sprintf("host=localhost port=%d user=testuser1 password=password1 dbname=postgres sslmode=disable connect_timeout=5",
			setup.MultigatewayPgPort)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		// Should be able to connect and execute queries
		var result int
		err = db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 1, result)
	})

	t.Run("wrong password authentication fails", func(t *testing.T) {
		connStr := fmt.Sprintf("host=localhost port=%d user=testuser1 password=wrongpassword dbname=postgres sslmode=disable connect_timeout=5",
			setup.MultigatewayPgPort)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		// Connection should fail during authentication
		err = db.Ping()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "password authentication failed")
	})

	t.Run("nonexistent user authentication fails", func(t *testing.T) {
		connStr := fmt.Sprintf("host=localhost port=%d user=nonexistent password=anypassword dbname=postgres sslmode=disable connect_timeout=5",
			setup.MultigatewayPgPort)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		// Connection should fail for nonexistent user
		err = db.Ping()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "authentication failed")
	})

	t.Run("multiple users with separate connection pools", func(t *testing.T) {
		// Connect as testuser1
		connStr1 := fmt.Sprintf("host=localhost port=%d user=testuser1 password=password1 dbname=postgres sslmode=disable connect_timeout=5",
			setup.MultigatewayPgPort)
		db1, err := sql.Open("postgres", connStr1)
		require.NoError(t, err)
		defer db1.Close()

		// Connect as testuser2
		connStr2 := fmt.Sprintf("host=localhost port=%d user=testuser2 password=password2 dbname=postgres sslmode=disable connect_timeout=5",
			setup.MultigatewayPgPort)
		db2, err := sql.Open("postgres", connStr2)
		require.NoError(t, err)
		defer db2.Close()

		// Both should be able to connect
		require.NoError(t, db1.Ping())
		require.NoError(t, db2.Ping())

		// Verify each connection sees the correct username
		var user1 string
		err = db1.QueryRow("SELECT current_user").Scan(&user1)
		require.NoError(t, err)
		assert.Equal(t, "testuser1", user1)

		var user2 string
		err = db2.QueryRow("SELECT current_user").Scan(&user2)
		require.NoError(t, err)
		assert.Equal(t, "testuser2", user2)

		// Query multiple times to ensure consistent results
		err = db1.QueryRow("SELECT current_user").Scan(&user1)
		require.NoError(t, err)
		assert.Equal(t, "testuser1", user1, "user1 should consistently see their own username")

		err = db2.QueryRow("SELECT current_user").Scan(&user2)
		require.NoError(t, err)
		assert.Equal(t, "testuser2", user2, "user2 should consistently see their own username")
	})
}
