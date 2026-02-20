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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SSL_RequireMode tests that clients can connect to multigateway
// using sslmode=require, which establishes a TLS connection without certificate verification.
func TestMultiGateway_SSL_RequireMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=require connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err, "query over TLS with sslmode=require should succeed")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_SSL_VerifyCA tests that clients can connect using sslmode=verify-ca,
// which verifies the server certificate against the provided CA certificate.
func TestMultiGateway_SSL_VerifyCA(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)
	require.NotNil(t, setup.MultigatewayTLSCertPaths, "TLS cert paths should be set")

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=verify-ca sslrootcert=%s connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword, setup.MultigatewayTLSCertPaths.CACertFile)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err, "query over TLS with sslmode=verify-ca should succeed")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_SSL_VerifyFull tests that clients can connect using sslmode=verify-full,
// which verifies the server certificate against the CA and checks hostname matches the cert SAN.
func TestMultiGateway_SSL_VerifyFull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)
	require.NotNil(t, setup.MultigatewayTLSCertPaths, "TLS cert paths should be set")

	// verify-full checks that the server hostname matches the certificate SAN.
	// Our test certs have SAN=localhost, and we connect to localhost, so this should work.
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=verify-full sslrootcert=%s connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword, setup.MultigatewayTLSCertPaths.CACertFile)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err, "query over TLS with sslmode=verify-full should succeed")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_SSL_DisableStillWorks tests that clients can still connect
// with sslmode=disable when TLS is configured on the server.
// The server should accept both TLS and non-TLS connections.
func TestMultiGateway_SSL_DisableStillWorks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	// sslmode=disable means the client won't try SSL at all - sends StartupMessage directly.
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err, "query without SSL should still work when server has TLS configured")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_SSL_AuthOverTLS tests that SCRAM-SHA-256 authentication works
// correctly over a TLS connection.
func TestMultiGateway_SSL_AuthOverTLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	// First create a test user via the admin connection (using sslmode=require over TLS)
	adminConnStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=require connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	adminDB, err := sql.Open("postgres", adminConnStr)
	require.NoError(t, err)
	defer adminDB.Close()

	_, err = adminDB.Exec("CREATE USER ssl_testuser WITH PASSWORD 'ssl_password'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = adminDB.Exec("DROP USER IF EXISTS ssl_testuser")
	})

	// Connect as the test user over TLS
	userConnStr := fmt.Sprintf("host=localhost port=%d user=ssl_testuser password=ssl_password dbname=postgres sslmode=require connect_timeout=5",
		setup.MultigatewayPgPort)
	userDB, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err)
	defer userDB.Close()

	// Verify authentication works and we can execute queries
	var currentUser string
	err = userDB.QueryRow("SELECT current_user").Scan(&currentUser)
	require.NoError(t, err, "SCRAM authentication over TLS should succeed")
	assert.Equal(t, "ssl_testuser", currentUser)

	// Verify wrong password fails even over TLS
	badConnStr := fmt.Sprintf("host=localhost port=%d user=ssl_testuser password=wrong_password dbname=postgres sslmode=require connect_timeout=5",
		setup.MultigatewayPgPort)
	badDB, err := sql.Open("postgres", badConnStr)
	require.NoError(t, err)
	defer badDB.Close()

	err = badDB.Ping()
	require.Error(t, err, "wrong password should fail even over TLS")
	assert.Contains(t, err.Error(), "password authentication failed")
}

// TestMultiGateway_SSL_MultipleQueries tests that a TLS connection remains stable
// across multiple queries, verifying the TLS session is maintained correctly.
func TestMultiGateway_SSL_MultipleQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=require connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	// Execute multiple queries to verify the TLS connection is stable
	for i := 1; i <= 5; i++ {
		var result int
		err = db.QueryRow("SELECT $1::int", i).Scan(&result)
		require.NoError(t, err, "query %d over TLS should succeed", i)
		assert.Equal(t, i, result)
	}
}

// TestMultiGateway_SSL_PreferMode_WithTLS tests sslmode=prefer against a TLS-enabled
// multigateway. The client sends SSLRequest, the server responds 'S', and the connection
// upgrades to TLS. Uses pgx because lib/pq doesn't support sslmode=prefer.
func TestMultiGateway_SSL_PreferMode_WithTLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=prefer connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	ctx := utils.WithTimeout(t, 10*time.Second)
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "pgx connect with sslmode=prefer should succeed")
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "query with sslmode=prefer should succeed (server accepts SSL)")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_SSL_PreferMode_FallbackToPlaintext tests sslmode=prefer against a
// non-TLS multigateway. The client sends SSLRequest, the server responds 'N' (decline),
// and pgx falls back to a plaintext connection. This exercises the 'N' response path
// and the client's fallback-to-StartupMessage logic.
// Uses pgx because lib/pq doesn't support sslmode=prefer.
func TestMultiGateway_SSL_PreferMode_FallbackToPlaintext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	// Use the non-TLS shared setup — multigateway has no TLS config,
	// so it responds 'N' to SSLRequest.
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=prefer connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	ctx := utils.WithTimeout(t, 10*time.Second)
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "pgx connect with sslmode=prefer should succeed (fallback to plaintext)")
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "query with sslmode=prefer should succeed after SSL decline + plaintext fallback")
	assert.Equal(t, 1, result)
}
