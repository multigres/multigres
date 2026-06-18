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

package multipooler

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	pgclient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestPGClientTLS_VerifyFull boots a shard where postgres is provisioned with
// TLS via pgctld --pg-initdb-extra-conf and the multipooler dials it over
// TCP with sslmode=verify-full. It then asserts that the connections
// multipooler holds in its admin and per-user pools are actually encrypted by
// inspecting pg_stat_ssl on the postgres side.
//
// This is the integration counterpart to the libpq-parity unit tests in
// pgprotocol/client/ssl_test.go and exercises the wiring added for MUL-370.
func TestPGClientTLS_VerifyFull(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerPGTLS(),
	)
	defer cleanup()

	require.NotNil(t, setup.MultipoolerPGTLSCertPaths, "expected PG TLS assets to be provisioned")

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "expected a primary multipooler")

	// Drive a query through the multipooler so it opens at least one regular
	// pool connection to postgres. This is the connection we want to confirm
	// went over TLS.
	mpClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err, "create multipooler client")
	defer mpClient.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err = mpClient.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	require.NoError(t, err, "SELECT 1 through multipooler")

	// Open a direct admin connection to postgres (also over TLS) to read
	// pg_stat_ssl. We trust the same CA the multipooler trusts and require
	// verify-full, so this connection itself proves the postgres TLS listener
	// is up before we even check pg_stat_ssl.
	dsn := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=verify-full sslrootcert=%s",
		primary.Pgctld.PgPort,
		shardsetup.TestPostgresPassword,
		setup.MultipoolerPGTLSCertPaths.CACertFile,
	)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open admin TLS connection")
	defer db.Close()
	require.NoError(t, db.PingContext(ctx), "ping postgres over TLS")

	// pg_stat_ssl has one row per active backend. At least one — the
	// multipooler connections we just exercised — must report ssl=true.
	var sslBackends int
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_stat_ssl
		WHERE ssl = true
		  AND pid <> pg_backend_pid()
	`).Scan(&sslBackends), "query pg_stat_ssl")

	require.Greater(t, sslBackends, 0, "expected at least one TLS-encrypted multipooler backend in pg_stat_ssl")
}

// TestPGClientTLS_DirectNegotiation validates the pgprotocol client's
// sslnegotiation=direct (PostgreSQL 17 TLS-first handshake, mandatory ALPN)
// against a REAL PostgreSQL 17 server — the same oracle PG's own
// 005_negotiate_encryption.pl uses for its direct-SSL rows. The connection
// is asserted encrypted via pg_stat_ssl from inside the session itself.
func TestPGClientTLS_DirectNegotiation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerPGTLS(),
	)
	defer cleanup()

	require.NotNil(t, setup.MultipoolerPGTLSCertPaths, "expected PG TLS assets to be provisioned")

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "expected a primary multipooler")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	tlsCfg, err := pgclient.BuildTLSConfig(pgclient.SSLModeVerifyFull,
		setup.MultipoolerPGTLSCertPaths.CACertFile, "localhost")
	require.NoError(t, err)

	conn, err := pgclient.Connect(ctx, ctx, &pgclient.Config{
		Host:           "localhost",
		Port:           primary.Pgctld.PgPort,
		User:           "postgres",
		Password:       shardsetup.TestPostgresPassword,
		Database:       "postgres",
		SSLMode:        pgclient.SSLModeVerifyFull,
		SSLNegotiation: pgclient.SSLNegotiationDirect,
		TLSConfig:      tlsCfg,
		DialTimeout:    10 * time.Second,
	})
	require.NoError(t, err, "direct TLS connect to real PostgreSQL must succeed")
	defer conn.Close()

	// The backend must report this very session as SSL-encrypted.
	results, err := conn.Query(ctx,
		"SELECT ssl::text FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
	require.NoError(t, err, "query over direct TLS must succeed")
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	require.Equal(t, "true", string(results[0].Rows[0].Values[0]),
		"pg_stat_ssl must report the direct-TLS session as encrypted")
}

// TestPGClientTLS_DirectNegotiation_Multipooler boots a shard whose
// multipoolers dial postgres with --pg-client-sslnegotiation=direct and
// asserts (a) queries flow end-to-end and (b) the pool connections are
// encrypted per pg_stat_ssl — the multipooler → postgres leg of query
// serving running entirely on PG 17 direct TLS.
func TestPGClientTLS_DirectNegotiation_Multipooler(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerPGTLSDirect(),
	)
	defer cleanup()

	require.NotNil(t, setup.MultipoolerPGTLSCertPaths, "expected PG TLS assets to be provisioned")

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "expected a primary multipooler")

	mpClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err, "create multipooler client")
	defer mpClient.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err = mpClient.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	require.NoError(t, err, "SELECT 1 through multipooler over direct TLS")

	// Confirm via a direct admin connection that the multipooler backends
	// are SSL-encrypted.
	dsn := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=verify-full sslrootcert=%s",
		primary.Pgctld.PgPort,
		shardsetup.TestPostgresPassword,
		setup.MultipoolerPGTLSCertPaths.CACertFile,
	)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open admin TLS connection")
	defer db.Close()
	require.NoError(t, db.PingContext(ctx), "ping postgres over TLS")

	var sslBackends int
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_stat_ssl
		WHERE ssl = true
		  AND pid <> pg_backend_pid()
	`).Scan(&sslBackends), "query pg_stat_ssl")

	require.Greater(t, sslBackends, 0,
		"expected at least one direct-TLS-encrypted multipooler backend in pg_stat_ssl")
}
