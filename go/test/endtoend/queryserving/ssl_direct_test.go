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

// Direct TLS (PostgreSQL 17 sslnegotiation=direct) end-to-end tests against
// multigateway.
//
// The accept/reject matrix is logically imported from PostgreSQL's own
// negotiation test suite, src/test/libpq/t/005_negotiate_encryption.pl
// (sslnegotiation=direct rows):
//
//	server ssl=on,  sslmode=require sslnegotiation=direct -> connect (TLS)
//	server ssl=off, sslmode=require sslnegotiation=direct -> fail
//	weak sslmode + sslnegotiation=direct                  -> client-side error
//
// plus the ALPN requirement from PG 17's backend_startup.c. The positive
// paths are driven through real psql/libpq (the same client PG's TAP tests
// use), so the gateway's direct-TLS implementation is validated against the
// canonical client implementation rather than a re-implementation.

package queryserving

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgclient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// skipIfNoDirectTLSPsql skips the test unless a psql with libpq >= 17 is on
// PATH (sslnegotiation was added in PostgreSQL 17).
func skipIfNoDirectTLSPsql(t *testing.T) {
	t.Helper()
	out, err := exec.Command("psql", "--version").CombinedOutput()
	if err != nil {
		t.Skipf("psql not available: %v", err)
	}
	// "psql (PostgreSQL) 17.7 (Homebrew)" -> 17
	m := regexp.MustCompile(`(\d+)(?:\.\d+)?`).FindStringSubmatch(string(out))
	if m == nil {
		t.Skipf("cannot parse psql version from %q", string(out))
	}
	major, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	if major < 17 {
		t.Skipf("psql major version %d < 17; sslnegotiation=direct unsupported", major)
	}
}

// runPsql executes one psql command against the given conninfo and returns
// combined output and the run error.
func runPsql(t *testing.T, conninfo, query string) (string, error) {
	t.Helper()
	cmd := exec.Command("psql", conninfo, "-X", "-At", "-c", query)
	out, err := cmd.CombinedOutput()
	t.Logf("psql %q -c %q => err=%v output=%s", conninfo, query, err, string(out))
	return string(out), err
}

// TestMultiGateway_SSL_DirectNegotiation_Psql connects real libpq with
// sslnegotiation=direct to a TLS-enabled multigateway and runs a query.
// This is the Envoy-handles-SSLRequest model's client-visible behavior:
// the first bytes on the wire are a TLS ClientHello, no SSLRequest.
func TestMultiGateway_SSL_DirectNegotiation_Psql(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=require", "sslnegotiation=direct", "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT 41 + 1")
	require.NoError(t, err, "psql with sslnegotiation=direct should succeed: %s", out)
	assert.Equal(t, "42", strings.TrimSpace(out))
}

// TestMultiGateway_SSL_DirectNegotiation_VerifyCA pairs direct negotiation
// with certificate verification (sslmode=verify-ca), matching the stricter
// rows of PG's negotiation matrix.
func TestMultiGateway_SSL_DirectNegotiation_VerifyCA(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)
	require.NotNil(t, setup.MultigatewayTLSCertPaths, "TLS cert paths should be set")

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=verify-ca", "sslnegotiation=direct",
		"sslrootcert="+setup.MultigatewayTLSCertPaths.CACertFile, "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT current_user")
	require.NoError(t, err, "psql direct + verify-ca should succeed: %s", out)
	assert.Equal(t, shardsetup.DefaultTestUser, strings.TrimSpace(out))
}

// TestMultiGateway_SSL_DirectNegotiation_RequireSSLGateway verifies direct
// TLS satisfies the --pg-require-ssl=true posture (the client negotiated
// TLS before its StartupMessage, just not via SSLRequest).
func TestMultiGateway_SSL_DirectNegotiation_RequireSSLGateway(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getRequireSSLSharedSetup(t)
	setup.SetupTest(t)

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=require", "sslnegotiation=direct", "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT 1")
	require.NoError(t, err, "direct TLS must satisfy --pg-require-ssl=true: %s", out)
	assert.Equal(t, "1", strings.TrimSpace(out))
}

// TestMultiGateway_SSL_DirectNegotiation_NoTLSGateway_Fails: a gateway
// without TLS configured must reject the TLS-first connection (silently
// closed, libpq reports a connection failure). Mirrors the server-ssl=off /
// sslnegotiation=direct row of 005_negotiate_encryption.pl.
func TestMultiGateway_SSL_DirectNegotiation_NoTLSGateway_Fails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getSharedSetup(t) // no TLS on the gateway
	setup.SetupTest(t)

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=require", "sslnegotiation=direct", "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT 1")
	require.Error(t, err, "direct TLS against a non-TLS gateway must fail, got: %s", out)
}

// TestMultiGateway_SSL_DirectNegotiation_WeakSSLMode_ClientError: libpq
// itself rejects weak sslmode values combined with sslnegotiation=direct
// before connecting. Documents the client-side contract our pgprotocol
// client mirrors.
func TestMultiGateway_SSL_DirectNegotiation_WeakSSLMode_ClientError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=prefer", "sslnegotiation=direct", "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT 1")
	require.Error(t, err, "libpq must reject sslmode=prefer with sslnegotiation=direct, got: %s", out)
	assert.Contains(t, out, "sslnegotiation")
}

// TestMultiGateway_SSL_DirectNegotiation_GoClient drives a query through
// the full path (pgprotocol client -> multigateway -> multipooler ->
// postgres) over a direct TLS session, proving query serving works on the
// TLS-first channel, not just connection admission.
func TestMultiGateway_SSL_DirectNegotiation_GoClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	clientTLS, err := pgclient.BuildTLSConfig(pgclient.SSLModeRequire, "", "")
	require.NoError(t, err)

	ctx := utils.WithTimeout(t, 30*time.Second)
	conn, err := pgclient.Connect(ctx, ctx, &pgclient.Config{
		Host:           "localhost",
		Port:           setup.MultigatewayPgPort,
		User:           shardsetup.DefaultTestUser,
		Password:       shardsetup.TestPostgresPassword,
		Database:       "postgres",
		SSLMode:        pgclient.SSLModeRequire,
		SSLNegotiation: pgclient.SSLNegotiationDirect,
		TLSConfig:      clientTLS,
		DialTimeout:    10 * time.Second,
	})
	require.NoError(t, err, "direct TLS connect to multigateway must succeed")
	defer conn.Close()

	results, err := conn.Query(ctx, "SELECT 6 * 7")
	require.NoError(t, err, "query over direct TLS must succeed")
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	assert.Equal(t, "42", string(results[0].Rows[0].Values[0]))
}

// TestMultiGateway_SSL_DirectNegotiation_NoALPN_Rejected: a TLS-first
// client that completes the handshake without offering ALPN must receive
// PostgreSQL's exact FATAL diagnostic (SQLSTATE 08P01) through the tunnel
// and then lose the connection. Mirrors PG 17's mandatory-ALPN rule for
// direct connections.
func TestMultiGateway_SSL_DirectNegotiation_NoALPN_Rejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	raw, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", setup.MultigatewayPgPort), 5*time.Second)
	require.NoError(t, err)
	defer raw.Close()

	tlsConn := tls.Client(raw, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test asserts rejection, not cert validity
		MinVersion:         tls.VersionTLS12,
		// No ALPN offered.
	})
	require.NoError(t, tlsConn.Handshake(), "TLS handshake itself should succeed without ALPN")
	require.NoError(t, tlsConn.SetReadDeadline(time.Now().Add(10*time.Second)))

	// Expect an ErrorResponse ('E') frame carrying FATAL / 08P01.
	header := make([]byte, 5)
	_, err = io.ReadFull(tlsConn, header)
	require.NoError(t, err)
	require.Equal(t, byte(protocol.MsgErrorResponse), header[0])
	bodyLen := int(binary.BigEndian.Uint32(header[1:5])) - 4
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(tlsConn, body)
	require.NoError(t, err)

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "FATAL")
	assert.Contains(t, bodyStr, "08P01")
	assert.Contains(t, bodyStr, "ALPN protocol negotiation extension")

	// Server closes after the FATAL.
	one := make([]byte, 1)
	_, readErr := tlsConn.Read(one)
	require.Error(t, readErr, "server must close the connection after the ALPN rejection")
}

// TestMultiGateway_SSL_NegotiatedStillWorks_Regression guards the classic
// SSLRequest path against regressions from the direct-TLS first-byte peek:
// libpq's default sslnegotiation=postgres must behave exactly as before.
func TestMultiGateway_SSL_NegotiatedStillWorks_Regression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSL test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SSL tests")
	}
	skipIfNoDirectTLSPsql(t)

	setup := getTLSSharedSetup(t)
	setup.SetupTest(t)

	conninfo := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=require", "sslnegotiation=postgres", "connect_timeout=5")
	out, err := runPsql(t, conninfo, "SELECT 1")
	require.NoError(t, err, "sslnegotiation=postgres must keep working: %s", out)
	assert.Equal(t, "1", strings.TrimSpace(out))
}
