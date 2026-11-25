// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package backingconn checks backingconn.Conn against the libpq that multigres runs, in the
// tools that dial a connection: psql, pg_dump, and the postgres that runs CREATE SUBSCRIPTION.
//
// A connection is validated by our own parser, which must not disagree with libpq's: whatever
// we accept libpq must not refuse, every parameter libpq knows must have a class in params.go, and
// the string each client is dialed with must pin the parameters that the environment of the process
// could otherwise change to our harm (the secret and internal ones) and leave the operational ones
// to it. None of this is something a unit test can know without libpq, so these tests run strings
// through it.
//
// They need psql, pg_dump and the PostgreSQL server binaries of PostgreSQL 17, and run in the
// integration job of CI, which installs them. The libpq they are linked against is libpq 17 or a
// later one: pgdg's postgresql-client-17 loads the libpq5 of the newest major, and the tests
// assert nothing that differs between them. A machine without the tools, or with a libpq before
// 17, skips; CI (CI=true) fails instead, so the tests cannot silently stop running.
package backingconn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// closedPort is a port nothing listens on: libpq reports an invalid option or string before it
// connects, and "Connection refused" once it has found nothing wrong.
const closedPort = 1

const connectionRefused = "Connection refused"

// requireTools skips the test when it is run with -short or a tool is missing, and in CI fails it
// when a tool is missing.
func requireTools(t *testing.T, names ...string) {
	t.Helper()
	if testing.Short() {
		t.Skip("runs libpq tools; skipped with -short")
	}
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is not installed, and CI must run this test: %v", name, err)
			}
			t.Skipf("%s is not installed", name)
		}
	}
}

// requireLibpq17OrLater skips the test unless the libpq psql is linked against is libpq 17 or
// later, the oldest whose parameters the table classifies, and in CI fails it. It asks libpq
// itself, which psql --version cannot say: a distribution's postgresql-client-17 can load a newer
// libpq5 (pgdg's does). libpq 17 is the first that knows sslnegotiation.
func requireLibpq17OrLater(t *testing.T) {
	t.Helper()
	if knows(t, "sslnegotiation") {
		return
	}
	msg := fmt.Sprintf("the libpq that psql is linked against is older than libpq 17, whose parameters these tests assert (psql --version: %s)", psqlVersion(t))
	if os.Getenv("CI") != "" {
		t.Fatalf("%s, and CI must run this test", msg)
	}
	t.Skipf("SKIPPED, NOT CHECKED: %s", msg)
}

// knows is whether libpq knows a connection parameter: it reports an unknown one as an invalid
// connection option before it dials, whatever the value.
func knows(t *testing.T, parameter string) bool {
	t.Helper()
	return !strings.Contains(libpqSays(t, fmt.Sprintf("host=127.0.0.1 port=%d %s=x", closedPort, parameter)), "invalid connection option")
}

func psqlVersion(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("psql", "--version").Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// cleanEnv is the environment a libpq client is run in: nothing of PG*, no home directory.
func cleanEnv(extra ...string) []string {
	return append([]string{"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent", "LC_ALL=C"}, extra...)
}

// libpqSays is the first line psql prints for a string, which is what libpq said at connect time:
// an invalid string or option, or Connection refused. It is empty when psql connected.
func libpqSays(t *testing.T, dsn string, env ...string) string {
	t.Helper()
	cmd := exec.Command("psql", "-X", "-w", "-d", dsn, "-c", "")
	cmd.Env = cleanEnv(env...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return ""
	}
	line, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")
	return strings.TrimPrefix(line, "psql: error: ")
}

var (
	hostRef = regexp.MustCompile(`([=@,])h\d?([ ,:/?]|$)`)
	address = regexp.MustCompile(`\b\d+\.\d+\.\d+\.\d+\b`)
)

// toLoopback points a corpus string at the closed port of the machine: its hosts h, h1, h2 are
// 127.0.0.1 and its ports are closedPort, and any address is the loopback, so that no string
// connects anywhere.
func toLoopback(dsn string) string {
	for range 4 {
		dsn = hostRef.ReplaceAllString(dsn, "${1}127.0.0.1${2}")
	}
	dsn = strings.NewReplacer("5432", strconv.Itoa(closedPort), "5433", strconv.Itoa(closedPort)).Replace(dsn)
	return address.ReplaceAllString(dsn, "127.0.0.1")
}

type corpusEntry struct {
	DSN      string `json:"dsn"`
	Accepted bool   `json:"accepted"`
	Source   string `json:"source"`
	Why      string `json:"why"`
}

func readCorpus(t *testing.T) []corpusEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "common", "backingconn", "testdata", "corpus.json"))
	require.NoError(t, err)
	var corpus []corpusEntry
	require.NoError(t, json.Unmarshal(data, &corpus))
	return corpus
}

// withPassword gives a corpus string the password a connection requires.
func withPassword(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://u@") {
		return strings.Replace(dsn, "postgres://u@", "postgres://u:pw@", 1)
	}
	if strings.HasPrefix(dsn, "password=") || strings.Contains(dsn, " password=") {
		return dsn
	}
	return dsn + " password=pw"
}

// loopbackConn is a connection that points at the closed port of the machine, whatever it names.
func loopbackConn(t *testing.T, dsn string) *backingconn.Conn {
	t.Helper()
	conn, err := backingconn.FromDSN(withPassword(dsn))
	require.NoError(t, err, dsn)
	for i := range conn.Hosts {
		conn.Hosts[i] = "127.0.0.1"
	}
	for i := range conn.Ports {
		conn.Ports[i] = closedPort
	}
	if addrs, ok := conn.Params["hostaddr"]; ok {
		conn.Params["hostaddr"] = strings.Repeat("127.0.0.1,", strings.Count(addrs, ",")) + "127.0.0.1"
	}
	require.NoError(t, conn.Validate())
	return conn
}

// Every string the connection parser accepts, libpq does not refuse, and the string the libpq
// clients are dialed with for it is one libpq accepts: the pins are libpq's own parameters, with
// values it takes. The strings are the corpus the unit tests of the package read too, which has
// the strings of the review of the parser as they were written. The ones we refuse are not sent
// to libpq: that they are refused is a unit test of the package, which needs no tools.
func TestWhatWeAcceptLibpqDoesNotRefuse(t *testing.T) {
	requireTools(t, "psql")
	requireLibpq17OrLater(t)

	for _, e := range readCorpus(t) {
		if !e.Accepted {
			continue
		}
		t.Run(e.DSN, func(t *testing.T) {
			// A string without a port goes to the default port, where something may listen on the
			// machine: PGPORT keeps it at the closed one.
			assert.Contains(t, libpqSays(t, toLoopback(e.DSN), "PGPORT="+strconv.Itoa(closedPort)), connectionRefused,
				"libpq refuses a string we accept: %s", e.Why)
			conn := loopbackConn(t, e.DSN)
			assert.Contains(t, libpqSays(t, conn.LibpqString()), connectionRefused,
				"libpq refuses the string it is dialed with")
		})
	}
}

// libpqParameters is every connection parameter of libpq 17 and 18, so that a parameter either
// knows is one the table classifies. A parameter that a later libpq adds is not in it until it is
// added here and to the table.
var libpqParameters = []string{
	"application_name", "channel_binding", "client_encoding", "connect_timeout", "dbname", "fallback_application_name",
	"gssdelegation", "gssencmode", "gsslib", "host", "hostaddr", "keepalives", "keepalives_count", "keepalives_idle",
	"keepalives_interval", "krbsrvname", "load_balance_hosts", "max_protocol_version", "min_protocol_version",
	"oauth_client_id", "oauth_client_secret", "oauth_issuer", "oauth_scope", "options", "passfile", "password", "port",
	"replication", "require_auth", "requirepeer", "requiressl", "scram_client_key", "scram_server_key", "service",
	"servicefile", "ssl_max_protocol_version", "ssl_min_protocol_version", "sslcert", "sslcertmode", "sslcompression",
	"sslcrl", "sslcrldir", "sslkey", "sslkeylogfile", "sslmode", "sslnegotiation", "sslpassword", "sslrootcert", "sslsni",
	"target_session_attrs", "tcp_user_timeout", "user",
}

// Every parameter the linked libpq knows has a class in the table, so that a parameter of a libpq
// that the table has not seen is not dialed unclassified. libpq lists its parameters only to a C
// program (PQconndefaults), which CI does not have a compiler for; psql reports one it does not
// know as an invalid option, so each name of libpqParameters is asked of the libpq that is linked.
func TestEveryParameterLibpqKnowsHasAClass(t *testing.T) {
	requireTools(t, "psql")
	requireLibpq17OrLater(t)
	require.False(t, knows(t, "nosuchparameter"), "premise: libpq refuses a parameter it does not know")

	for _, name := range libpqParameters {
		if knows(t, name) {
			assert.True(t, backingconn.IsParam(name), "libpq knows %s, which has no class in params.go", name)
		}
	}
}

// ignoredEnvironment is every variable libpq reads for a parameter that a string decides or pins
// (a carried, secret or internal one, or the location of a service), set to a value that changes
// what is dialed, or fails it, if it is read.
var ignoredEnvironment = []string{
	"PGHOST=env.invalid", "PGHOSTADDR=203.0.113.9", "PGPORT=bogus", "PGDATABASE=envdb", "PGUSER=envuser",
	"PGPASSWORD=envpw", "PGPASSFILE=/nonexistent/passfile", "PGREQUIREAUTH=bogus", "PGCHANNELBINDING=bogus",
	"PGSERVICEFILE=/nonexistent/services", "PGOPTIONS=-c bogus=1", "PGSSLMODE=bogus", "PGSSLCERT=/nonexistent/c",
	"PGSSLKEY=/nonexistent/k", "PGSSLROOTCERT=/nonexistent/r", "PGSSLCRL=/nonexistent/crl", "PGSSLCRLDIR=/nonexistent/d",
	"PGSSLMINPROTOCOLVERSION=bogus", "PGSSLMAXPROTOCOLVERSION=bogus", "PGGSSENCMODE=bogus", "PGKRBSRVNAME=x",
	"PGGSSLIB=x", "PGGSSDELEGATION=bogus", "PGSSLCERTMODE=bogus", "PGSSLNEGOTIATION=bogus", "PGSYSCONFDIR=/nonexistent",
}

// operationalEnvironment is the variables libpq reads for the operational parameters that it
// validates when it connects, each set to a value it refuses. A string that does not set the
// parameter leaves it to the environment, so libpq says what is wrong with it.
var operationalEnvironment = []string{"PGCONNECT_TIMEOUT=bogus", "PGTARGETSESSIONATTRS=bogus", "PGLOADBALANCEHOSTS=bogus"}

// The string a libpq client is dialed with ignores the variables of the parameters it pins or
// carries: with each set to something hostile, one at a time, the client still goes to the server
// the connection names, and its string is not refused for a value of the environment. The
// operational variables are read, unless the connection sets the parameter.
func TestTheStringLibpqIsDialedWithIgnoresTheEnvironmentWhereItPins(t *testing.T) {
	requireTools(t, "psql")
	requireLibpq17OrLater(t)
	conn := loopbackConn(t, "host=h dbname=app user=u")
	pinned, unpinned := conn.LibpqString(), conn.DSN()
	require.Contains(t, libpqSays(t, pinned), connectionRefused)

	for _, hostile := range ignoredEnvironment {
		name, _, _ := strings.Cut(hostile, "=")
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, libpqSays(t, pinned, hostile), connectionRefused, "the pinned string is read as it says")
		})
	}

	// The premise: the string without the pins does take them from the environment. (The port and
	// the sslmode are always in the string, so the environment cannot change them.)
	for _, hostile := range []string{"PGCHANNELBINDING=bogus", "PGREQUIREAUTH=bogus"} {
		assert.NotContains(t, libpqSays(t, unpinned, hostile), connectionRefused, hostile)
	}

	// An operational parameter is the environment's to set, and the connection's own value wins.
	own := loopbackConn(t, "host=h dbname=app user=u connect_timeout=5 target_session_attrs=any load_balance_hosts=disable")
	for _, hostile := range operationalEnvironment {
		name, _, _ := strings.Cut(hostile, "=")
		t.Run(name, func(t *testing.T) {
			assert.NotContains(t, libpqSays(t, pinned, hostile), connectionRefused, "the environment is read")
			assert.Contains(t, libpqSays(t, own.LibpqString(), hostile), connectionRefused, "the connection's value wins")
		})
	}
}

// A service cannot be pinned, and need not be: it only fills in what the string leaves out, which
// is the operational parameters and nothing that is pinned. A service that does not exist fails the
// connection, which is as closed as it gets.
func TestAServiceInTheEnvironmentFillsInOnlyTheOperationalParameters(t *testing.T) {
	requireTools(t, "psql")
	requireLibpq17OrLater(t)
	services := filepath.Join(t.TempDir(), "pg_service.conf")
	require.NoError(t, os.WriteFile(services, []byte(`[evil]
host=203.0.113.9
port=6543
dbname=evildb
user=evil
password=evilpw
sslmode=disable
hostaddr=198.51.100.7
connect_timeout=1
options=-c evil=1
replication=database

[operational]
target_session_attrs=bogus
`), 0o600))
	conn := loopbackConn(t, "host=h dbname=app user=u")

	assert.Contains(t, libpqSays(t, conn.LibpqString(), "PGSERVICE=evil", "PGSERVICEFILE="+services), connectionRefused,
		"the service fills in nothing that is pinned")
	assert.NotContains(t, libpqSays(t, conn.DSN(), "PGSERVICE=evil", "PGSERVICEFILE="+services), "127.0.0.1",
		"premise: without the pins it fills in the address")
	assert.Contains(t, libpqSays(t, conn.LibpqString(), "PGSERVICE=operational", "PGSERVICEFILE="+services), "target_session_attrs",
		"the service fills in an operational parameter")
	assert.Contains(t, libpqSays(t, conn.LibpqString(), "PGSERVICE=nosuchservice"), "definition of service",
		"a service that is not there fails the connection")
}

// startPostgres runs a throwaway PostgreSQL 17 with logical replication, authenticating with a
// password over TCP, in the environment given, and returns its port.
func startPostgres(t *testing.T, password string, env ...string) int {
	t.Helper()
	return startPostgresAuth(t, "scram-sha-256", password, env...)
}

// startPostgresAuth is startPostgres with the authentication method of the TCP connections in
// pg_hba.conf, such as scram-sha-256, md5, password or trust.
func startPostgresAuth(t *testing.T, method, password string, env ...string) int {
	t.Helper()
	port := utils.GetFreePort(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	pwFile := filepath.Join(root, "pwfile")
	require.NoError(t, os.WriteFile(pwFile, []byte(password), 0o600))

	initdb := executil.Command(t.Context(), "initdb", "-D", dataDir, "-U", "postgres", "--pwfile="+pwFile,
		"--encoding=UTF8", "--locale=C", "--auth-local=trust", "--auth-host=scram-sha-256")
	initdb.SetEnv(utils.BaseTestEnv())
	if out, err := initdb.CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	hba := "local all all trust\nhost all all 127.0.0.1/32 " + method + "\nhost replication all 127.0.0.1/32 " + method + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "pg_hba.conf"), []byte(hba), 0o600))

	logFile, err := os.Create(filepath.Join(root, "postgres.log"))
	require.NoError(t, err)
	pg := executil.Command(context.Background(), "postgres", "-D", dataDir, "-p", strconv.Itoa(port),
		"-c", "listen_addresses=127.0.0.1", "-c", "unix_socket_directories=", "-c", "wal_level=logical",
		"-c", "max_wal_senders=10", "-c", "max_replication_slots=10", "-c", "fsync=off").WithProcessGroup()
	pg.SetEnv(append(utils.BaseTestEnv(), "LC_ALL=en_US.UTF-8"))
	pg.AddEnv(env...)
	pg.SetStdout(logFile)
	pg.SetStderr(logFile)
	require.NoError(t, pg.Start())
	t.Cleanup(func() {
		_, _ = pg.Stop(context.Background())
		_ = logFile.Close()
		if t.Failed() {
			if data, err := os.ReadFile(logFile.Name()); err == nil {
				t.Logf("postgres log:\n%s", data)
			}
		}
	})
	require.Eventually(t, func() bool {
		return executil.Command(t.Context(), "pg_isready", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-q").Run() == nil
	}, 30*time.Second, 200*time.Millisecond, "postgres must become ready")
	return port
}

// psql runs a statement against a server and returns its output.
func psql(t *testing.T, port int, statement string) (string, error) {
	t.Helper()
	cmd := executil.Command(t.Context(), "psql", "-X", "-w", "-At", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-c", statement)
	cmd.SetEnv(cleanEnv("PGPASSWORD=pw"))
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// require_auth is pinned to the methods a source may use (password, md5, scram-sha-256, none), and
// connections to a server that uses each of them work, for the libpq client and for pgx.
func TestTheStringsConnectWhicheverMethodTheServerAuthenticatesWith(t *testing.T) {
	requireTools(t, "psql", "initdb", "postgres", "pg_isready")
	requireLibpq17OrLater(t)
	for _, method := range []string{"scram-sha-256", "md5", "password", "trust"} {
		t.Run(method, func(t *testing.T) {
			port := startPostgresAuth(t, method, "pw")
			conn, err := backingconn.FromDSN(fmt.Sprintf("host=127.0.0.1 port=%d dbname=postgres user=postgres password=pw sslmode=disable", port))
			require.NoError(t, err)

			cmd := executil.Command(t.Context(), "psql", "-X", "-w", "-At", "-d", conn.LibpqString(), "-c", "SELECT 'connected'")
			cmd.SetEnv(cleanEnv())
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			assert.Equal(t, "connected", strings.TrimSpace(string(out)))

			cfg, err := pgx.ParseConfig(conn.LibpqString())
			require.NoError(t, err)
			// pgx sends the server the keys it does not parse as settings, which a server
			// refuses; the migrator drops the parameters of the table from them (dialConfig).
			for key := range cfg.RuntimeParams {
				if key != "application_name" && backingconn.IsParam(key) {
					delete(cfg.RuntimeParams, key)
				}
			}
			pg, err := pgx.ConnectConfig(t.Context(), cfg)
			require.NoError(t, err)
			defer pg.Close(t.Context())
			var one int
			require.NoError(t, pg.QueryRow(t.Context(), "SELECT 1").Scan(&one))
		})
	}
}

// The strings against real servers, with the process of a postgres in a hostile environment, which
// is what pgctld hands the postgres that runs CREATE SUBSCRIPTION: psql and pg_dump connect with
// the string, and a subscription made with it copies and replicates, where the same connection
// rendered without the pins is refused by the environment it inherits. An operational variable is
// honored.
func TestTheLibpqStringWorksAgainstAServerFromAHostileEnvironment(t *testing.T) {
	requireTools(t, "psql", "pg_dump", "initdb", "postgres", "pg_isready")
	requireLibpq17OrLater(t)
	publisher := startPostgres(t, "pw")
	_, err := psql(t, publisher, "CREATE TABLE t (id int PRIMARY KEY, v text); INSERT INTO t VALUES (1, 'a'); CREATE PUBLICATION pub FOR TABLE t")
	require.NoError(t, err)

	conn, err := backingconn.FromDSN(fmt.Sprintf("host=127.0.0.1 port=%d dbname=postgres user=postgres password=pw sslmode=disable", publisher))
	require.NoError(t, err)

	t.Run("psql", func(t *testing.T) {
		// PGREQUIRESSL=1 asks for TLS, which this server does not speak: the sslmode of the string
		// wins over it.
		cmd := executil.Command(t.Context(), "psql", "-X", "-w", "-At", "-d", conn.LibpqString(), "-c", "SELECT 'connected'")
		cmd.SetEnv(cleanEnv(append([]string{"PGREQUIRESSL=1"}, ignoredEnvironment...)...))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		assert.Equal(t, "connected", strings.TrimSpace(string(out)))
	})

	t.Run("psql takes an operational parameter from the environment", func(t *testing.T) {
		cmd := executil.Command(t.Context(), "psql", "-X", "-w", "-At", "-d", conn.LibpqString(), "-c", "SHOW application_name")
		cmd.SetEnv(cleanEnv(append([]string{"PGAPPNAME=from-the-environment"}, ignoredEnvironment...)...))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		assert.Equal(t, "from-the-environment", strings.TrimSpace(string(out)))

		withName := conn.Clone()
		withName.Params = map[string]string{"application_name": "from-the-connection"}
		cmd = executil.Command(t.Context(), "psql", "-X", "-w", "-At", "-d", withName.LibpqString(), "-c", "SHOW application_name")
		cmd.SetEnv(cleanEnv("PGAPPNAME=from-the-environment"))
		out, err = cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		assert.Equal(t, "from-the-connection", strings.TrimSpace(string(out)), "the connection's value wins")
	})

	t.Run("pg_dump", func(t *testing.T) {
		noPassword := conn.Clone()
		noPassword.Password = ""
		cmd := executil.Command(t.Context(), "pg_dump", "--schema-only", "-d", noPassword.LibpqString())
		cmd.SetEnv(cleanEnv("PGPASSWORD=pw"))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		assert.Contains(t, string(out), "CREATE TABLE public.t")
	})

	// The postgres that dials is the one with the hostile environment: what pgctld's has is the
	// environment of whoever configured the pod. Its operational variables are the ones that the
	// string leaves to it, so they hold nothing hostile.
	hostile := []string{
		"PGSSLMODE=bogus", "PGPORT=1", "PGHOST=nowhere.invalid", "PGUSER=nobody",
		"PGDATABASE=nodb", "PGPASSWORD=wrong", "PGOPTIONS=-c bogus=1", "PGSSLROOTCERT=/nonexistent/root",
		"PGCHANNELBINDING=bogus", "PGGSSENCMODE=bogus", "PGREQUIREAUTH=bogus", "PGSSLCERTMODE=bogus",
		"PGSSLNEGOTIATION=bogus", "PGPASSFILE=/nonexistent/pgpass",
	}
	subscriber := startPostgres(t, "pw", hostile...)
	_, err = psql(t, subscriber, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	require.NoError(t, err)
	quote := func(s string) string { return strings.ReplaceAll(s, "'", "''") }

	t.Run("the subscription of a string that leaves it to the environment is refused by it", func(t *testing.T) {
		out, err := psql(t, subscriber, "CREATE SUBSCRIPTION unpinned CONNECTION '"+quote(conn.DSN())+"' PUBLICATION pub")
		require.Error(t, err, "premise: the environment of the server changes what it dials")
		assert.NotContains(t, out, "created replication slot")
	})

	t.Run("the subscription of the pinned string copies and replicates", func(t *testing.T) {
		out, err := psql(t, subscriber, "CREATE SUBSCRIPTION pinned CONNECTION '"+quote(conn.LibpqString())+"' PUBLICATION pub")
		require.NoError(t, err, out)
		require.Eventually(t, func() bool {
			n, err := psql(t, subscriber, "SELECT count(*) FROM t")
			return err == nil && n == "1"
		}, 30*time.Second, 200*time.Millisecond, "the initial copy")
		_, err = psql(t, publisher, "INSERT INTO t VALUES (2, 'b')")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			n, err := psql(t, subscriber, "SELECT count(*) FROM t")
			return err == nil && n == "2"
		}, 30*time.Second, 200*time.Millisecond, "the replicated row")
	})
}
