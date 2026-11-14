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

package migration

import (
	"cmp"
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn"
)

// wildcardPasswordFile writes a password file like the one pgctld leaves in a pooler's directory,
// whose entry matches every host, and returns its path. HOME is pointed at a directory whose
// .pgpass holds the same entry, so that the default password file is a trap too.
func wildcardPasswordFile(t *testing.T, password string) string {
	t.Helper()
	dir := t.TempDir()
	entry := []byte("*:*:*:*:" + password + "\n")
	path := filepath.Join(dir, "pgbackrest.pgpass")
	require.NoError(t, os.WriteFile(path, entry, 0o600))
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(home, ".pgpass"), entry, 0o600))
	t.Setenv("HOME", home)
	return path
}

// dialDSN is the config a source is dialed with, for a string read as a connection is when it
// is created.
func dialDSN(dsn string) (*pgx.ConnConfig, error) {
	conn, err := backingconn.FromDSN(dsn)
	if err != nil {
		return nil, err
	}
	return dialConfig(conn)
}

// hostileEnvironment is every variable pgx reads for a parameter that the connection decides or
// that is pinned (carried, secret and internal parameters, and a service file), set to a value that
// would change what is dialed, or fail it, if it were read. The operational variables are the
// process's to set (TestDialConfigHonorsTheOperationalEnvironment).
var hostileEnvironment = map[string]string{
	"PGHOST": "env.invalid", "PGPORT": "1", "PGDATABASE": "envdb", "PGUSER": "envuser",
	"PGPASSWORD": "envpw", "PGPASSFILE": "/nonexistent/passfile", "PGSSLMODE": "disable",
	"PGSSLKEY": "/nonexistent/key", "PGSSLCERT": "/nonexistent/cert", "PGSSLROOTCERT": "/nonexistent/root",
	"PGSSLPASSWORD": "envsslpw", "PGSSLNEGOTIATION": "direct", "PGSERVICEFILE": "/nonexistent/services",
	"PGOPTIONS": "-c search_path=evil", "PGCHANNELBINDING": "require", "PGREQUIREAUTH": "gss",
}

// described is what of a config says what is dialed and how.
type described struct {
	Host, User, Database, Password string
	Port                           uint16
	RuntimeParams                  map[string]string
	ConnectTimeout                 time.Duration
	TLS                            string
	Fallbacks                      []string
	SSLNegotiation                 string
	ChannelBinding                 string
	MinProtocol, MaxProtocol       string
	RequireAuth                    string
	ValidateConnectSet             bool
}

func tlsOf(c *tls.Config) string {
	if c == nil {
		return "none"
	}
	return "tls server=" + c.ServerName + " skipverify=" + map[bool]string{true: "yes", false: "no"}[c.InsecureSkipVerify] +
		" roots=" + map[bool]string{true: "set", false: "unset"}[c.RootCAs != nil] +
		" certs=" + map[bool]string{true: "set", false: "unset"}[len(c.Certificates) > 0]
}

func describe(cfg *pgx.ConnConfig) described {
	d := described{
		Host: cfg.Host, Port: cfg.Port, User: cfg.User, Database: cfg.Database, Password: cfg.Password,
		RuntimeParams: cfg.RuntimeParams, ConnectTimeout: cfg.ConnectTimeout, TLS: tlsOf(cfg.TLSConfig),
		SSLNegotiation: cfg.SSLNegotiation, ChannelBinding: cfg.ChannelBinding,
		MinProtocol: cfg.MinProtocolVersion, MaxProtocol: cfg.MaxProtocolVersion, RequireAuth: cfg.RequireAuth,
		ValidateConnectSet: cfg.ValidateConnect != nil,
	}
	for _, fb := range cfg.Fallbacks {
		d.Fallbacks = append(d.Fallbacks, fb.Host+":"+strconv.Itoa(int(fb.Port))+" "+tlsOf(fb.TLSConfig))
	}
	return d
}

// TestDialConfigDoesNotReadTheEnvironment: whatever the variables for the parameters the connection
// decides or that are pinned hold, including a password file that matches every host and a client
// certificate that is not there, the config a source is dialed with is the one a clean environment
// gives, with the connection's own password.
func TestDialConfigDoesNotReadTheEnvironment(t *testing.T) {
	for _, dsn := range []string{
		"host=src.example.com dbname=app user=u password=pw",
		"host=a.example.com,b.example.com port=6543,6544 dbname=app user=u password=pw sslmode=require",
		"postgres://u:pw@src.example.com/app?sslmode=disable&application_name=mine&connect_timeout=7",
		"host=src.example.com dbname=app user=u password=pw sslmode=verify-full sslrootcert=system",
		"host=src.example.com dbname=app user=u password=pw sslmode=require sslnegotiation=direct target_session_attrs=read-write channel_binding=disable",
	} {
		t.Run(dsn, func(t *testing.T) {
			clean, err := dialDSN(dsn)
			require.NoError(t, err)

			// The premise: pgx, given the same connection as a string that leaves the
			// environment its say, reads it.
			conn, err := backingconn.FromDSN(dsn)
			require.NoError(t, err)
			for k, v := range hostileEnvironment {
				t.Setenv(k, v)
			}
			t.Setenv("PGPASSFILE", wildcardPasswordFile(t, "from-the-file"))
			bare, err := pgx.ParseConfig(conn.DSN())
			if err == nil {
				require.NotEqual(t, describe(clean), describe(bare), "premise: the environment changes what pgx dials")
			}

			hostile, err := dialDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, describe(clean), describe(hostile))
			require.Equal(t, "pw", hostile.Password)
			require.Empty(t, hostile.RuntimeParams["options"], "PGOPTIONS is not read")
		})
	}
}

// The operational parameters are the process's to set: what the connection leaves out is taken
// from the environment, and what it sets wins.
func TestDialConfigHonorsTheOperationalEnvironment(t *testing.T) {
	t.Setenv("PGAPPNAME", "from-the-environment")
	t.Setenv("PGCONNECT_TIMEOUT", "9")
	t.Setenv("PGTZ", "UTC")
	t.Setenv("PGTARGETSESSIONATTRS", "read-only")
	t.Setenv("PGSSLSNI", "0")
	t.Setenv("PGMINPROTOCOLVERSION", "3.2")

	const base = "host=src.example.com port=5432 user=u password=p dbname=d sslmode=require "
	cfg, err := dialDSN(base)
	require.NoError(t, err)
	require.Equal(t, "from-the-environment", cfg.RuntimeParams["application_name"])
	require.Equal(t, "UTC", cfg.RuntimeParams["timezone"])
	require.Equal(t, 9*time.Second, cfg.ConnectTimeout)
	require.NotNil(t, cfg.ValidateConnect, "target_session_attrs=read-only")
	require.Empty(t, cfg.TLSConfig.ServerName, "sslsni=0")
	require.Equal(t, "3.2", cfg.MinProtocolVersion)

	cfg, err = dialDSN(base + "application_name=mine connect_timeout=7 target_session_attrs=any")
	require.NoError(t, err)
	require.Equal(t, "mine", cfg.RuntimeParams["application_name"])
	require.Equal(t, 7*time.Second, cfg.ConnectTimeout)
	require.Nil(t, cfg.ValidateConnect)
}

// A service cannot be pinned, so a process that has one set dials no source.
func TestDialConfigRefusesAServiceInTheEnvironment(t *testing.T) {
	services := filepath.Join(t.TempDir(), "services")
	require.NoError(t, os.WriteFile(services, []byte("[s]\npassword=from-the-service\n"), 0o600))
	t.Setenv("PGSERVICEFILE", services)
	t.Setenv("PGSERVICE", "s")
	_, err := dialDSN("host=src.example.com port=5432 user=postgres password=p dbname=postgres")
	require.ErrorContains(t, err, "PGSERVICE")
}

// TestDialConfigKeepsWhatAConnectionMayCarry: the parameters a connection may carry reach pgx.
func TestDialConfigKeepsWhatAConnectionMayCarry(t *testing.T) {
	cfg, err := dialDSN("host=src.example.com port=6543 user=u password=p dbname=d connect_timeout=7 application_name=x sslmode=disable target_session_attrs=read-only channel_binding=require")
	require.NoError(t, err)
	require.Equal(t, "src.example.com", cfg.Host)
	require.EqualValues(t, 6543, cfg.Port)
	require.Equal(t, "u", cfg.User)
	require.Equal(t, "p", cfg.Password)
	require.Equal(t, "d", cfg.Database)
	require.Equal(t, "x", cfg.RuntimeParams["application_name"])
	require.Equal(t, 7*time.Second, cfg.ConnectTimeout)
	require.Equal(t, "require", cfg.ChannelBinding)
	require.NotNil(t, cfg.ValidateConnect, "target_session_attrs is honored")
	require.Nil(t, cfg.TLSConfig)
	require.Equal(t, "password,md5,scram-sha-256,none", cfg.RequireAuth, "a source cannot ask for our Kerberos credentials")
}

// pgx sends the server every key it does not parse itself as a setting, and the string it is given
// has the keys of libpq's parameters: for each parameter a connection may carry, and with all of
// them and every pin, what is left in RuntimeParams is application_name alone, and the parameters
// pgx parses took the value of the string.
func TestDialConfigSendsTheServerOnlyApplicationName(t *testing.T) {
	// The operational parameters a connection leaves out come from the environment, and CI exports
	// some of them (PGCONNECT_TIMEOUT): what this test compares is the string alone. pgx reads an
	// empty variable as unset.
	for _, k := range []string{"PGAPPNAME", "PGCONNECT_TIMEOUT", "PGTARGETSESSIONATTRS", "PGCHANNELBINDING"} {
		t.Setenv(k, "")
	}
	const base = "host=src.example.com port=6543 user=u password=p dbname=d "
	extras := []string{
		"", "sslmode=require", "sslmode=disable", "sslmode=prefer", "sslmode=verify-full sslrootcert=system", "sslnegotiation=direct",
		"application_name=mine", "connect_timeout=7", "target_session_attrs=read-write", "target_session_attrs=any",
		"load_balance_hosts=disable", "gssencmode=disable", "channel_binding=require", "channel_binding=disable",
		"hostaddr=10.0.0.1", "keepalives=0", "keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=3",
	}
	everything := "application_name=mine connect_timeout=7 target_session_attrs=read-only load_balance_hosts=disable " +
		"gssencmode=disable channel_binding=require hostaddr=10.0.0.1 keepalives=1 keepalives_idle=60 " +
		"keepalives_interval=10 keepalives_count=3 sslmode=require sslnegotiation=direct"
	extras = append(extras, everything)

	for _, extra := range extras {
		t.Run(extra, func(t *testing.T) {
			conn, err := backingconn.FromDSN(base + extra)
			require.NoError(t, err)
			cfg, err := dialConfig(conn)
			require.NoError(t, err)

			for key := range cfg.RuntimeParams {
				require.Equal(t, "application_name", key, "pgx would send the server a setting called %s", key)
			}
			require.Equal(t, conn.Params["application_name"], cfg.RuntimeParams["application_name"])

			require.Equal(t, conn.Host(), cfg.Host)
			require.EqualValues(t, conn.Port(), cfg.Port)
			require.Equal(t, conn.User, cfg.User)
			require.Equal(t, conn.Password, cfg.Password)
			require.Equal(t, conn.Database, cfg.Database)
			if conn.SSLMode == "disable" {
				require.Nil(t, cfg.TLSConfig)
			} else {
				require.NotNil(t, cfg.TLSConfig)
				require.Equal(t, conn.SSLRootCert == "system", cfg.TLSConfig.RootCAs != nil, "sslrootcert")
			}
			require.Equal(t, cmp.Or(conn.SSLNegotiation, "postgres"), cfg.SSLNegotiation)
			require.Equal(t, cmp.Or(conn.Params["channel_binding"], "prefer"), cfg.ChannelBinding)
			require.Equal(t, "password,md5,scram-sha-256,none", cfg.RequireAuth)
			wantTimeout, _ := strconv.Atoi(conn.Params["connect_timeout"])
			require.Equal(t, time.Duration(wantTimeout)*time.Second, cfg.ConnectTimeout)
			require.Equal(t, !slices.Contains([]string{"", "any"}, conn.Params["target_session_attrs"]), cfg.ValidateConnect != nil)
		})
	}
}

// hostaddr is the address of each host, which pgx has no parameter for: it resolves the host to it
// and verifies the certificate against the host.
func TestDialConfigHonorsHostaddr(t *testing.T) {
	cfg, err := dialDSN("host=a.example.com,b.example.com port=5432 user=u password=p dbname=d sslmode=verify-full sslrootcert=system hostaddr=10.0.0.1,10.0.0.2")
	require.NoError(t, err)
	require.NotNil(t, cfg.LookupFunc)
	addrs, err := cfg.LookupFunc(context.Background(), "a.example.com")
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1"}, addrs)
	addrs, err = cfg.LookupFunc(context.Background(), "b.example.com")
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.2"}, addrs)
	require.Equal(t, "a.example.com", cfg.TLSConfig.ServerName, "the certificate is checked against the host, not the address")
	require.Equal(t, "b.example.com", cfg.Fallbacks[0].TLSConfig.ServerName)

	// Without it the host is resolved as it is.
	cfg, err = dialDSN("host=127.0.0.1 port=5432 user=u password=p dbname=d")
	require.NoError(t, err)
	addrs, err = cfg.LookupFunc(context.Background(), "127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1"}, addrs)
}

// The keepalives parameters are honored by the dialer, since pgx has no parameter for them.
func TestDialConfigHonorsKeepalives(t *testing.T) {
	const base = "host=src.example.com port=5432 user=u password=p dbname=d "
	conn, err := backingconn.FromDSN(base + "keepalives=0")
	require.NoError(t, err)
	dialer := keepaliveDialer(conn, 3*time.Second)
	require.Negative(t, dialer.KeepAlive, "keepalives=0 turns them off")
	require.Equal(t, 3*time.Second, dialer.Timeout)

	conn, err = backingconn.FromDSN(base + "keepalives_idle=60 keepalives_interval=10 keepalives_count=3")
	require.NoError(t, err)
	require.Equal(t, net.KeepAliveConfig{Enable: true, Idle: 60 * time.Second, Interval: 10 * time.Second, Count: 3}, keepaliveDialer(conn, 0).KeepAliveConfig)

	conn, err = backingconn.FromDSN(base)
	require.NoError(t, err)
	require.Nil(t, keepaliveDialer(conn, 0), "nothing named: pgx keeps its dialer")

	cfg, err := dialDSN(base + "keepalives_idle=60")
	require.NoError(t, err)
	require.NotNil(t, cfg.DialFunc)
}

// pg_dump is given the string every libpq client of a connection is dialed with, which has the
// connection's parameters and the pins of params.go and not the password; the password is in its
// environment.
func TestDumpSchemaGivesPgDumpThePinnedStringAndThePasswordInTheEnvironment(t *testing.T) {
	bin := t.TempDir()
	out := filepath.Join(t.TempDir(), "invocation")
	script := "#!/bin/sh\n" +
		"for a in \"$@\"; do printf 'ARG %s\\n' \"$a\"; done > " + out + "\n" +
		"printf 'PGPASSWORD %s\\n' \"$PGPASSWORD\" >> " + out + "\n" +
		"echo 'CREATE TABLE t ();'\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "pg_dump"), []byte(script), 0o755))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PGPASSWORD", "from-the-environment")

	conn, err := backingconn.FromDSN("host=src.example.com dbname=app user=u password='s3 cr3t' connect_timeout=7")
	require.NoError(t, err)
	dump, err := (&source{src: conn, ctx: context.Background()}).DumpSchema([]string{"public.t"})
	require.NoError(t, err)
	require.Contains(t, dump, "CREATE TABLE t ();")

	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	invocation := string(raw)
	require.Contains(t, invocation, "ARG --table=public.t\n")
	require.Contains(t, invocation, "PGPASSWORD s3 cr3t\n", "the connection's password, not the one of the environment")
	require.NotContains(t, invocation, "from-the-environment")

	noPassword := conn.Clone()
	noPassword.Password = ""
	require.Contains(t, invocation, "ARG "+noPassword.LibpqString()+"\n")
	require.NotContains(t, strings.SplitN(invocation, "PGPASSWORD", 2)[0], "s3 cr3t", "the password is not in the argument list")
	for _, pin := range []string{"port='5432'", "sslmode='require'", "connect_timeout='7'", "options=''", "sslcertmode='disable'"} {
		require.Contains(t, invocation, pin)
	}
	// client_encoding is the environment's: pinned to its default, auto, libpq would take the
	// encoding from the locale.
	for _, left := range []string{"client_encoding", "application_name", "sslsni", "keepalives"} {
		require.NotContains(t, invocation, left)
	}
}
