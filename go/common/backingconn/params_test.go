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

package backingconn

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// withParam is a valid connection that also carries one parameter.
func withParam(key, value string) *Conn {
	return valid(func(c *Conn) { c.Params = map[string]string{key: value} })
}

// The values of the parameters a connection may carry are checked as libpq checks them, so that
// what libpq refuses at connect time is refused when the connection is created.
func TestParamValuesAreChecked(t *testing.T) {
	ints := []string{"connect_timeout", "keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count"}
	for _, key := range ints {
		for _, ok := range []string{"0", "1", "5", "2147483647"} {
			require.NoError(t, withParam(key, ok).Validate(), "%s=%s", key, ok)
		}
		for _, bad := range []string{"abc", "1.5", "", "99999999999", "2147483648", "-1", " 5", "5 ", "+5", "0x10", "1e3", "٥"} {
			err := withParam(key, bad).Validate()
			require.ErrorContains(t, err, "parameter "+key+" is not valid", "%s=%q", key, bad)
			require.NotContains(t, err.Error(), bad+"\"", "an error never repeats a value")
		}
	}

	enums := map[string]struct{ ok, bad []string }{
		"gssencmode":           {[]string{"disable"}, []string{"prefer", "allow", "require", "bogus", "", "Disable"}},
		"channel_binding":      {[]string{"disable", "prefer", "require"}, []string{"allow", "bogus", "", "REQUIRE"}},
		"target_session_attrs": {[]string{"any", "read-write", "read-only", "primary", "standby", "prefer-standby"}, []string{"bogus", "", "Any", "read_write"}},
		"load_balance_hosts":   {[]string{"disable"}, []string{"random", "bogus", "", "round-robin", "Random"}},
	}
	for key, values := range enums {
		for _, ok := range values.ok {
			require.NoError(t, withParam(key, ok).Validate(), "%s=%s", key, ok)
		}
		for _, bad := range values.bad {
			require.ErrorContains(t, withParam(key, bad).Validate(), "parameter "+key+" is not valid", "%s=%q", key, bad)
		}
	}

	// application_name is any text libpq can send.
	for _, ok := range []string{"", "a b", "ünï", "a'b\\c"} {
		require.NoError(t, withParam("application_name", ok).Validate(), "%q", ok)
	}
	for _, bad := range []string{"a\x00b", "a\xffb"} {
		require.ErrorContains(t, withParam("application_name", bad).Validate(), "parameter application_name is not valid", "%q", bad)
	}
}

func TestHostaddr(t *testing.T) {
	for _, ok := range []string{"10.0.0.1", "127.0.0.1", "::1", "2001:db8::1"} {
		require.NoError(t, withParam("hostaddr", ok).Validate(), ok)
	}
	for _, bad := range []string{"localhost", "notanip", "", "1.1", "10.0.0.1,", ",10.0.0.1", "fe80::1%eth0", "10.0.0.1/8", "h.example.com", "1.1.1.1 "} {
		require.ErrorContains(t, withParam("hostaddr", bad).Validate(), "parameter hostaddr is not valid", "%q", bad)
	}

	// One address for each host, which is libpq's rule too ("could not match 1 host names to 2
	// hostaddr values").
	require.ErrorContains(t, withParam("hostaddr", "1.1.1.1,2.2.2.2").Validate(), "one address for each host")
	two := valid(func(c *Conn) {
		c.Hosts = []string{"a", "b"}
		c.Params = map[string]string{"hostaddr": "1.1.1.1"}
	})
	require.ErrorContains(t, two.Validate(), "one address for each host")
	two.Params["hostaddr"] = "1.1.1.1,2.2.2.2"
	require.NoError(t, two.Validate())

	// With no host there is nothing to verify the certificate against, and nothing to dial.
	noHost := valid(func(c *Conn) {
		c.Hosts, c.SSLMode, c.SSLRootCert = nil, "verify-full", "/ca.pem"
		c.Params = map[string]string{"hostaddr": "1.1.1.1"}
	})
	require.ErrorContains(t, noHost.Validate(), "host is required")
}

// Text libpq cannot read as it is written is refused in every field, in a connection that came
// from JSON as well as one that came from a string.
func TestValidateRefusesNULAndInvalidUTF8(t *testing.T) {
	for name, mutate := range map[string]func(*Conn, string){
		"host":        func(c *Conn, v string) { c.Hosts = []string{v} },
		"user":        func(c *Conn, v string) { c.User = v },
		"database":    func(c *Conn, v string) { c.Database = v },
		"password":    func(c *Conn, v string) { c.Password = v },
		"sslrootcert": func(c *Conn, v string) { c.SSLRootCert, c.SSLMode = v, "require" },
	} {
		for _, bad := range []string{"a\x00b", "a\xffb"} {
			c := valid(func(c *Conn) { mutate(c, bad) })
			err := c.Validate()
			require.Error(t, err, "%s %q", name, bad)
			require.NotContains(t, err.Error(), "a\x00b")
		}
	}
}

// The closed set: what a connection may carry, and nothing else, whatever libpq knows.
func TestOnlyTheAllowedParametersMayBeCarried(t *testing.T) {
	want := []string{
		"application_name", "channel_binding", "connect_timeout", "dbname", "gssencmode", "host", "hostaddr",
		"keepalives", "keepalives_count", "keepalives_idle", "keepalives_interval", "load_balance_hosts",
		"password", "port", "sslmode", "sslnegotiation", "sslrootcert", "target_session_attrs",
		"user",
	}
	require.Equal(t, want, namesWhere(func(p param) bool { return p.allowed }))

	// Everything else libpq knows is refused.
	for name, def := range params {
		if !def.allowed {
			require.ErrorContains(t, withParam(name, "x").Validate(), "parameter "+name+" is not supported", name)
		}
	}
	// The parameters that matter most are in the table, so that they are pinned and refused as not
	// supported, not left to be refused as unrecognized.
	for _, name := range []string{
		"replication", "sslcertmode", "options", "min_protocol_version", "max_protocol_version",
		"ssl_min_protocol_version", "ssl_max_protocol_version", "requirepeer", "krbsrvname", "gsslib",
		"gssdelegation", "tcp_user_timeout", "oauth_issuer", "scram_client_key", "scram_server_key", "client_encoding",
		"fallback_application_name", "sslcompression", "sslsni", "require_auth", "requiressl",
		"passfile", "service", "servicefile", "sslcert", "sslkey", "sslpassword", "sslcrl", "sslcrldir", "sslkeylogfile",
	} {
		require.True(t, IsParam(name), name)
	}
	require.False(t, IsParam("nope"))
}

// A parameter is refused or allowed by the table alone, so what Validate does cannot drift from the
// table, and the string never has a key that libpq 17 does not know.
func TestParamsTableIsConsistent(t *testing.T) {
	unknownToLibpq17 := map[string]bool{}
	for name, def := range params {
		require.NotZero(t, def.class, "%s has a class", name)
		if def.class == classCarried {
			require.True(t, def.allowed, "%s is carried, so it is allowed", name)
			require.True(t, def.check == nil || name == "sslmode" || name == "sslnegotiation",
				"%s: only a carried field with a closed set of values has a check", name)
		}
		if def.allowed && def.class != classCarried {
			require.NotNil(t, def.check, "%s: an allowed parameter has a rule for its value", name)
		}
		require.Equal(t, def.class == classCarried && (name == "sslrootcert" || name == "sslnegotiation"), def.fillsEmpty,
			"%s: only the carried fields that the connection may leave empty are filled in", name)
		if def.class != classSecret && def.class != classInternal && !def.fillsEmpty {
			require.Empty(t, def.value, "%s: only what the string writes by itself has a value", name)
		}
		if def.class == classService {
			require.False(t, def.allowed, "%s cannot be pinned, so a string never carries it", name)
		}
		if !slices.Contains(libpq17Parameters, name) {
			unknownToLibpq17[name] = true
			// libpq 17 refuses a key it does not know, so none that it does not know is ever written.
			require.NotEqual(t, classCarried, def.class, "%s", name)
			require.False(t, def.allowed, "%s: libpq 17 does not know it, so a connection cannot carry it", name)
			require.NotContains(t, []class{classSecret, classInternal}, def.class, "%s: libpq 17 does not know it, so it is not pinned", name)
		}
	}
	// What libpq 17 does not know: the protocol versions (new in libpq 18, which pgx reads), the other
	// parameters of libpq 18, and servicefile, which pgx reads and neither libpq 17 nor 18 lists.
	require.Equal(t, []string{
		"max_protocol_version", "min_protocol_version", "oauth_client_id", "oauth_client_secret", "oauth_issuer", "oauth_scope",
		"scram_client_key", "scram_server_key", "servicefile", "sslkeylogfile",
	}, slices.Sorted(maps.Keys(unknownToLibpq17)))
}

// What each parameter is, which is what decides whether a string pins it. A new libpq or pgx
// parameter has to be placed here and in the table before it can be dialed.
func TestParamsClasses(t *testing.T) {
	want := map[class][]string{
		classCarried:     {"dbname", "host", "password", "port", "sslmode", "sslnegotiation", "sslrootcert", "user"},
		classSecret:      {"gssdelegation", "gssencmode", "gsslib", "krbsrvname", "passfile", "sslcert", "sslcertmode", "sslkey", "sslpassword"},
		classInternal:    {"channel_binding", "hostaddr", "options", "require_auth", "ssl_max_protocol_version", "ssl_min_protocol_version", "sslcrl", "sslcrldir"},
		classOperational: {"application_name", "client_encoding", "connect_timeout", "load_balance_hosts", "max_protocol_version", "min_protocol_version", "requirepeer", "requiressl", "sslcompression", "sslsni", "target_session_attrs"},
		classStringOnly: {
			"fallback_application_name", "keepalives", "keepalives_count", "keepalives_idle", "keepalives_interval", "oauth_client_id",
			"oauth_client_secret", "oauth_issuer", "oauth_scope", "replication", "scram_client_key", "scram_server_key", "sslkeylogfile", "tcp_user_timeout",
		},
		classService: {"service", "servicefile"},
	}
	for c, names := range want {
		require.Equal(t, names, namesWhere(func(p param) bool { return p.class == c }), "class %d", c)
	}
}

// libpq17Parameters are the keywords libpq 17 lists (PQconndefaults), and requiressl, which it
// reads and does not list.
var libpq17Parameters = []string{
	"application_name", "channel_binding", "client_encoding", "connect_timeout", "dbname", "fallback_application_name",
	"gssdelegation", "gssencmode", "gsslib", "host", "hostaddr", "keepalives", "keepalives_count", "keepalives_idle",
	"keepalives_interval", "krbsrvname", "load_balance_hosts", "options", "passfile", "password", "port", "replication",
	"require_auth", "requirepeer", "requiressl", "service", "ssl_max_protocol_version", "ssl_min_protocol_version",
	"sslcert", "sslcertmode", "sslcompression", "sslcrl", "sslcrldir", "sslkey", "sslmode", "sslnegotiation", "sslpassword",
	"sslrootcert", "sslsni", "target_session_attrs", "tcp_user_timeout", "user",
}

// pgxEnvironment is the environment pgx v5.10 reads (parseEnvSettings), by the libpq name of what
// each sets.
var pgxEnvironment = map[string]string{
	"PGHOST": "host", "PGPORT": "port", "PGDATABASE": "dbname", "PGUSER": "user", "PGPASSWORD": "password",
	"PGPASSFILE": "passfile", "PGAPPNAME": "application_name", "PGCONNECT_TIMEOUT": "connect_timeout",
	"PGSSLMODE": "sslmode", "PGSSLKEY": "sslkey", "PGSSLCERT": "sslcert", "PGSSLSNI": "sslsni",
	"PGSSLROOTCERT": "sslrootcert", "PGSSLPASSWORD": "sslpassword", "PGSSLNEGOTIATION": "sslnegotiation",
	"PGTARGETSESSIONATTRS": "target_session_attrs", "PGSERVICE": "service", "PGSERVICEFILE": "servicefile",
	"PGOPTIONS": "options", "PGMINPROTOCOLVERSION": "min_protocol_version", "PGMAXPROTOCOLVERSION": "max_protocol_version",
	"PGCHANNELBINDING": "channel_binding", "PGREQUIREAUTH": "require_auth",
	// PGTZ is the session's time zone, which libpq sends the same way. It is operational and not a
	// connection parameter, so it is not in the table.
	"PGTZ": "timezone",
}

// Every parameter libpq 17 knows, and every parameter pgx reads from its environment, has a class.
// A newer libpq or pgx that knows another one is not dialed until it is classified.
func TestEveryParameterTheClientsReadHasAClass(t *testing.T) {
	for _, name := range libpq17Parameters {
		require.Contains(t, params, name, "libpq 17 knows %s", name)
	}
	for env, name := range pgxEnvironment {
		if name == "timezone" {
			require.False(t, IsParam(name), "%s is not a connection parameter", env)
			continue
		}
		require.Contains(t, params, name, "pgx reads %s", env)
	}
}

// keysOf is the keys of a rendered string.
func keysOf(t *testing.T, rendered string) map[string]string {
	t.Helper()
	kv := map[string]string{}
	for pair := range strings.SplitSeq(rendered, "' ") {
		key, value, ok := strings.Cut(pair, "='")
		require.True(t, ok, pair)
		kv[key] = strings.TrimSuffix(value, "'")
	}
	return kv
}

// The string clients are dialed with carries the connection's parameters and the pins of the
// secret and internal ones, and nothing of the other classes that the connection does not set, and
// libpq 17 knows every key of it.
func TestRenderedString(t *testing.T) {
	conn := valid(func(c *Conn) {
		c.Hosts, c.Ports = []string{"h1", "h2"}, []int{5432, 5433}
		c.Password = `p w'd\`
		c.Params = map[string]string{"connect_timeout": "7", "keepalives": "0", "keepalives_idle": "60", "hostaddr": "10.0.0.1,10.0.0.2", "target_session_attrs": "read-write"}
	})
	require.NoError(t, conn.Validate())

	kv := keysOf(t, conn.LibpqString())
	require.ElementsMatch(t, []string{
		// carried
		"host", "port", "dbname", "user", "password", "sslmode", "sslrootcert", "sslnegotiation",
		// secret
		"passfile", "sslcert", "sslkey", "sslpassword", "sslcertmode", "gssencmode", "gssdelegation", "krbsrvname", "gsslib",
		// internal
		"hostaddr", "options", "sslcrl", "sslcrldir", "channel_binding", "require_auth", "ssl_min_protocol_version", "ssl_max_protocol_version",
		// what the connection sets of the others
		"connect_timeout", "keepalives", "keepalives_idle", "target_session_attrs",
	}, slices.Collect(maps.Keys(kv)))
	for key := range kv {
		require.Contains(t, libpq17Parameters, key, "libpq 17 refuses a key it does not know")
	}
	// The connection's own values win over pins; the pins are the defaults otherwise.
	require.Equal(t, "60", kv["keepalives_idle"])
	require.Equal(t, "h1,h2", kv["host"])
	require.Equal(t, "5432,5433", kv["port"])
	require.Equal(t, "7", kv["connect_timeout"])
	require.Equal(t, "0", kv["keepalives"])
	require.Equal(t, "10.0.0.1,10.0.0.2", kv["hostaddr"])
	require.Equal(t, "read-write", kv["target_session_attrs"])
	require.Equal(t, "prefer", kv["sslmode"])
	require.Equal(t, "postgres", kv["sslnegotiation"])
	require.Equal(t, "", kv["sslrootcert"])
	require.Equal(t, "disable", kv["gssencmode"], "our Kerberos credentials are never offered")
	require.Equal(t, "disable", kv["sslcertmode"], "a client certificate of the dialer is never offered")
	require.Equal(t, "", kv["options"])
	require.Equal(t, "", kv["passfile"])
	require.Equal(t, "password,md5,scram-sha-256,none", kv["require_auth"])

	// The password is quoted, so a value cannot inject a parameter.
	require.Contains(t, conn.LibpqString(), `password='p w\'d\\'`)
}

// An operational or string-only parameter the connection does not set is left to the environment of
// the client, or to its default: the string does not name it.
func TestRenderLeavesTheOperationalParametersToTheEnvironment(t *testing.T) {
	kv := keysOf(t, valid(nil).LibpqString())
	for _, key := range []string{
		"application_name", "connect_timeout", "target_session_attrs", "load_balance_hosts", "client_encoding", "sslsni",
		"sslcompression", "requirepeer", "requiressl", "min_protocol_version", "max_protocol_version",
		"keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count", "tcp_user_timeout",
		"fallback_application_name", "replication", "service", "servicefile",
	} {
		require.NotContains(t, kv, key)
	}
	require.Equal(t, "7", keysOf(t, withParam("connect_timeout", "7").LibpqString())["connect_timeout"])
	require.Equal(t, "x", keysOf(t, withParam("application_name", "x").LibpqString())["application_name"])
}

// A parameter the connection does not carry is never taken from it, even by a connection that was
// not validated: the rendering is for what Validate accepted.
func TestRenderNeverTakesAParameterThatIsNotAllowed(t *testing.T) {
	conn := valid(func(c *Conn) {
		c.Params = map[string]string{"sslcert": "/c.pem", "passfile": "/p", "options": "-c x=1", "sslsni": "0", "client_encoding": "latin1"}
	})
	rendered := conn.LibpqString()
	kv := keysOf(t, rendered)
	require.Empty(t, kv["sslcert"])
	require.Empty(t, kv["passfile"])
	require.Empty(t, kv["options"])
	require.NotContains(t, kv, "sslsni")
	require.NotContains(t, kv, "client_encoding")
	require.NotContains(t, rendered, "/c.pem")
	require.NotContains(t, rendered, "/p'")
}

// pgxParses is the keys of the table that pgx v5.10 parses itself (notRuntimeParams in
// pgconn/config.go); every other key of a string it puts in RuntimeParams, which it sends to the
// server as settings.
var pgxParses = []string{
	"channel_binding", "connect_timeout", "dbname", "host", "krbsrvname", "max_protocol_version", "min_protocol_version",
	"passfile", "password", "port", "require_auth", "service", "servicefile", "sslcert", "sslkey", "sslmode", "sslnegotiation",
	"sslpassword", "sslrootcert", "sslsni", "target_session_attrs", "user",
}

// allowedSamples is a value for each parameter, other than the fields of the connection, that a
// connection may carry.
var allowedSamples = map[string]string{
	"application_name": "mine", "channel_binding": "require", "connect_timeout": "7", "gssencmode": "disable",
	"hostaddr": "10.0.0.1", "keepalives": "1", "keepalives_count": "3", "keepalives_idle": "60", "keepalives_interval": "10",
	"load_balance_hosts": "disable", "target_session_attrs": "read-write",
}

// pgx, given the string that every client is dialed with, in the environment that would change
// what it dials, takes the value of the string for every parameter it parses, and what it leaves in
// RuntimeParams are parameters of the table, which dialConfig drops, and nothing else. This is for
// the connection with each parameter it may carry, one at a time, and with all of them.
func TestPgxReadsTheString(t *testing.T) {
	for env := range pgxEnvironment {
		t.Setenv(env, "")
	}
	var variants []*Conn
	variants = append(variants, valid(nil),
		valid(func(c *Conn) { c.SSLMode = "disable" }),
		valid(func(c *Conn) { c.SSLMode, c.SSLNegotiation = "require", "direct" }),
		valid(func(c *Conn) { c.SSLMode, c.SSLRootCert = "verify-full", "system" }),
		valid(func(c *Conn) { c.Hosts, c.Ports = []string{"h1", "h2"}, []int{5432, 5433} }),
	)
	all := map[string]string{}
	for name, def := range params {
		if def.allowed && def.class != classCarried {
			sample, ok := allowedSamples[name]
			require.True(t, ok, "%s: an allowed parameter has a sample", name)
			variants = append(variants, withParam(name, sample))
			all[name] = sample
		}
	}
	variants = append(variants, valid(func(c *Conn) {
		c.SSLMode, c.SSLNegotiation = "require", "direct"
		c.Params = all
	}))

	for _, conn := range variants {
		require.NoError(t, conn.Validate(), conn)
		rendered := conn.LibpqString()
		sent := keysOf(t, rendered)
		cfg, err := pgx.ParseConfig(rendered)
		require.NoError(t, err, conn)

		wantRuntime := map[string]string{}
		for key, value := range sent {
			if !slices.Contains(pgxParses, key) {
				wantRuntime[key] = value
			}
		}
		require.Equal(t, wantRuntime, cfg.RuntimeParams, "pgx leaves the keys it does not parse as settings: %s", rendered)
		for key := range cfg.RuntimeParams {
			require.True(t, IsParam(key), "%s is a parameter of the table, so dialConfig drops it", key)
		}

		require.Equal(t, conn.Hosts[0], cfg.Host)
		require.EqualValues(t, conn.Ports[0], cfg.Port)
		require.Equal(t, conn.User, cfg.User)
		require.Equal(t, conn.Database, cfg.Database)
		require.Equal(t, conn.Password, cfg.Password)
		if conn.SSLMode == "disable" {
			require.Nil(t, cfg.TLSConfig)
		} else {
			require.NotNil(t, cfg.TLSConfig, "sslmode=%s", conn.SSLMode)
			require.Equal(t, conn.SSLRootCert == "system", cfg.TLSConfig.RootCAs != nil, "sslrootcert")
		}
		require.Equal(t, cmp.Or(conn.SSLNegotiation, "postgres"), cfg.SSLNegotiation)
		require.Equal(t, cmp.Or(conn.Params["channel_binding"], "prefer"), cfg.ChannelBinding)
		require.Equal(t, allowedAuth, cfg.RequireAuth)
		seconds, _ := strconv.Atoi(conn.Params["connect_timeout"])
		require.Equal(t, time.Duration(seconds)*time.Second, cfg.ConnectTimeout)
		require.Equal(t, !slices.Contains([]string{"", "any"}, conn.Params["target_session_attrs"]), cfg.ValidateConnect != nil)
		require.Equal(t, "postgres", cfg.KerberosSrvName)
	}
}

// PGTZ is operational: pgx sends it as the session's time zone, which is not a parameter of the
// table, so it is left alone.
func TestPgxTakesTheTimeZoneFromTheEnvironment(t *testing.T) {
	t.Setenv("PGTZ", "UTC")
	cfg, err := pgx.ParseConfig(valid(nil).LibpqString())
	require.NoError(t, err)
	require.Equal(t, "UTC", cfg.RuntimeParams["timezone"])
	require.False(t, IsParam("timezone"))
}
