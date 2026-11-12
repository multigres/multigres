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
	"testing"

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
		"gssencmode":           {[]string{"disable", "prefer"}, []string{"allow", "require", "bogus", "", "Prefer"}},
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

// A parameter is refused or allowed by the table alone, so the lists derived from it cannot
// drift from what Validate does.
func TestParamsTableIsConsistent(t *testing.T) {
	for name, def := range params {
		if def.carried {
			require.True(t, def.allowed, "%s is carried, so it is allowed", name)
			require.True(t, def.check == nil || name == "sslmode" || name == "sslnegotiation",
				"%s: only a carried field with a closed set of values has a check", name)
		}
		if def.allowed && !def.carried {
			require.NotNil(t, def.check, "%s: an allowed parameter has a rule for its value", name)
		}
	}
}
