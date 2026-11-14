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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// refusedDSN is a connection string the parser must refuse, the reason in the error, and the
// fragments of the string the error must not carry.
type refusedDSN struct {
	name string
	dsn  string
	// reason is a fragment of the error.
	reason string
	// secrets are fragments of dsn the error must not repeat.
	secrets []string
}

// libpqRefusals are strings libpq refuses or reads differently from a lenient parser.
var libpqRefusals = []refusedDSN{
	// Parameter names are exactly libpq's, in lower case, in both forms.
	{
		name:   "keyword: a name in upper case is not folded onto the real one",
		dsn:    "host=good.example.com HOST=evil.example.com port=5432 dbname=app user=alice",
		reason: "unrecognized",
	},
	{
		name:   "keyword: a name in mixed case",
		dsn:    "host=h port=5432 dbname=app user=alice SslMode=disable",
		reason: "unrecognized",
	},
	{
		name:   "uri: a query parameter in upper case",
		dsn:    "postgres://alice@h:5432/app?SSLMODE=disable",
		reason: "unrecognized",
	},
	{
		name:   "uri: a query parameter in mixed case",
		dsn:    "postgres://alice@h:5432/app?SslRootCert=/ca.pem",
		reason: "unrecognized",
	},

	// A parameter given twice is refused in both forms, whichever value is the valid one.
	{
		name:   "keyword: a parameter twice, the last valid",
		dsn:    "host=h port=5432 dbname=app user=alice sslmode=verify-full sslmode=disable",
		reason: "more than once",
	},
	{
		name:   "keyword: a parameter twice, the first invalid",
		dsn:    "host=h port=5432 dbname=app user=alice sslmode=bogus sslmode=disable",
		reason: "more than once",
	},
	{
		name:   "keyword: a parameter twice, the last invalid",
		dsn:    "host=h port=5432 dbname=app user=alice sslmode=disable sslmode=bogus",
		reason: "more than once",
	},
	{
		name:   "keyword: host twice",
		dsn:    "host=a host=b port=5432 dbname=app user=alice",
		reason: "more than once",
	},
	{
		name:   "uri: a query parameter twice",
		dsn:    "postgres://alice@h:5432/app?sslmode=verify-full&sslmode=disable",
		reason: "more than once",
	},
	{
		name:    "uri: a query parameter that repeats the authority",
		dsn:     "postgres://alice:s3cr3t@h:5432/app?user=bob",
		reason:  "both in the URI and as a query parameter",
		secrets: []string{"s3cr3t"},
	},

	// A keyword-form token without "=" is refused, not dropped with the rest of the string.
	{
		name:    "keyword: a word after the password",
		dsn:     "host=srv port=5432 dbname=app user=bob password=hunter2 morepw",
		reason:  `no "="`,
		secrets: []string{"hunter2", "morepw"},
	},
	{
		name:    "keyword: an unquoted password with a space",
		dsn:     "host=srv port=5432 dbname=app user=bob password=abc def",
		reason:  `no "="`,
		secrets: []string{"abc", "def"},
	},
	{
		name:   "keyword: a parameter without a value",
		dsn:    "host=srv port=5432 dbname=app user=bob sslmode",
		reason: `no "="`,
	},
	{
		name:   "keyword: trailing junk",
		dsn:    "host=srv port=5432 dbname=app user=bob trailingjunk",
		reason: `no "="`,
	},
	{
		name:   "keyword: a flag after sslmode",
		dsn:    "host=srv port=5432 dbname=app user=bob sslmode=verify-full requiressl",
		reason: `no "="`,
	},
	{
		name:   "keyword: a word after sslrootcert",
		dsn:    "host=srv port=5432 dbname=app user=bob sslrootcert=/ca.pem extra",
		reason: `no "="`,
	},
	{
		name:   "keyword: a bare word",
		dsn:    "garbage",
		reason: `no "="`,
	},

	// An unquoted password with a space makes part of it a parameter name. The name is never in
	// the error, which travels over gRPC into another pooler's logs.
	{
		name:    "keyword: a password fragment as a parameter name",
		dsn:     "host=srv port=5432 dbname=app user=bob password=ab cd=ef",
		reason:  "(unrecognized)",
		secrets: []string{"ab", "cd", "ef"},
	},
	{
		name:    "keyword: a password fragment as a name given twice",
		dsn:     "host=srv port=5432 dbname=app user=bob password=a zz=1 zz=2",
		reason:  "(unrecognized)",
		secrets: []string{"zz"},
	},
	{
		name:    "uri: a password fragment as a query parameter name",
		dsn:     "postgres://bob:pw@srv:5432/app?cd=ef",
		reason:  "(unrecognized)",
		secrets: []string{"cd", "ef", "pw"},
	},

	// Neither a port that is not a number nor a list of servers that does not fit repeats the string.
	{
		name:    "keyword: a port that is not a number",
		dsn:     "host=h port=notaport dbname=app user=alice password=s3cr3t",
		reason:  "port",
		secrets: []string{"s3cr3t", "notaport"},
	},
	{
		name:    "uri: a port that is not a number",
		dsn:     "postgres://alice:s3cr3t@h:notaport/app",
		reason:  "port",
		secrets: []string{"s3cr3t", "notaport"},
	},
	{
		name:    "uri: some servers with a port and some without",
		dsn:     "postgres://alice:s3cr3t@h1:5432,h2/app",
		reason:  "every host has a port or none does",
		secrets: []string{"s3cr3t"},
	},
	{
		name:    "uri: a password fragment as a bare query word",
		dsn:     "postgres://alice:s3cr3t@h:5432/app?fragment",
		reason:  `no "="`,
		secrets: []string{"s3cr3t", "fragment"},
	},
	{
		name:    "uri: a password fragment as a query parameter name given twice",
		dsn:     "postgres://alice:s3cr3t@h:5432/app?fragment=1&fragment=2",
		reason:  "(unrecognized)",
		secrets: []string{"s3cr3t", "fragment"},
	},

	// "#" has no meaning in a libpq URI; a URL parser would cut the string there and drop the
	// query, so the connection would silently lose its TLS settings.
	{
		name:   "uri: a # before the query",
		dsn:    "postgres://alice@h:5432/app#1?sslmode=verify-full&sslrootcert=/etc/ca.pem",
		reason: "'#'",
	},
	{
		name:    "uri: a # in the password",
		dsn:     "postgres://alice:pa#ss@h:5432/app",
		reason:  "'#'",
		secrets: []string{"pa#ss"},
	},

	// A URI query parameter without "=" is refused, not read as an empty value.
	{
		name:   "uri: sslmode without a value",
		dsn:    "postgres://alice@h:5432/app?sslmode",
		reason: `no "="`,
	},
	{
		name:   "uri: sslrootcert without a value",
		dsn:    "postgres://alice@h:5432/app?sslrootcert",
		reason: `no "="`,
	},
	{
		name:   "uri: sslnegotiation without a value",
		dsn:    "postgres://alice@h:5432/app?sslnegotiation",
		reason: `no "="`,
	},
	{
		name:   "uri: a parameter without a value after one with",
		dsn:    "postgres://alice@h:5432/app?sslmode=require&sslrootcert",
		reason: `no "="`,
	},
	{
		name:   "uri: an empty parameter",
		dsn:    "postgres://alice@h:5432/app?sslmode=require&&sslrootcert=/ca.pem",
		reason: `no "="`,
	},

	// Quoting that libpq refuses.
	{
		name:    "keyword: an unterminated quote",
		dsn:     "host=h port=5432 dbname=app user=alice password='abc",
		reason:  "not closed",
		secrets: []string{"abc"},
	},
	{
		name:    "keyword: a trailing backslash",
		dsn:     `host=h port=5432 dbname=app user=alice password=abc\`,
		reason:  "backslash",
		secrets: []string{"abc"},
	},
	{
		name:    "uri: a bad percent-encoding in the query",
		dsn:     "postgres://alice:s3cr3t@h:5432/app?sslrootcert=%zz",
		reason:  "percent-encoding",
		secrets: []string{"s3cr3t"},
	},
	// A keyword-form value starts directly after "=": libpq skips the white space and reads the
	// next word as the value.
	{
		name:   "keyword: a value after white space swallows the next word",
		dsn:    "host=h port=5432 dbname=app user=u password= sslmode=require",
		reason: `must start directly after "="`,
	},
	{
		name:   "keyword: an empty value at the end is written ''",
		dsn:    "host=h port=5432 dbname=app user=u password=",
		reason: "a value is empty",
	},
	{
		name:   "keyword: an empty sslmode is not dropped",
		dsn:    "host=h port=5432 dbname=app user=u sslmode=''",
		reason: "sslmode is empty",
	},
	{
		name:   "keyword: an empty sslnegotiation is not dropped",
		dsn:    "host=h port=5432 dbname=app user=u sslnegotiation=''",
		reason: "sslnegotiation is empty",
	},
	{
		name:   "uri: an empty sslmode is not dropped",
		dsn:    "postgres://u@h:5432/app?sslmode=",
		reason: "sslmode is empty",
	},

	// A raw "=" or white space in a URI query value, and %00.
	{
		name:   "uri: a raw = in a query value",
		dsn:    "postgres://u@h:5432/app?application_name=a=b",
		reason: "raw",
	},
	{
		name:   "uri: a raw tab in a query value",
		dsn:    "postgres://u@h:5432/app?application_name=a\tb",
		reason: "white space",
	},
	{
		name:   "uri: %00 in the password",
		dsn:    "postgres://u:p%00w@h:5432/app",
		reason: "percent-encoding",
	},
	{
		name:   "uri: %00 in the database",
		dsn:    "postgres://u@h:5432/a%00pp",
		reason: "percent-encoding",
	},
	{
		name:   "uri: invalid UTF-8 once decoded",
		dsn:    "postgres://u:p%ffw@h:5432/app",
		reason: "percent-encoding",
	},

	// NUL and invalid UTF-8 in the string itself.
	{
		name:   "keyword: a NUL in a value",
		dsn:    "host=h port=5432 dbname=app user=u password=p\x00w",
		reason: "NUL",
	},
	{
		name:   "keyword: invalid UTF-8 in a value",
		dsn:    "host=h port=5432 dbname=app user=u password=p\xffw",
		reason: "UTF-8",
	},
}

// uncarriedParams are parameters libpq reads and a backing connection does not carry.
var uncarriedParams = []string{
	"sslcert=/c.pem", "sslkey=/k.pem", "sslcrl=/l.pem", "sslcrldir=/d", "sslpassword=x", "sslkeylogfile=/k.log", "servicefile=/s", "channel_binding=require", "options=-c%20role%3Dx",
	"hostaddr=10.0.0.1", "target_session_attrs=read-write", "load_balance_hosts=disable",
	"application_name=x", "connect_timeout=5", "service=s", "passfile=/p", "requiressl=1",
}

func TestParseDSNRefuses(t *testing.T) {
	for _, tc := range libpqRefusals {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := ParseDSN(tc.dsn)
			require.Error(t, err)
			require.Nil(t, conn)
			require.ErrorContains(t, err, tc.reason)
			for _, secret := range tc.secrets {
				require.NotContains(t, err.Error(), secret)
			}
		})
	}

	t.Run("nothing", func(t *testing.T) {
		for _, dsn := range []string{"", "   ", "\t\n"} {
			_, err := ParseDSN(dsn)
			require.ErrorContains(t, err, "neither a postgres:// URI nor keyword=value pairs")
		}
	})
}

// dsnPair is one connection written in both forms of a libpq connection string. Each must be
// accepted identically: the stored form is a choice of whoever wrote the connection.
type dsnPair struct {
	name string
	uri  string
	kv   string
	want Conn
}

func TestParseDSNAccepts(t *testing.T) {
	for _, tc := range []dsnPair{
		{
			name: "every parameter",
			uri:  "postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem&sslnegotiation=direct",
			kv:   "host=db.example.com port=6543 dbname=app user=alice password=s3cret sslmode=verify-full sslrootcert=/ca.pem sslnegotiation=direct",
			want: Conn{
				Hosts: []string{"db.example.com"}, Ports: []int{6543}, Database: "app", User: "alice",
				Password: "s3cret", SSLMode: "verify-full", SSLRootCert: "/ca.pem", SSLNegotiation: "direct",
			},
		},
		{
			name: "no password, no TLS settings",
			uri:  "postgresql://alice@db.example.com:5432/app",
			kv:   "host=db.example.com port=5432 dbname=app user=alice",
			want: Conn{Hosts: []string{"db.example.com"}, Ports: []int{5432}, Database: "app", User: "alice"},
		},
		{
			name: "quoting and escapes",
			uri:  "postgres://al%40ice:p%20w%27d@db.example.com:5432/app",
			kv:   `host=db.example.com port=5432 dbname=app user=al@ice password='p w\'d'`,
			want: Conn{
				Hosts: []string{"db.example.com"}, Ports: []int{5432}, Database: "app",
				User: "al@ice", Password: "p w'd",
			},
		},
		{
			name: "ipv6 host",
			uri:  "postgres://alice@[2001:db8::1]:5432/app",
			kv:   "host=2001:db8::1 port=5432 dbname=app user=alice",
			want: Conn{Hosts: []string{"2001:db8::1"}, Ports: []int{5432}, Database: "app", User: "alice"},
		},
		{
			// libpq makes the character after a backslash literal, in a quoted value or not.
			name: "a backslash escapes any character",
			uri:  "postgres://alice:pa%5Css@h:5432/app",
			kv:   `host=h port=5432 dbname=app user=alice password=pa\\ss`,
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice", Password: `pa\ss`},
		},
		{
			// Only %XX is decoded in a query; "+" is a plus, not a space.
			name: "a plus in a query value",
			uri:  "postgres://alice@h:5432/app?sslrootcert=/a+b.pem",
			kv:   "host=h port=5432 dbname=app user=alice sslrootcert=/a+b.pem",
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice", SSLRootCert: "/a+b.pem"},
		},
		{
			name: "a percent-encoded query value",
			uri:  "postgres://alice@h:5432/app?sslrootcert=%2Fetc%2Fca.pem",
			kv:   "host=h port=5432 dbname=app user=alice sslrootcert=/etc/ca.pem",
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice", SSLRootCert: "/etc/ca.pem"},
		},
		{
			name: "spaces around =",
			uri:  "postgres://alice@h:5432/app?sslmode=disable",
			kv:   "  host =h   port =5432 dbname =app\tuser =alice\nsslmode =disable ",
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice", SSLMode: "disable"},
		},
		{
			name: "an empty value is a value, written ''",
			uri:  "postgres://alice@h:5432/app?application_name=",
			kv:   "host=h port=5432 dbname=app user=alice application_name=''",
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice", Params: map[string]string{"application_name": ""}},
		},
		{
			name: "an empty query",
			uri:  "postgres://alice@h:5432/app?",
			kv:   "host=h port=5432 dbname=app user=alice",
			want: Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fromURI, err := ParseDSN(tc.uri)
			require.NoError(t, err)
			require.Equal(t, &tc.want, fromURI)
			fromKV, err := ParseDSN(tc.kv)
			require.NoError(t, err)
			require.Equal(t, &tc.want, fromKV)
		})
	}
}

// TestParseDSNKeepsWhatItDoesNotCarry: a libpq parameter that a backing connection does not carry
// is kept, in either form, so that the migrator loses nothing; ForPooler is what refuses it.
func TestParseDSNKeepsWhatItDoesNotCarry(t *testing.T) {
	for _, param := range uncarriedParams {
		key, value, _ := strings.Cut(param, "=")
		if key == "options" {
			value = "-c role=x"
		}
		for _, dsn := range []string{
			"postgres://alice@h:5432/app?" + param,
			"host=h port=5432 dbname=app user=alice " + key + "='" + value + "'",
		} {
			conn, err := ParseDSN(dsn)
			require.NoError(t, err, dsn)
			require.Equal(t, map[string]string{key: value}, conn.Params, dsn)
			conn.Password = "p" // Validate requires one; the string under test has none
			conn.Complete()
			if !params[key].allowed {
				// Parsed, then refused by Validate.
				require.ErrorContains(t, conn.Validate(), "parameter "+key+" is not supported", dsn)
			} else {
				require.NoError(t, conn.Validate(), dsn)
			}
			require.ErrorContains(t, conn.ForPooler(), "parameter "+key+" is not supported", dsn)
			back, err := ParseDSN(conn.DSN())
			require.NoError(t, err)
			require.Equal(t, conn.Params, back.Params)
		}
	}
}

// A list of hosts or ports is decoded as a list; refusing more than one server is ForPooler's.
func TestParseDSNKeepsLists(t *testing.T) {
	conn, err := ParseDSN("host=a.example.com,b.example.com port=5432,5433 dbname=app user=alice")
	require.NoError(t, err)
	require.Equal(t, []string{"a.example.com", "b.example.com"}, conn.Hosts)
	require.Equal(t, []int{5432, 5433}, conn.Ports)

	_, err = ParseDSN("host=a port=5432,x dbname=app user=alice")
	require.ErrorContains(t, err, "port is not a valid port number")

	// A URI names a list of servers in its authority, as libpq reads it.
	conn, err = ParseDSN("postgres://alice@h1:5432,[2001:db8::1]:5433/app")
	require.NoError(t, err)
	require.Equal(t, []string{"h1", "2001:db8::1"}, conn.Hosts)
	require.Equal(t, []int{5432, 5433}, conn.Ports)
	_, err = ParseDSN("postgres://alice@h1:5432,h2/app")
	require.ErrorContains(t, err, "every host has a port or none does")
}

// TestParseDSNIgnoresTheEnvironment: the string is read as it is. A parameter it leaves out is
// not filled from PG* variables or a password file.
func TestParseDSNIgnoresTheEnvironment(t *testing.T) {
	t.Setenv("PGHOST", "elsewhere.example.com")
	t.Setenv("PGPORT", "6000")
	t.Setenv("PGUSER", "someone")
	t.Setenv("PGDATABASE", "other")
	t.Setenv("PGSSLMODE", "disable")
	conn, err := ParseDSN("host=h port=5432 dbname=app user=alice")
	require.NoError(t, err)
	require.Equal(t, &Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "alice"}, conn)

	conn, err = ParseDSN("host=h dbname=app user=alice")
	require.NoError(t, err)
	require.Empty(t, conn.Ports, "the port is left to the caller, not read from PGPORT")
	require.ErrorContains(t, conn.ForPooler(), "port is required")

	conn, err = ParseDSN("port=5432 dbname=app user=alice")
	require.NoError(t, err)
	require.Empty(t, conn.Hosts, "the host is left to the caller, not read from PGHOST")
}
