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

package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// TestBackingConnectionIsReadFromTheCatalog: the connection is a row of multigres.connections,
// read with one query, and the dsn column decodes into the structured connection.
func TestBackingConnectionIsReadFromTheCatalog(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections WHERE name", mock.MakeQueryResult([]string{"dsn"}, [][]any{{"postgres://u@h:5432/db"}}))
		conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "src", conn.GetName())
		require.Equal(t, "h", conn.GetHost())
		require.Equal(t, "u", conn.GetUser())
		require.EqualValues(t, 1, qs.AdminQueryCount(), "one query, nothing created")
	})
	t.Run("not found", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", mock.MakeQueryResult(nil, nil))
		_, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("a keyword=value dsn the migrator wrote is servable as it is", func(t *testing.T) {
		const dsn = "host=db.example.com port=5432 dbname=app user=repl password='p w'"
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", mock.MakeQueryResult([]string{"dsn"}, [][]any{{dsn}}))
		conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "db.example.com", conn.GetHost())
		require.Equal(t, "repl", conn.GetUser())
		require.Equal(t, "p w", conn.GetPassword())
	})
	t.Run("a dsn that does not name one server is refused with its code, naming the connection", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", mock.MakeQueryResult([]string{"dsn"}, [][]any{{"host=h1,h2 port=5432 dbname=app user=u"}}))
		_, _, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, `connection "src"`)
	})
}

// dsnCase is one connection described in both forms of a libpq connection string. Every case
// must be accepted or refused identically in each: the stored form is a choice of whoever
// wrote the connection, not a different contract.
type dsnCase struct {
	name string
	uri  string
	kv   string
	// refused is a fragment of the error, or empty when the connection is accepted.
	refused string
}

var dsnCases = []dsnCase{
	{
		name: "every parameter",
		uri:  "postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem&sslnegotiation=direct",
		kv:   "host=db.example.com port=6543 dbname=app user=alice password=s3cret sslmode=verify-full sslrootcert=/ca.pem sslnegotiation=direct",
	},
	{
		name: "no password, no TLS settings",
		uri:  "postgresql://alice@db.example.com:5432/app",
		kv:   "host=db.example.com port=5432 dbname=app user=alice",
	},
	{
		name: "quoting and escapes",
		uri:  "postgres://al%40ice:p%20w%27d@db.example.com:5432/app",
		kv:   `host=db.example.com port=5432 dbname=app user=al@ice password='p w\'d'`,
	},
	{
		name: "ipv6 host",
		uri:  "postgres://alice@[2001:db8::1]:5432/app",
		kv:   "host=2001:db8::1 port=5432 dbname=app user=alice",
	},

	{name: "no host", uri: "postgres://alice@:5432/app", kv: "port=5432 dbname=app user=alice", refused: "host is required"},
	{name: "no port", uri: "postgres://alice@h/app", kv: "host=h dbname=app user=alice", refused: "port is required"},
	{name: "no dbname", uri: "postgres://alice@h:5432", kv: "host=h port=5432 user=alice", refused: "dbname is required"},
	{name: "no user", uri: "postgres://h:5432/app", kv: "host=h port=5432 dbname=app", refused: "user is required"},
	{name: "a host list", uri: "postgres://alice@h1:5432,h2:5432/app", kv: "host=h1,h2 port=5432 dbname=app user=alice", refused: "more than one host"},
	{name: "a port list", uri: "postgres://alice@h:5432,5433/app", kv: "host=h port=5432,5433 dbname=app user=alice", refused: "port"},
	{name: "a bad port", uri: "postgres://alice@h:notaport/app", kv: "host=h port=notaport dbname=app user=alice", refused: "port"},
	{name: "a socket directory", uri: "postgres://alice@:5432/app?host=%2Fvar%2Frun%2Fpostgresql", kv: "host=/var/run/postgresql port=5432 dbname=app user=alice", refused: "socket"},
	{name: "hostaddr", uri: "postgres://alice@h:5432/app?hostaddr=10.0.0.1", kv: "host=h hostaddr=10.0.0.1 port=5432 dbname=app user=alice", refused: "hostaddr"},
	{name: "target_session_attrs", uri: "postgres://alice@h:5432/app?target_session_attrs=read-write", kv: "host=h port=5432 dbname=app user=alice target_session_attrs=read-write", refused: "target_session_attrs"},
	{name: "load_balance_hosts", uri: "postgres://alice@h:5432/app?load_balance_hosts=random", kv: "host=h port=5432 dbname=app user=alice load_balance_hosts=random", refused: "load_balance_hosts"},
	// A parameter that is not carried is refused, not dropped: dropping it would weaken the
	// connection or change how it authenticates.
	{name: "channel_binding", uri: "postgres://alice@h:5432/app?channel_binding=require", kv: "host=h port=5432 dbname=app user=alice channel_binding=require", refused: "channel_binding"},
	{name: "sslcert", uri: "postgres://alice@h:5432/app?sslcert=/c.pem", kv: "host=h port=5432 dbname=app user=alice sslcert=/c.pem", refused: "sslcert"},
	{name: "sslkey", uri: "postgres://alice@h:5432/app?sslkey=/k.pem", kv: "host=h port=5432 dbname=app user=alice sslkey=/k.pem", refused: "sslkey"},
	{name: "options", uri: "postgres://alice@h:5432/app?options=-c%20role%3Dx", kv: "host=h port=5432 dbname=app user=alice options='-c role=x'", refused: "options"},
	{name: "application_name", uri: "postgres://alice@h:5432/app?application_name=x", kv: "host=h port=5432 dbname=app user=alice application_name=x", refused: "application_name"},
	{name: "a parameter twice", uri: "postgres://alice@h:5432/app?sslmode=require&sslmode=disable", kv: "", refused: "more than once"},
	{name: "a URI parameter that repeats the authority", uri: "postgres://alice@h:5432/app?user=bob", kv: "", refused: "both in the URI and as a query parameter"},
	{name: "nothing", uri: "", kv: "", refused: "neither"},
}

func TestParseBackingDSN(t *testing.T) {
	for _, tc := range dsnCases {
		for _, form := range []struct{ name, dsn string }{{"uri", tc.uri}, {"keyword", tc.kv}} {
			if form.dsn == "" && tc.name != "nothing" {
				continue // a case that only exists in the other form
			}
			t.Run(tc.name+"/"+form.name, func(t *testing.T) {
				got, err := parseBackingDSN(form.dsn)
				if tc.refused != "" {
					require.Error(t, err)
					require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
					require.ErrorContains(t, err, tc.refused)
					// No value of the dsn, which holds the password, is in the message.
					require.NotContains(t, err.Error(), "s3cret")
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, got.GetHost())
				require.NotZero(t, got.GetPort())
				require.NotEmpty(t, got.GetDatabase())
				require.NotEmpty(t, got.GetUser())
			})
		}
	}

	// Both forms of an accepted case decode to the same connection.
	for _, tc := range dsnCases {
		if tc.refused != "" {
			continue
		}
		fromURI, err := parseBackingDSN(tc.uri)
		require.NoError(t, err, tc.name)
		fromKV, err := parseBackingDSN(tc.kv)
		require.NoError(t, err, tc.name)
		require.True(t, proto.Equal(fromURI, fromKV), "%s: %v != %v", tc.name, fromURI, fromKV)
	}

	got, err := parseBackingDSN("postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem&sslnegotiation=direct")
	require.NoError(t, err)
	require.Equal(t, "db.example.com", got.GetHost())
	require.EqualValues(t, 6543, got.GetPort())
	require.Equal(t, "app", got.GetDatabase())
	require.Equal(t, "alice", got.GetUser())
	require.Equal(t, "s3cret", got.GetPassword())
	require.Equal(t, "verify-full", got.GetSslMode())
	require.Equal(t, "/ca.pem", got.GetSslRootCert())
	require.Equal(t, "direct", got.GetSslNegotiation())
}

// TestParseBackingDSNIgnoresTheEnvironment: the keyword form is read as the catalog stores
// it. A parameter the string leaves out is not filled from PG* variables or a password file.
func TestParseBackingDSNIgnoresTheEnvironment(t *testing.T) {
	t.Setenv("PGHOST", "elsewhere.example.com")
	t.Setenv("PGPORT", "6000")
	t.Setenv("PGUSER", "someone")
	t.Setenv("PGDATABASE", "other")
	_, err := parseBackingDSN("host=h dbname=app user=alice")
	require.ErrorContains(t, err, "port is required")
	_, err = parseBackingDSN("host=h port=5432 dbname=app")
	require.ErrorContains(t, err, "user is required")
}

// TestTheCatalogIsTheDefaultDirectory: a manager with no directory of its own reads
// connections from the sharding catalog.
func TestTheCatalogIsTheDefaultDirectory(t *testing.T) {
	pm, _ := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	require.IsType(t, &connectionStore{}, pm.connections())
}
