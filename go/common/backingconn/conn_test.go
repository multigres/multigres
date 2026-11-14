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
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func goodConn() *Conn {
	return &Conn{
		Name: "src", Hosts: []string{"db.example.com"}, Ports: []int{5432}, Database: "app", User: "alice",
		Password: "s3cret", SSLMode: "verify-full", SSLRootCert: "/ca.pem", SSLNegotiation: "direct",
	}
}

// valid is a connection Validate accepts, changed by mutate: complete, with every field a
// connection must have.
func valid(mutate func(*Conn)) *Conn {
	c := &Conn{Hosts: []string{"h"}, Ports: []int{5432}, Database: "d", User: "u", Password: "p", SSLMode: "prefer"}
	if mutate != nil {
		mutate(c)
	}
	return c
}

// TestDSNRoundTrips: the string a connection renders as is read back as the same connection,
// whatever the values hold, so what a libpq client connects with is what was validated.
func TestDSNRoundTrips(t *testing.T) {
	for name, conn := range map[string]*Conn{
		"every carried parameter": goodConn(),
		"hard values": {
			Hosts: []string{"db.example.com"}, Ports: []int{5432}, Database: `we ird'db\`, User: "al ice",
			Password: `p w'd\ and "quotes" and =`,
		},
		"lists": {Hosts: []string{"a", "2001:db8::1"}, Ports: []int{5432, 5433}, Database: "app", User: "u"},
		"parameters the pooler does not carry": {
			Hosts: []string{"h"}, Ports: []int{5432}, Database: "app", User: "u",
			Params: map[string]string{"sslcert": "/c.pem", "sslkey": "/k.pem", "connect_timeout": "5"},
		},
		"nothing but a host": {Hosts: []string{"h"}},
	} {
		t.Run(name, func(t *testing.T) {
			back, err := ParseDSN(conn.DSN())
			require.NoError(t, err)
			want := *conn
			want.Name = ""
			require.Equal(t, &want, back)
			require.Equal(t, conn.DSN(), back.DSN(), "rendering is deterministic")
		})
	}
}

func TestConnJSONRoundTrips(t *testing.T) {
	conn := goodConn()
	conn.Params = map[string]string{"sslcert": "/c.pem"}
	data, err := json.Marshal(conn)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"host": ["db.example.com"], "port": [5432], "dbname": "app", "user": "alice",
		"password": "s3cret", "sslmode": "verify-full", "sslrootcert": "/ca.pem",
		"sslnegotiation": "direct", "params": {"sslcert": "/c.pem"}}`, string(data))

	var back Conn
	require.NoError(t, json.Unmarshal(data, &back))
	want := *conn
	want.Name = ""
	require.Equal(t, want, back)

	// The name is a column of its own, and is kept when a value is read into a connection that has
	// one.
	named := Conn{Name: "src"}
	require.NoError(t, json.Unmarshal(data, &named))
	require.Equal(t, "src", named.Name)
}

// A stored connection is read as it was written or not at all: a field this version does not
// know could be one that makes the connection safer, and is never dropped.
func TestConnJSONIsReadStrictly(t *testing.T) {
	for name, tc := range map[string]struct {
		json   string
		reason string
	}{
		"an unknown field":        {`{"host":["h"],"future":1}`, "field this version does not know"},
		"a field of a wrong type": {`{"host":"h"}`, `field "host" has the wrong type`},
		"not an object":           {`[1]`, "wrong type"},
		"not JSON":                {`{"host":`, "unexpected end of JSON input"},
		"data after the object":   {`{"host":["h"]} {}`, "after top-level value"},
	} {
		t.Run(name, func(t *testing.T) {
			var c Conn
			err := json.Unmarshal([]byte(tc.json), &c)
			require.ErrorContains(t, err, tc.reason)
		})
	}

	// An error never quotes the input, which holds the password.
	var c Conn
	err := json.Unmarshal([]byte(`{"password":"s3cret","host":"h"}`), &c)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "s3cret")
	err = json.Unmarshal([]byte(`{"password":"s3cret","future":1}`), &c)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "s3cret")
}

// The password is never printed by formatting a connection, however it is formatted.
func TestConnNeverPrintsItsPassword(t *testing.T) {
	conn := goodConn()
	conn.Params = map[string]string{"sslpassword": "keysecret"}
	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("connection", "conn", conn, "value", *conn)
	for _, out := range []string{
		fmt.Sprintf("%v", conn), fmt.Sprintf("%+v", conn), fmt.Sprintf("%#v", conn),
		fmt.Sprintf("%v", *conn), fmt.Sprintf("conn=%s", conn), fmt.Sprint(conn), logged.String(),
	} {
		require.NotContains(t, out, "s3cret")
		require.NotContains(t, out, "keysecret", "the value of a parameter is not printed, only its name")
		require.Contains(t, out, "db.example.com")
	}
	require.Contains(t, fmt.Sprint(conn), "sslpassword")
	require.Contains(t, fmt.Sprint(conn), "<redacted>")
}

func TestConnValidate(t *testing.T) {
	require.NoError(t, goodConn().Validate())
	require.NoError(t, valid(nil).Validate())
	require.NoError(t, valid(func(c *Conn) { c.Hosts, c.Ports = []string{"a", "b"}, []int{1, 2} }).Validate(), "a list is a list")
	require.NoError(t, valid(func(c *Conn) { c.Hosts = []string{"a", "b"} }).Validate())

	for name, tc := range map[string]struct {
		mutate func(*Conn)
		reason string
	}{
		"no host":                  {func(c *Conn) { c.Hosts = nil }, "host is required"},
		"an empty host":            {func(c *Conn) { c.Hosts = []string{"a", ""} }, "empty entry"},
		"a blank host":             {func(c *Conn) { c.Hosts = []string{" "} }, "empty entry"},
		"a host with a space":      {func(c *Conn) { c.Hosts = []string{"a b"} }, "hostname"},
		"a host with an @":         {func(c *Conn) { c.Hosts = []string{"u@h"} }, "hostname"},
		"a host list in one entry": {func(c *Conn) { c.Hosts = []string{"a,b"} }, "hostname"},
		"a socket directory":       {func(c *Conn) { c.Hosts = []string{"/var/run/postgresql"} }, "socket directory"},
		"a host with a path":       {func(c *Conn) { c.Hosts = []string{"db.example.com/x"} }, "single hostname"},
		"no port":                  {func(c *Conn) { c.Ports = nil }, "port is required"},
		"port 0":                   {func(c *Conn) { c.Ports = []int{0} }, "between 1 and 65535"},
		"port too large":           {func(c *Conn) { c.Ports = []int{65536} }, "between 1 and 65535"},
		"ports that do not match the hosts": {
			func(c *Conn) { c.Hosts, c.Ports = []string{"a", "b", "c"}, []int{1, 2} }, "one for each host",
		},
		"no user":                     {func(c *Conn) { c.User = "" }, "user is required"},
		"no database":                 {func(c *Conn) { c.Database = "" }, "database is required"},
		"no password":                 {func(c *Conn) { c.Password = "" }, "password is required"},
		"no sslmode":                  {func(c *Conn) { c.SSLMode = "" }, "sslmode is required"},
		"sslmode":                     {func(c *Conn) { c.SSLMode = "bogus" }, "sslmode is not valid"},
		"sslmode in caps":             {func(c *Conn) { c.SSLMode = "DISABLE" }, "sslmode is not valid"},
		"sslmode with a space":        {func(c *Conn) { c.SSLMode = " require" }, "sslmode is not valid"},
		"sslnegotiation":              {func(c *Conn) { c.SSLNegotiation = "bogus" }, "sslnegotiation is not valid"},
		"sslnegotiation in caps":      {func(c *Conn) { c.SSLNegotiation = "DIRECT" }, "sslnegotiation is not valid"},
		"sslnegotiation with a space": {func(c *Conn) { c.SSLNegotiation = "direct " }, "sslnegotiation is not valid"},
		"a parameter that is not libpq's": {
			func(c *Conn) { c.Params = map[string]string{"nope": "x"} }, "unrecognized",
		},
		"a carried parameter in params": {
			func(c *Conn) { c.Params = map[string]string{"host": "x"} }, "unrecognized",
		},
		"a client certificate":        {func(c *Conn) { c.Params = map[string]string{"sslcert": "/c.pem"} }, "parameter sslcert is not supported: a connection carries only "},
		"a client key":                {func(c *Conn) { c.Params = map[string]string{"sslkey": "/k.pem"} }, "parameter sslkey is not supported"},
		"a client key password":       {func(c *Conn) { c.Params = map[string]string{"sslpassword": "x"} }, "parameter sslpassword is not supported"},
		"a revocation list":           {func(c *Conn) { c.Params = map[string]string{"sslcrl": "/l.pem"} }, "parameter sslcrl is not supported"},
		"a revocation list directory": {func(c *Conn) { c.Params = map[string]string{"sslcrldir": "/d"} }, "parameter sslcrldir is not supported"},
		"a password file":             {func(c *Conn) { c.Params = map[string]string{"passfile": "/p"} }, "parameter passfile is not supported"},
		"a service":                   {func(c *Conn) { c.Params = map[string]string{"service": "s"} }, "parameter service is not supported"},
		"a service file":              {func(c *Conn) { c.Params = map[string]string{"servicefile": "/s"} }, "parameter servicefile is not supported"},
		"a key log":                   {func(c *Conn) { c.Params = map[string]string{"sslkeylogfile": "/k.log"} }, "parameter sslkeylogfile is not supported"},
		// The rules between settings belong to the client package (ValidateSSLRootCert,
		// ValidateSSLNegotiation): one case each.
		"system roots, a weaker sslmode": {func(c *Conn) { c.SSLRootCert, c.SSLMode = "system", "require" }, `weak sslmode "require" may not be used with sslrootcert=system`},
		"direct negotiation, prefer":     {func(c *Conn) { c.SSLNegotiation, c.SSLMode = "direct", "prefer" }, `weak sslmode "prefer" may not be used with sslnegotiation=direct`},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, valid(tc.mutate).Validate(), tc.reason)
		})
	}
	for _, mode := range []string{"disable", "prefer", "require", "verify-ca"} {
		require.NoError(t, valid(func(c *Conn) {
			c.SSLMode = mode
			if mode == "verify-ca" {
				c.SSLRootCert = "/ca.pem"
			}
		}).Validate(), mode)
	}
	for _, neg := range []string{"postgres", "direct"} {
		require.NoError(t, valid(func(c *Conn) { c.SSLNegotiation, c.SSLMode = neg, "require" }).Validate(), neg)
	}
}

// Complete is the one place a connection gets what it leaves out: the port, the database and the
// sslmode, and only those. The sslmode is require, not libpq's prefer.
func TestComplete(t *testing.T) {
	c := &Conn{Hosts: []string{"h"}, Database: "d", User: "u", Password: "p"}
	c.Complete()
	require.Equal(t, []int{5432}, c.Ports)
	require.Equal(t, "require", c.SSLMode)
	require.Equal(t, "d", c.Database, "what is given is kept")

	c = &Conn{Hosts: []string{"h"}, SSLRootCert: "system"}
	c.Complete()
	require.Equal(t, "verify-full", c.SSLMode, "libpq's default with system roots")
	require.Equal(t, "postgres", c.Database)
	require.Empty(t, c.User, "nothing is decided for the user")
	require.Empty(t, c.Password)

	c = &Conn{Hosts: []string{"a", "b"}, Ports: []int{1, 2}, SSLMode: "require"}
	c.Complete()
	require.Equal(t, []int{1, 2}, c.Ports, "what is given is kept")
	require.Equal(t, "require", c.SSLMode)
}

// FromDSN is how a connection is read when it is created: a string that leaves out what a
// client would take from its environment is completed or refused, and a string libpq reads
// differently is refused.
func TestFromDSN(t *testing.T) {
	conn, err := FromDSN("host=h dbname=d user=u password=p")
	require.NoError(t, err)
	require.Equal(t, valid(func(c *Conn) { c.SSLMode = "require" }), conn)

	// The database has a default.
	conn, err = FromDSN("host=h user=u password=p")
	require.NoError(t, err)
	require.Equal(t, "postgres", conn.Database)

	for name, dsn := range map[string]string{
		"no host":     "port=5432 dbname=d user=u password=p",
		"no user":     "host=h dbname=d password=p",
		"no password": "host=h dbname=d user=u",
		"not a dsn":   "nope",
	} {
		_, err := FromDSN(dsn)
		require.Error(t, err, name)
	}
}

// TestForPooler: what a pooler asks beyond Validate, one case each, and one connection that fails
// through Validate.
func TestForPooler(t *testing.T) {
	require.NoError(t, goodConn().ForPooler())
	require.NoError(t, valid(func(c *Conn) { c.Hosts = []string{"2001:db8::1"} }).ForPooler(), "an IP address")

	for name, tc := range map[string]struct {
		mutate func(*Conn)
		reason string
	}{
		"a parameter the pooler does not carry": {
			func(c *Conn) { c.Params = map[string]string{"connect_timeout": "5"} }, "parameter connect_timeout is not supported; a backing connection carries only",
		},
		"two servers": {func(c *Conn) { c.Hosts, c.Ports = []string{"a", "b"}, []int{1, 2} }, "more than one server"},
		"not valid":   {func(c *Conn) { c.Password = "" }, "password is required"},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, valid(tc.mutate).ForPooler(), tc.reason)
		})
	}

	// The parameters it refuses are named by the one that sorts first, never by text of the string.
	err := valid(func(c *Conn) { c.Params = map[string]string{"connect_timeout": "5", "application_name": "x"} }).ForPooler()
	require.ErrorContains(t, err, "parameter application_name is not supported")
}

func TestCloneSharesNothing(t *testing.T) {
	conn := goodConn()
	conn.Params = map[string]string{"sslcert": "/c.pem"}
	clone := conn.Clone()
	require.Equal(t, conn, clone)
	clone.Hosts[0], clone.Ports[0], clone.Params["sslcert"] = "x", 1, "/other"
	require.Equal(t, goodConn().Hosts, conn.Hosts)
	require.Equal(t, goodConn().Ports, conn.Ports)
	require.Equal(t, "/c.pem", conn.Params["sslcert"])
	require.Nil(t, (&Conn{}).Clone().Params, "nothing becomes something")
}

// sslrootcert=system, as in libpq 16: the system trust store, with verify-full the only sslmode it
// allows.
func TestSystemRootCert(t *testing.T) {
	system := valid(func(c *Conn) { c.SSLRootCert, c.SSLMode = "system", "verify-full" })
	require.NoError(t, system.Validate())
	require.NoError(t, system.ForPooler())
	// "system" in any other case is a path to a file named that.
	require.NoError(t, valid(func(c *Conn) { c.SSLRootCert, c.SSLMode = "System", "require" }).Validate())

	// Both forms of a string can say it.
	for _, dsn := range []string{
		"host=h port=5432 dbname=d user=u password=p sslrootcert=system",
		"postgres://u:p@h:5432/d?sslrootcert=system&sslmode=verify-full",
	} {
		conn, err := FromDSN(dsn)
		require.NoError(t, err)
		require.Equal(t, "system", conn.SSLRootCert)
	}
}

func TestHostAndPortOfOneServer(t *testing.T) {
	c := goodConn()
	require.Equal(t, "db.example.com", c.Host())
	require.Equal(t, 5432, c.Port())
}

func TestProtoRoundTrip(t *testing.T) {
	conn := goodConn()
	p := conn.ToProto()
	require.Equal(t, "src", p.GetName())
	require.Equal(t, []string{"db.example.com"}, p.GetHosts())
	require.Equal(t, []int32{5432}, p.GetPorts())
	require.Equal(t, "app", p.GetDatabase())
	require.Equal(t, "alice", p.GetUser())
	require.Equal(t, "s3cret", p.GetPassword())
	require.Equal(t, "verify-full", p.GetSslMode())
	require.Equal(t, "/ca.pem", p.GetSslRootCert())
	require.Equal(t, "direct", p.GetSslNegotiation())
	require.Equal(t, conn, FromProto(p))

	// A reply with nothing is a connection with nothing; ForPooler is what refuses it.
	empty := FromProto(nil)
	require.Equal(t, &Conn{}, empty)
	require.Error(t, empty.ForPooler())
}
