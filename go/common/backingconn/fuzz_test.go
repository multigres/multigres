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
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/pgutil"
)

// hostileEnvironment is every variable pgx reads for a parameter that a connection's string decides
// or pins (a carried, secret or internal one, or a service file), set to a value that would change
// what is dialed, or fail it, if it were read. The operational variables (PGAPPNAME,
// PGCONNECT_TIMEOUT, PGSSLSNI, PGTARGETSESSIONATTRS, PGTZ, the protocol versions) are left out:
// they are the process's to set.
var hostileEnvironment = map[string]string{
	"PGHOST": "env.invalid", "PGPORT": "1", "PGDATABASE": "envdb", "PGUSER": "envuser", "PGPASSWORD": "envpw",
	"PGPASSFILE": "/nonexistent/passfile", "PGSSLMODE": "disable", "PGSSLKEY": "/nonexistent/key",
	"PGSSLCERT": "/nonexistent/cert", "PGSSLROOTCERT": "/nonexistent/root", "PGSSLPASSWORD": "x",
	"PGSSLNEGOTIATION": "direct", "PGSERVICEFILE": "/nonexistent/services", "PGOPTIONS": "-c search_path=evil",
	"PGCHANNELBINDING": "require", "PGREQUIREAUTH": "gss",
}

// FuzzConnection: a string that is accepted is a connection that reads back as itself in every
// form it is kept in or rendered as, and that pgx, in the worst environment, reads as what was
// validated.
func FuzzConnection(f *testing.F) {
	for _, e := range readCorpus(f) {
		f.Add(withCredentials(e.DSN))
	}
	f.Add("postgres://u:p%40w@h1:5432,h2:5433/app?sslmode=require")
	f.Add("host=/var/run/postgresql dbname=app user=u password=p")
	f.Add("host=h dbname=app user=u password=p sslrootcert=system")

	f.Fuzz(func(t *testing.T, dsn string) {
		conn, err := FromDSN(dsn)
		if err != nil {
			return
		}
		require.NoError(t, conn.Validate())

		// parse -> render -> parse is the same connection.
		back, err := FromDSN(conn.DSN())
		require.NoError(t, err, "what is stored must be accepted again")
		require.Equal(t, conn, back)
		require.Equal(t, conn.DSN(), back.DSN())

		// And so is the JSON it is stored as.
		data, err := json.Marshal(conn)
		require.NoError(t, err)
		var stored Conn
		require.NoError(t, json.Unmarshal(data, &stored))
		require.NoError(t, stored.Validate())
		require.Equal(t, conn, &stored)

		// The string clients are dialed with says what the connection says, and nothing else: no
		// value of the connection adds or changes a parameter of the string.
		rendered := conn.LibpqString()
		kv := pgutil.ParseConnInfo(rendered)
		require.Equal(t, strings.Join(conn.Hosts, ","), kv["host"])
		require.Equal(t, joinPorts(conn.Ports), kv["port"])
		require.Equal(t, conn.User, kv["user"])
		require.Equal(t, conn.Database, kv["dbname"])
		require.Equal(t, conn.Password, kv["password"])
		require.Equal(t, conn.SSLMode, kv["sslmode"])
		for key := range kv {
			require.True(t, IsParam(key), "%s is a libpq parameter", key)
		}

		// pgx with every variable it reads set to something hostile agrees on what is dialed.
		for k, v := range hostileEnvironment {
			t.Setenv(k, v)
		}
		cfg, err := pgx.ParseConfig(rendered)
		if err != nil {
			// The only thing pgx reads that a string can fail on is a file: a root certificate that
			// is not there.
			require.NotEmpty(t, conn.SSLRootCert, "pgx refused a connection that is valid: %v", err)
			require.NotEqual(t, "system", conn.SSLRootCert)
			return
		}
		require.Equal(t, conn.Hosts[0], cfg.Host)
		require.EqualValues(t, conn.Ports[0], cfg.Port)
		require.Equal(t, conn.User, cfg.User)
		require.Equal(t, conn.Database, cfg.Database)
		require.Equal(t, conn.Password, cfg.Password)
		require.Equal(t, 1+len(cfg.Fallbacks) >= len(conn.Hosts), true)
		if conn.SSLMode == "disable" {
			require.Nil(t, cfg.TLSConfig)
		} else {
			require.NotNil(t, cfg.TLSConfig, "sslmode=%s", conn.SSLMode)
		}
		require.Empty(t, cfg.RuntimeParams["options"])
		require.Equal(t, conn.Params["application_name"], cfg.RuntimeParams["application_name"])
		// What pgx does not parse it sends the server as a setting, so every key it leaves there is a
		// parameter of the table, which the migrator drops (dialConfig).
		for key := range cfg.RuntimeParams {
			require.True(t, IsParam(key), "pgx would send the server a setting called %s", key)
		}
	})
}

func joinPorts(ports []int) string {
	s := make([]string, len(ports))
	for i, p := range ports {
		s[i] = strconv.Itoa(p)
	}
	return strings.Join(s, ",")
}
