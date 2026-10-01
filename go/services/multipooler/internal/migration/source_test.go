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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectGrantSQL(t *testing.T) {
	// Empty role list yields no statements (the opt-in default).
	require.Nil(t, connectGrantSQL(false, "appdb", nil))
	require.Nil(t, connectGrantSQL(true, "appdb", []string{}))

	// REVOKE fences PUBLIC first (so a role relying only on PUBLIC's grant is still
	// cut), then each named role; identifiers are quoted per part.
	revoke := connectGrantSQL(false, "appdb", []string{"app", "reporting"})
	require.Equal(t, []string{
		"REVOKE CONNECT ON DATABASE appdb FROM PUBLIC",
		"REVOKE CONNECT ON DATABASE appdb FROM app",
		"REVOKE CONNECT ON DATABASE appdb FROM reporting",
	}, revoke)

	// GRANT restores PUBLIC and each named role symmetrically.
	grant := connectGrantSQL(true, "appdb", []string{"app"})
	require.Equal(t, []string{
		"GRANT CONNECT ON DATABASE appdb TO PUBLIC",
		"GRANT CONNECT ON DATABASE appdb TO app",
	}, grant)

	// Mixed-case / reserved database and role names are quoted.
	q := connectGrantSQL(false, "AppDB", []string{"User"})
	require.Equal(t, []string{
		`REVOKE CONNECT ON DATABASE "AppDB" FROM PUBLIC`,
		`REVOKE CONNECT ON DATABASE "AppDB" FROM "User"`,
	}, q)
}

func TestTerminateClientBackendsSQL(t *testing.T) {
	// The hard-quiesce write-cut must target only ordinary client backends, spare
	// the caller's own backend, and exclude the migrator's connections (parameter
	// $1). It must never touch walsenders/replication.
	require.Contains(t, terminateClientBackendsSQL, "pg_terminate_backend(pid)")
	require.Contains(t, terminateClientBackendsSQL, "backend_type = 'client backend'")
	require.Contains(t, terminateClientBackendsSQL, "pid <> pg_backend_pid()")
	require.Contains(t, terminateClientBackendsSQL, "application_name IS DISTINCT FROM $1")
	require.Contains(t, terminateClientBackendsSQL, "datname = current_database()")
	require.NotContains(t, strings.ToLower(terminateClientBackendsSQL), "walsender")
}
