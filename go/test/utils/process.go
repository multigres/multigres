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

package utils

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/executil"
)

// BaseTestEnv returns a copy of os.Environ() safe for use in pgctld and multipooler
// test processes. It strips variables that the Supabase Postgres container exports
// and that would corrupt the test cluster setup:
//
//   - POSTGRES_USER=supabase_admin — causes initdb to create a "supabase_admin"
//     superuser instead of "postgres", so "postgres" gets no SCRAM verifier and all
//     test connections (which authenticate as "postgres") fail with "password
//     authentication failed".
//   - POSTGRES_INITDB_SQL_FILES — points at Supabase migration SQL that assumes a
//     pre-existing "supabase_admin" role; running it under the test user breaks
//     initdb post-setup.
//   - POSTGRES_INITDB_SQL_DIRS — same concern as POSTGRES_INITDB_SQL_FILES.
//
// POSTGRES_INITDB_ARGS is intentionally NOT stripped. The Supabase image sets it to
// "--locale-provider=icu --icu-locale=en_US.UTF-8 --allow-group-access". The ICU
// locale flags are necessary: the Supabase Postgres binary requires ICU locale data
// for startup, and that data is available via LOCALE_ARCHIVE inside the container.
// Stripping these flags causes Postgres to start but crash immediately (before
// multipooler can connect), because the binary cannot fall back to a glibc-only
// locale configuration.  On a developer's macOS machine POSTGRES_INITDB_ARGS is
// typically absent, so leaving it unstripped is a no-op there.
//
// After stripping, POSTGRES_USER=postgres is appended so initdb always creates
// "postgres" as the superuser — the role every test connection uses.
func BaseTestEnv() []string {
	strip := map[string]bool{
		constants.PgUserEnvVar:           true,
		constants.PgInitdbSQLFilesEnvVar: true,
		constants.PgInitdbSQLDirsEnvVar:  true,
	}
	base := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if !strip[key] {
			base = append(base, kv)
		}
	}
	base = append(base, constants.PgUserEnvVar+"="+constants.DefaultPostgresUser)
	return base
}

// CommandWithOrphanProtection creates a command wrapped in run_in_test.sh for orphan
// process protection. The process uses context.Background() for its lifetime, avoiding
// premature SIGKILL when the monitoring context is cancelled. Instead, Stop() is called
// when monitorCtx is cancelled, giving run_in_test.sh time to send SIGTERM to its child
// process before exiting.
//
// Callers should set any required environment variables (e.g. MULTIGRES_TESTDATA_DIR)
// on the returned Cmd before calling Start().
func CommandWithOrphanProtection(monitorCtx context.Context, name string, args ...string) *executil.Cmd {
	allArgs := append([]string{name}, args...)
	cmd := executil.Command(context.Background(), "run_in_test.sh", allArgs...)
	go func() {
		<-monitorCtx.Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = cmd.Stop(stopCtx)
		cancel()
	}()
	return cmd
}
