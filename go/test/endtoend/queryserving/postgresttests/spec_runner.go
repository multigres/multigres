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

package postgresttests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// specTarget is a database endpoint (as seen from the host) that the PostgREST
// spec suite should connect to. The spec runs inside a container, so the runner
// rewrites Host to host.docker.internal (which resolves to the host loopback on
// both OrbStack and Linux Docker via --add-host) and keeps the same Port.
type specTarget struct {
	Name string // "direct" or "gateway", for logging/reporting
	Port int    // host TCP port (standalone PG port, or multigateway pg-port)
}

// runSpec runs the PostgREST spec suite (from specImage) against target and
// returns the parsed result. match/skip scope the run via hspec's -m/--skip
// (empty match runs everything). The suite connects as the authenticator over
// TCP; fixtures must already be loaded into the target database.
func runSpec(t *testing.T, ctx context.Context, target specTarget, match string) (*specResult, error) {
	t.Helper()

	args := []string{
		"run", "--rm",
		// Resolve host.docker.internal to the host on Linux Docker too; on
		// OrbStack/macOS it already maps to the host loopback.
		"--add-host=host.docker.internal:host-gateway",
		"-e", "PGHOST=host.docker.internal",
		"-e", "PGPORT=" + strconv.Itoa(target.Port),
		"-e", "PGUSER=" + authenticatorRole,
		"-e", "PGPASSWORD=" + authenticatorPassword,
		"-e", "PGDATABASE=postgres",
		"-e", "PGSSLMODE=disable",
		"-e", "PGRST_DB_SCHEMAS=" + fixtureSchema,
		"-e", "PGTZ=utc",
		// search_path for the test harness's own setup queries (queryPgVersion /
		// schema-cache load) that run outside PostgREST's per-request transaction.
		// The server timezone is fixed to UTC at the database level in loadFixtures
		// (PostgREST resets the session to the DB default per request, so a session
		// PGOPTIONS timezone would not stick).
		"-e", "PGOPTIONS=-c search_path=public," + fixtureSchema,
		specImage,
		// failed-examples prints each failure's full description path plus the
		// summary line, which is all the divergence report needs.
		"--format=failed-examples",
	}
	if match != "" {
		args = append(args, "--match", match)
	}

	t.Logf("Running spec suite against %s (host.docker.internal:%d, match=%q)...", target.Name, target.Port, match)
	var outBuf bytes.Buffer
	cmd := executil.Command(ctx, "docker", args...)
	// Stream to the test log and capture for parsing.
	cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &outBuf)
	cmd.SetWaitDelay(10 * time.Second)

	runErr := cmd.Run()

	res := parseHspecOutput(outBuf.String())
	res.Target = target.Name
	// A non-zero exit is expected when specs fail; only treat it as a harness
	// error when we could not parse any examples (e.g. the suite crashed at
	// startup — a connection/introspection failure through the gateway).
	if res.Total == 0 {
		return res, fmt.Errorf("spec suite produced no results against %s: %w", target.Name, runErr)
	}
	return res, nil
}
