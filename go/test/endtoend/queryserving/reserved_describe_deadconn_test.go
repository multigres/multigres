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

package queryserving

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestReservedDescribeDeadConnReturnsCleanError is the regression for MTD06
// "describe failed … broken pipe". When the backend behind a reserved connection is
// gone, a Describe must return a clean, retryable error — not an opaque MTD06
// broken-pipe — so the client reconnects (and recreates any temp replication slot)
// rather than seeing an internal failure. pg_terminate_backend makes Postgres itself
// send a real 57P01 admin_shutdown FATAL before the socket closes, which multipooler
// now preserves verbatim; a pure connection-level failure with no diagnostic at all
// (e.g. a hard crash) would instead surface the synthesized 08006 connection_failure.
func TestReservedDescribeDeadConnReturnsCleanError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// This is a gateway/pooler-specific path (reserved connections); run against the
	// multigateway target.
	var port int
	for _, target := range setup.GetComparisonTargets(t) {
		if target.Name == "multigateway" {
			port = target.Port
		}
	}
	require.NotZero(t, port, "multigateway target port")

	conn := connectLowLevelToPort(t, ctx, port)
	defer conn.Close()
	killer := connectLowLevelToPort(t, ctx, port)
	defer killer.Close()

	// Reserve the connection with a transaction and prepare a statement on its backend.
	_, err := conn.Query(ctx, "BEGIN")
	require.NoError(t, err)
	require.NoError(t, conn.Parse(ctx, "mtd06_d1", "SELECT $1::int", []uint32{23}))

	// Identify the reserved backend pid, then terminate it from another session.
	res, err := conn.Query(ctx, "SELECT pg_backend_pid()")
	require.NoError(t, err)
	require.NotEmpty(t, res)
	pid := string(res[0].Rows[0].Values[0])

	_, err = killer.Query(ctx, "SELECT pg_terminate_backend("+pid+")")
	require.NoError(t, err)

	// Let the socket fully close; the reserved conn stays held (no background health check).
	time.Sleep(1 * time.Second)

	// Describe on the now-dead reserved connection.
	_, err = conn.DescribePrepared(ctx, "mtd06_d1")
	require.Error(t, err, "describe on a dead reserved connection must error")

	msg := err.Error()
	assert.NotContains(t, msg, "MTD06", "must not be the opaque describe-failed code")
	assert.NotContains(t, msg, "broken pipe", "must not surface the raw write error")
	assert.True(t,
		strings.Contains(msg, "reserved connection terminated") ||
			strings.Contains(msg, "08006") ||
			strings.Contains(msg, "57P01") ||
			strings.Contains(msg, "administrator command"),
		"expected either Postgres's own admin_shutdown (57P01) preserved verbatim or the "+
			"synthesized connection_failure (08006), got: %s", msg)
}
