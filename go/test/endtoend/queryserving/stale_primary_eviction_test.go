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
	"context"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestGateway_EvictsStrandedPrimaryOnStaleHealthStream reproduces the
// stranded-gateway stale-primary bug and asserts the fix: when a primary
// pooler's health stream to the gateway goes stale, the gateway must
// retract that pooler's PRIMARY routing claim instead of keeping it forever and
// routing writes (and CONSISTENT reads) to a superseded primary.
//
// Scenario:
//  1. 3-node cluster with a gateway. Poolers advertise a short health-stream
//     staleness window (2s) so the gateway marks a frozen pooler stale quickly
//     instead of waiting the ~90s default; this keeps the test fast and is why
//     it is gated behind testing.Short().
//  2. A baseline write and read through the gateway confirm traffic routes to
//     the primary (A). On the main gateway port every query is leader-routed
//     (WRITABLE), so a plain SELECT is a leader read, not a replica read.
//  3. Freeze A's multipooler with SIGSTOP. A's postgres keeps running, so A is a
//     write-capable "stranded" primary whose gateway health stream now stalls.
//     Crucially there is no voluntary demotion (a frozen A cannot report role
//     != PRIMARY) and no topology OnGone — the exact state that used to leave
//     the gateway routing to a superseded primary. setHealthError preserves A's
//     cached PRIMARY RoutingState while recording LastError.
//  4. Assert the gateway logged "routing primary retracted" for A because its
//     stream went stale. This line is emitted only by the fixed
//     onPoolerHealthUpdate (the else branch is reached only when LastError != nil,
//     since A's cached role stays PRIMARY). Pre-fix the claim was re-affirmed and
//     the line never appeared — a genuine fixed-vs-broken signal.
//  5. Assert both a write and a leader-routed read now fail fast with "no
//     writable primary" instead of hanging on — or reading stale from — the
//     stranded primary. This covers both halves of the bug ("stale reads and hung
//     writes"). Pre-fix the gateway keeps A's claim and routes the query to the
//     frozen A, which blocks until the client deadline.
//
// This deliberately does NOT run a live multiorch failover. A normal failover
// demotes the old primary (it reports role REPLICA and the gateway clears the
// claim through the ordinary path — no bug), and freezing the live primary to
// keep its stale PRIMARY claim also blocks multiorch's own (~90s) frozen-node
// detection, so an in-window election is not reliably reproducible. The fix under
// test is entirely gateway-side: it retracts a stranded primary's claim purely
// because the stream went stale, independent of whether a replacement is elected.
// Stranding the primary reproduces exactly that path. The pure
// asymmetric-partition variant (gateway still reaches the old primary, its stream
// never stalls) is out of scope here — it needs a pooler-side self-demoting lease
// and a transport-level partition primitive the harness does not have.
func TestGateway_EvictsStrandedPrimaryOnStaleHealthStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stranded-primary eviction test in short mode (uses real postgres and a timed staleness window)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping stranded-primary eviction test")
	}

	// No persistent multiorch: NewIsolated bootstraps the initial primary with a
	// throwaway temp-multiorch that is torn down before the test body runs, so the
	// stranded primary has nothing racing to replace it. Buffering is left at its
	// default (off) so the "no writable primary" error surfaces to the client.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultigateway(),
		// Shrink the staleness window the poolers advertise to the gateway so a
		// frozen pooler is detected in ~2s instead of the ~90s default.
		shardsetup.WithMultipoolerExtraArgs("--health-stream-staleness-timeout=2s"),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary should exist after bootstrap")
	primaryName := setup.PrimaryName
	t.Logf("Primary: %s", primaryName)

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()

	// Baseline schema + write + read. On the main gateway port every query is
	// leader-routed (WRITABLE) — including SELECTs — so both succeeding here
	// confirms the gateway routes reads and writes to the current primary.
	baseCtx, baseCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer baseCancel()
	_, err := gatewayDB.ExecContext(baseCtx, "CREATE TABLE IF NOT EXISTS stranded_test (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err, "baseline CREATE TABLE through gateway should succeed")
	_, err = gatewayDB.ExecContext(baseCtx, "INSERT INTO stranded_test (id, val) VALUES (1, 'before-freeze')")
	require.NoError(t, err, "baseline INSERT through gateway should route to the primary")
	var baselineCount int
	require.NoError(t, gatewayDB.QueryRowContext(baseCtx, "SELECT COUNT(*) FROM stranded_test").Scan(&baselineCount),
		"baseline SELECT through gateway should route to the primary")
	require.Equal(t, 1, baselineCount, "baseline read should see the row just written")

	// Freeze the primary's multipooler. Its postgres stays up (stranded,
	// write-capable primary) while its gateway health stream stalls.
	resume := setup.FreezeMultipooler(t, primaryName)
	defer resume()

	// Fix assertion: the gateway retracts the frozen primary's routing claim
	// because its health stream went stale. The pooler id embeds the node name,
	// so matching both substrings pins the eviction to the frozen node.
	line := shardsetup.WaitForLogLine(t, setup.Multigateway.LogFile, 15*time.Second,
		"routing primary retracted", primaryName)
	require.Contains(t, line, "stale_stream", "retraction should be attributed to a stale stream")
	t.Logf("Gateway evicted stranded primary %s: %s", primaryName, line)

	// Behavioral assertion: with the stranded primary evicted and no replacement,
	// WRITABLE traffic fails fast with "no writable primary" rather than hanging on
	// the frozen primary. Bounded per-attempt context; Eventually rides out the
	// brief window before the claim is fully retracted.
	require.Eventually(t, func() bool {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, err := gatewayDB.ExecContext(ctx, "INSERT INTO stranded_test (id, val) VALUES (2, 'after-freeze')")
		if err == nil {
			t.Log("write unexpectedly succeeded against a stranded primary")
			return false
		}
		if !strings.Contains(err.Error(), "no writable primary") {
			t.Logf("write failed but not yet with the eviction error: %v", err)
			return false
		}
		return true
	}, utils.ScaleTimeout(30*time.Second), 500*time.Millisecond,
		"WRITABLE writes should be rejected with 'no writable primary' after eviction, not hang on the stranded primary")

	// Same for reads: a leader-routed SELECT must also fail fast with "no writable
	// primary" rather than hang on — or serve a stale read from — the stranded
	// primary. This is the read half of the bug ("stale reads and hung writes").
	// By now the claim is retracted, so a single bounded attempt is deterministic.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	var count int
	err = gatewayDB.QueryRowContext(readCtx, "SELECT COUNT(*) FROM stranded_test").Scan(&count)
	require.Error(t, err, "a leader-routed read must not hang on or read stale from the stranded primary")
	require.Contains(t, err.Error(), "no writable primary",
		"leader-routed reads should be rejected with 'no writable primary' after eviction")

	// Thaw the primary before teardown so cleanup can stop it gracefully.
	resume()
}
