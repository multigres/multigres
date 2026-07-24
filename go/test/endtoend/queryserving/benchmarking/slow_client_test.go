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

package benchmarking

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// rssMB returns the resident set size of a process in MiB (0 if unavailable).
func rssMB(pid int) float64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return kb / 1024
}

// TestSlowClientBackpressure verifies that a single slow-reading client of a
// large result does not make the multigateway or multipooler buffer the whole
// result. The execution path streams in bounded batches and relies on
// backpressure (client TCP -> gRPC flow control -> Postgres) to hold the
// pipeline, so proxy memory should stay far below the total result size while
// the client drains slowly.
//
// Skipped unless RUN_BENCHMARKS=1.
func TestSlowClientBackpressure(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunBenchmarks)

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 90*time.Second)

	mgwPid := setup.Multigateway.Process.Process.Pid
	mpPid := setup.GetPrimary(t).Multipooler.Process.Process.Pid

	// Baseline (idle) RSS for reference.
	baseMgw, baseMp := rssMB(mgwPid), rssMB(mpPid)
	t.Logf("idle RSS: gateway=%.0fMB pooler=%.0fMB", baseMgw, baseMp)

	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// ~600 MB result (3,000,000 rows x ~200 bytes) streamed from Postgres.
	const totalMB = 600
	rows, err := conn.Query(ctx, "SELECT repeat('x', 200) FROM generate_series(1, 3000000)")
	require.NoError(t, err, "query start")
	defer rows.Close()

	// Read slowly: a small sleep every batch of rows throttles the client so the
	// proxy would have to buffer the rest if there were no backpressure. Sample
	// proxy RSS throughout and track the peak.
	var peakMgw, peakMp float64
	read := 0
	deadline := time.Now().Add(20 * time.Second)
	for rows.Next() && time.Now().Before(deadline) {
		read++
		if read%2000 == 0 {
			time.Sleep(15 * time.Millisecond) // throttle the drain
			if m := rssMB(mgwPid); m > peakMgw {
				peakMgw = m
			}
			if m := rssMB(mpPid); m > peakMp {
				peakMp = m
			}
		}
	}

	t.Logf("slow read: rows=%d of 3000000 (~%.0f MB of ~%d MB drained)", read, float64(read)*200/1e6, totalMB)
	t.Logf("peak RSS while client slow: gateway=%.0fMB pooler=%.0fMB", peakMgw, peakMp)
	t.Logf("growth over idle: gateway=+%.0fMB pooler=+%.0fMB", peakMgw-baseMgw, peakMp-baseMp)

	// Backpressure guard: the client drained hundreds of MB slowly, so if the
	// proxy buffered the whole result its RSS would grow by roughly totalMB.
	// A generous ceiling well below that (and well above one batch plus GC slack)
	// asserts the pipeline stays bounded instead.
	require.Greater(t, read, 100000, "test should have streamed a large result slowly")
	const boundMB = 250 // ~= a small multiple of the batch size + GC headroom, far below totalMB
	require.Less(t, peakMgw-baseMgw, float64(boundMB), "gateway buffered unboundedly (backpressure broken)")
	require.Less(t, peakMp-baseMp, float64(boundMB), "pooler buffered unboundedly (backpressure broken)")
}
