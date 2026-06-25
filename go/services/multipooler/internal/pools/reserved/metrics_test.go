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

package reserved

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// outcomeCounts collects the mg.pooler.txn.outcomes counter into outcome->count.
func outcomeCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name != "mg.pooler.txn.outcomes" {
				continue
			}
			sum, ok := mm.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				v, _ := dp.Attributes.Value(attribute.Key("outcome"))
				counts[v.AsString()] += dp.Value
			}
		}
	}
	return counts
}

// TestTxnMetrics_Outcomes drives a commit, a rollback, and an error-release
// (abort) through real reserved connections and verifies each outcome is
// counted exactly once.
func TestTxnMetrics_Outcomes(t *testing.T) {
	// Telemetry must be installed before NewPool so the pool's txnMetrics bind
	// to the test reader's meter provider.
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	reader := setup.MetricReader

	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()
	ctx := context.Background()

	// Commit.
	c1, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c1.Begin(ctx))
	require.NoError(t, c1.Commit(ctx))
	c1.Release(ReleaseCommit, nil)

	// Rollback.
	c2, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c2.Begin(ctx))
	require.NoError(t, c2.Rollback(ctx))
	c2.Release(ReleaseRollback, nil)

	// Abort: begin then release with an error reason, without commit/rollback.
	c3, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c3.Begin(ctx))
	c3.Release(ReleaseError, nil)

	counts := outcomeCounts(t, reader)
	assert.Equal(t, int64(1), counts[txnOutcomeCommit], "one commit expected")
	assert.Equal(t, int64(1), counts[txnOutcomeRollback], "one rollback expected")
	assert.Equal(t, int64(1), counts[txnOutcomeAbort], "one abort expected")
}

// TestTxnMetrics_CleanReleaseNoAbort verifies that releasing a connection that
// committed (transaction reason already removed) does not record an abort.
func TestTxnMetrics_CleanReleaseNoAbort(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	reader := setup.MetricReader

	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()
	ctx := context.Background()

	c, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c.Begin(ctx))
	require.NoError(t, c.Commit(ctx))
	// Even an error-flavored release must not double-count as abort: the commit
	// already removed the transaction reason.
	c.Release(ReleaseError, nil)

	counts := outcomeCounts(t, reader)
	assert.Equal(t, int64(1), counts[txnOutcomeCommit])
	assert.Equal(t, int64(0), counts[txnOutcomeAbort], "commit then release must not record an abort")
}

// TestTxnMetrics_RecordEdgeCases covers the nil-receiver guard and the
// zero-duration branch of record (counter increments, duration is skipped).
func TestTxnMetrics_RecordEdgeCases(t *testing.T) {
	// Nil receiver must be a no-op, not a panic.
	var nilM *txnMetrics
	nilM.record(context.Background(), txnOutcomeCommit, time.Second)

	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m := newTxnMetrics()
	// d <= 0: the outcome counter still increments, but no duration is recorded.
	m.record(t.Context(), txnOutcomeRollback, 0)

	counts := outcomeCounts(t, setup.MetricReader)
	assert.Equal(t, int64(1), counts[txnOutcomeRollback])
}

// TestRecordTxnOutcome_NilPool covers the no-owning-pool guard in
// Conn.recordTxnOutcome (e.g. unit-test fixtures with no pool).
func TestRecordTxnOutcome_NilPool(t *testing.T) {
	c := &Conn{} // pool is nil
	c.recordTxnOutcome(context.Background(), txnOutcomeCommit)
}
