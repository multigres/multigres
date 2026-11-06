// Copyright 2025 Supabase, Inc.
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

package heartbeat

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReaderReadHeartbeat tests that reading a heartbeat sets the appropriate
// fields on the object.
func TestReaderReadHeartbeat(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	quorumCommitTs := now.Add(-11 * time.Second)
	// Add query result for heartbeat read
	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn", "quorum_commit_ts"},
		[][]any{{now.Add(-10 * time.Second).UnixNano(), "0/16E5D38", "0/16E5D00", quorumCommitTs.UTC().Format("2006-01-02 15:04:05.999999-07")}},
	))

	tr.readHeartbeat(t.Context())
	lag, err := tr.Status()

	require.NoError(t, err)
	expectedLag := 10 * time.Second
	assert.Equal(t, expectedLag, lag, "wrong latest lag")
	assert.EqualValues(t, 1, tr.Reads(), "wrong read count")
	assert.EqualValues(t, 0, tr.ReadErrors(), "wrong read error count")

	// The same read also observes pg_last_wal_receive_lsn and stamps its advance time.
	advanceAt, ok := tr.LastReceiveLSNAdvance()
	require.True(t, ok, "should have observed a receive LSN")
	assert.Equal(t, now, advanceAt, "advance time should be the read time")

	// quorum_commit_lsn/quorum_commit_ts are reported as-is, not advance-tracked.
	quorumLSN, ok := tr.QuorumCommitLSN()
	require.True(t, ok, "should have observed a quorum commit LSN")
	assert.Equal(t, "0/16E5D00", quorumLSN.String())

	commitTs, ok := tr.QuorumCommitTs()
	require.True(t, ok, "should have observed a quorum commit ts")
	// timestamptz round-trips through Postgres text at microsecond precision,
	// so truncate the expected value before comparing.
	assert.True(t, quorumCommitTs.Truncate(time.Microsecond).Equal(commitTs),
		"expected %v, got %v", quorumCommitTs, commitTs)
}

// TestReaderTracksReceiveLSNAdvance verifies the WAL-receive progress tracking:
// the first observation stamps the advance time, a genuine LSN increase bumps it,
// and an unchanged LSN (idle keepalives) leaves it in place.
func TestReaderTracksReceiveLSNAdvance(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	clock := time.Now()
	tr.now = func() time.Time { return clock }

	const pattern = "SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*"
	addRead := func(lsn string) {
		queryService.AddQueryPatternOnce(pattern, mock.MakeQueryResult(
			[]string{"ts", "receive_lsn"},
			[][]any{{clock.UnixNano(), lsn}},
		))
	}

	// First observation stamps the advance time.
	addRead("0/100")
	tr.readHeartbeat(t.Context())
	at1, ok := tr.LastReceiveLSNAdvance()
	require.True(t, ok)
	assert.Equal(t, clock, at1)

	// A genuine increase bumps the advance time.
	clock = clock.Add(1 * time.Second)
	addRead("0/200")
	tr.readHeartbeat(t.Context())
	at2, ok := tr.LastReceiveLSNAdvance()
	require.True(t, ok)
	assert.Equal(t, clock, at2)
	assert.True(t, at2.After(at1), "advance time should move forward on an LSN increase")

	// No increase leaves the advance time unchanged (idle keepalives keep the
	// connection alive but produce no new WAL).
	clock = clock.Add(1 * time.Second)
	addRead("0/200")
	tr.readHeartbeat(t.Context())
	at3, ok := tr.LastReceiveLSNAdvance()
	require.True(t, ok)
	assert.Equal(t, at2, at3, "advance time must not move when receive_lsn is unchanged")
}

// TestReaderTracksQuorumCommitLSN verifies quorum_commit_lsn is reported
// as-is on each read, not advance-tracked like receive_lsn above.
func TestReaderTracksQuorumCommitLSN(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	clock := time.Now()
	tr.now = func() time.Time { return clock }

	const pattern = "SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*"
	addRead := func(quorumLSN string) {
		queryService.AddQueryPatternOnce(pattern, mock.MakeQueryResult(
			[]string{"ts", "receive_lsn", "quorum_commit_lsn"},
			[][]any{{clock.UnixNano(), "0/100", quorumLSN}},
		))
	}

	addRead("0/100")
	tr.readHeartbeat(t.Context())
	lsn, ok := tr.QuorumCommitLSN()
	require.True(t, ok)
	assert.Equal(t, "0/100", lsn.String())

	// A later read's value replaces the previous one, even if it were to
	// decrease -- quorum_commit_lsn is trusted as leader-authored, not
	// advance-guarded like receive_lsn.
	clock = clock.Add(1 * time.Second)
	addRead("0/200")
	tr.readHeartbeat(t.Context())
	lsn, ok = tr.QuorumCommitLSN()
	require.True(t, ok)
	assert.Equal(t, "0/200", lsn.String())
}

// TestReaderReadHeartbeatBadTimestamp covers the "failed to parse heartbeat
// timestamp" path: a non-integer ts is a read error, not a silent skip.
func TestReaderReadHeartbeatBadTimestamp(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn"},
		[][]any{{"not-a-number", "0/16E5D38"}},
	))

	tr.readHeartbeat(t.Context())
	_, err := tr.Status()

	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to parse heartbeat timestamp")
	assert.EqualValues(t, 0, tr.Reads(), "a parse failure is not a successful read")
	assert.EqualValues(t, 1, tr.ReadErrors(), "wrong read error count")
}

// TestReaderUnparsableReceiveLSN covers the "failed to parse pg_last_wal_receive_lsn"
// path: a present-but-unparsable receive_lsn is best-effort — it must not fail the
// heartbeat-lag read, and no advance is recorded.
func TestReaderUnparsableReceiveLSN(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn"},
		[][]any{{now.Add(-5 * time.Second).UnixNano(), "garbage"}},
	))

	tr.readHeartbeat(t.Context())
	lag, err := tr.Status()

	require.NoError(t, err, "an unparsable receive_lsn must not fail the lag read")
	assert.Equal(t, 5*time.Second, lag)
	assert.EqualValues(t, 1, tr.Reads())
	assert.EqualValues(t, 0, tr.ReadErrors())

	_, ok := tr.LastReceiveLSNAdvance()
	assert.False(t, ok, "an unparsable receive_lsn records no advance")
}

// TestReaderUnparsableQuorumCommitLSN mirrors TestReaderUnparsableReceiveLSN for
// quorum_commit_lsn: a present-but-unparsable value is best-effort and must not
// fail the heartbeat-lag read.
func TestReaderUnparsableQuorumCommitLSN(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn"},
		[][]any{{now.Add(-5 * time.Second).UnixNano(), "0/16E5D38", "garbage"}},
	))

	tr.readHeartbeat(t.Context())
	lag, err := tr.Status()

	require.NoError(t, err, "an unparsable quorum_commit_lsn must not fail the lag read")
	assert.Equal(t, 5*time.Second, lag)
	assert.EqualValues(t, 1, tr.Reads())
	assert.EqualValues(t, 0, tr.ReadErrors())

	_, ok := tr.QuorumCommitLSN()
	assert.False(t, ok, "an unparsable quorum_commit_lsn records no observation")
}

// TestReaderQuorumCommitTsNullUntilSecondWrite covers the NULL quorum_commit_ts
// case (e.g. right after a promotion, before the writer has a proven candidate).
func TestReaderQuorumCommitTsNullUntilSecondWrite(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn", "quorum_commit_ts"},
		[][]any{{now.Add(-5 * time.Second).UnixNano(), "0/16E5D38", nil, nil}},
	))

	tr.readHeartbeat(t.Context())
	_, err := tr.Status()
	require.NoError(t, err)

	_, ok := tr.QuorumCommitTs()
	assert.False(t, ok, "a NULL quorum_commit_ts records no observation")
}

// TestReaderUnparsableQuorumCommitTs mirrors TestReaderUnparsableQuorumCommitLSN:
// a present-but-unparsable value is best-effort and must not fail the read.
func TestReaderUnparsableQuorumCommitTs(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn", "quorum_commit_ts"},
		[][]any{{now.Add(-5 * time.Second).UnixNano(), "0/16E5D38", "0/16E5D00", "garbage"}},
	))

	tr.readHeartbeat(t.Context())
	lag, err := tr.Status()

	require.NoError(t, err, "an unparsable quorum_commit_ts must not fail the lag read")
	assert.Equal(t, 5*time.Second, lag)

	_, ok := tr.QuorumCommitTs()
	assert.False(t, ok, "an unparsable quorum_commit_ts records no observation")
}

// TestReaderReadHeartbeatError tests that we properly account for errors
// encountered in the reading of heartbeat.
func TestReaderReadHeartbeatError(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	// Don't add any query - this will cause an error

	tr.readHeartbeat(t.Context())
	lag, err := tr.Status()

	require.Error(t, err)
	assert.Equal(t, 0*time.Second, lag, "wrong lastKnownLag")
	assert.EqualValues(t, 0, tr.Reads(), "wrong read count")
	assert.EqualValues(t, 1, tr.ReadErrors(), "wrong read error count")
}

// TestReaderOpen tests that the reader starts reading heartbeats when opened.
func TestReaderOpen(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	// Add query result for heartbeat reads
	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano(), "0/16E5D38"}},
	))

	assert.False(t, tr.IsOpen())

	tr.Open()
	assert.True(t, tr.IsOpen())

	// Wait for some reads to happen
	time.Sleep(1 * time.Second)

	assert.Greater(t, tr.Reads(), int64(0), "should have read at least one heartbeat")
	assert.EqualValues(t, 0, tr.ReadErrors())

	// Verify we can get status
	lag, err := tr.Status()
	require.NoError(t, err)
	assert.Greater(t, lag, 0*time.Second, "lag should be greater than zero")
}

// TestReaderOpenClose tests the basic open/close lifecycle.
func TestReaderOpenClose(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)

	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano(), "0/16E5D38"}},
	))

	assert.False(t, tr.IsOpen())

	tr.Open()
	assert.True(t, tr.IsOpen())

	// Open should be idempotent
	tr.Open()
	assert.True(t, tr.IsOpen())

	tr.Close()
	assert.False(t, tr.IsOpen())

	// Close should be idempotent
	tr.Close()
	assert.False(t, tr.IsOpen())
}

// TestReaderStatusNoHeartbeat tests that Status returns an error if no heartbeat
// has been received in over 2x the interval.
func TestReaderStatusNoHeartbeat(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	// Set lastKnownTime to more than 2x interval ago
	tr.lagMu.Lock()
	tr.lastKnownTime = now.Add(-3 * tr.interval)
	tr.lagMu.Unlock()

	// Advance "now" by 3x interval
	tr.now = func() time.Time {
		return now.Add(3 * tr.interval)
	}

	_, err := tr.Status()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no heartbeat received in over 2x the heartbeat interval")
}

// newTestReader creates a new heartbeat reader for testing.
func newTestReader(_ *testing.T, queryService *mock.QueryService, frozenTime *time.Time) *Reader {
	logger := slog.Default()
	shardID := []byte("test-shard")

	// Use 250ms interval for tests to oversample
	tr := newReader(queryService, logger, shardID, 250*time.Millisecond)

	if frozenTime != nil {
		tr.now = func() time.Time {
			return *frozenTime
		}
	}

	return tr
}

// TestReaderFallsBackToLagOnlyWithoutQuorumColumns: a standby on a table from a release
// before the quorum_commit columns still measures lag and WAL-receive progress, since it
// cannot alter the table itself. The gap is counted as a schema error, the full query is
// retried after the retry interval, and the quorum values appear once the columns do.
func TestReaderFallsBackToLagOnlyWithoutQuorumColumns(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()
	ctx := t.Context()

	const fullQuery = "SELECT ts, pg_last_wal_receive_lsn\\(\\)::text, quorum_commit_lsn"
	const lagOnlyQuery = "SELECT ts, pg_last_wal_receive_lsn\\(\\)::text FROM"
	lagOnly := mock.MakeQueryResult([]string{"ts", "receive_lsn"}, [][]any{{now.Add(-time.Second).UnixNano(), "0/100"}})

	// The full query fails on the old table; the lag-only one serves the read.
	queryService.AddQueryPatternOnceWithError(fullQuery, pgErr("42703"))
	queryService.AddQueryPatternOnce(lagOnlyQuery, lagOnly)
	tr.readHeartbeat(ctx)
	lag, err := tr.Status()
	require.NoError(t, err)
	assert.InDelta(t, time.Second, lag, float64(10*time.Millisecond))
	assert.EqualValues(t, 1, tr.Reads(), "the lag read succeeded")
	assert.EqualValues(t, 1, tr.ReadSchemaErrors(), "the missing columns are still visible")
	_, haveLSN := tr.QuorumCommitLSN()
	assert.False(t, haveLSN)

	// Within the retry interval only the lag-only query runs: the full query has no
	// pattern left and would fail the read.
	queryService.AddQueryPatternOnce(lagOnlyQuery, lagOnly)
	tr.readHeartbeat(ctx)
	require.NoError(t, func() error { _, err := tr.Status(); return err }())
	assert.EqualValues(t, 1, tr.ReadSchemaErrors(), "no new error while on the fallback")
	assert.EqualValues(t, 2, tr.Reads())

	// After the interval the full query is tried again, and works once the primary has
	// upgraded the table.
	now = now.Add(quorumColumnsRetryInterval + time.Second)
	queryService.AddQueryPatternOnce(fullQuery, mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn", "quorum_commit_ts"},
		[][]any{{now.Add(-time.Second).UnixNano(), "0/200", "0/150", now.Add(-2 * time.Second)}},
	))
	tr.readHeartbeat(ctx)
	lsn, haveLSN := tr.QuorumCommitLSN()
	require.True(t, haveLSN)
	assert.Equal(t, "0/150", lsn.String())
	assert.EqualValues(t, 3, tr.Reads())
	assert.EqualValues(t, 1, tr.ReadSchemaErrors())
}

// TestReaderDoesNotFallBackOnOtherErrors: only a schema error means "columns missing";
// a transient failure of the full query is a plain read error.
func TestReaderDoesNotFallBackOnOtherErrors(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	queryService.AddQueryPatternOnceWithError("SELECT ts, pg_last_wal_receive_lsn", pgErr("57014"))
	tr.readHeartbeat(t.Context())
	assert.EqualValues(t, 0, tr.Reads())
	assert.EqualValues(t, 1, tr.ReadErrors())
	assert.EqualValues(t, 0, tr.ReadSchemaErrors())
}

// TestReaderFallbackFailureIsOneError: if the lag-only read fails too, it is one failed
// read, not two.
func TestReaderFallbackFailureIsOneError(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	queryService.AddQueryPatternOnceWithError("SELECT ts, pg_last_wal_receive_lsn\\(\\)::text, quorum_commit_lsn", pgErr("42703"))
	queryService.AddQueryPatternOnceWithError("SELECT ts, pg_last_wal_receive_lsn\\(\\)::text FROM", pgErr("42P01"))
	tr.readHeartbeat(t.Context())
	assert.EqualValues(t, 0, tr.Reads())
	assert.EqualValues(t, 1, tr.ReadErrors())
	assert.EqualValues(t, 1, tr.ReadSchemaErrors())
}

const (
	fullReadPattern    = "SELECT ts, pg_last_wal_receive_lsn\\(\\)::text, quorum_commit_lsn"
	lagOnlyReadPattern = "SELECT ts, pg_last_wal_receive_lsn\\(\\)::text FROM"
)

func fullReadResult(now time.Time, lsn string) *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{"ts", "receive_lsn", "quorum_commit_lsn", "quorum_commit_ts"},
		[][]any{{now.Add(-time.Second).UnixNano(), "0/200", lsn, now.Add(-2 * time.Second).UTC().Format("2006-01-02 15:04:05.999999-07")}},
	)
}

func lagOnlyReadResult(now time.Time) *sqltypes.Result {
	return mock.MakeQueryResult([]string{"ts", "receive_lsn"}, [][]any{{now.Add(-time.Second).UnixNano(), "0/300"}})
}

// TestReaderFallbackForgetsStaleQuorumValues: once the table stops carrying the quorum
// columns, the quorum values seen earlier are dropped. Kept, they would age and multiorch
// would read the frozen quorum_commit_ts as a stalled quorum.
func TestReaderFallbackForgetsStaleQuorumValues(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()
	ctx := t.Context()

	queryService.AddQueryPatternOnce(fullReadPattern, fullReadResult(now, "0/150"))
	tr.readHeartbeat(ctx)
	_, haveLSN := tr.QuorumCommitLSN()
	_, haveTs := tr.QuorumCommitTs()
	require.True(t, haveLSN)
	require.True(t, haveTs)

	// The columns go away: the full query now fails with a schema error.
	now = now.Add(time.Second)
	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx)

	_, err := tr.Status()
	require.NoError(t, err, "lag is still measured")
	_, haveLSN = tr.QuorumCommitLSN()
	_, haveTs = tr.QuorumCommitTs()
	assert.False(t, haveLSN, "a stale quorum_commit_lsn must not be reported")
	assert.False(t, haveTs, "a stale quorum_commit_ts must not be reported")

	// And they come back when the columns do.
	now = now.Add(quorumColumnsRetryInterval + time.Second)
	queryService.AddQueryPatternOnce(fullReadPattern, fullReadResult(now, "0/400"))
	tr.readHeartbeat(ctx)
	lsn, haveLSN := tr.QuorumCommitLSN()
	require.True(t, haveLSN)
	assert.Equal(t, "0/400", lsn.String())
}

// TestReaderLagOnlyFailureInsideTheRetryWindow: while on the fallback, a failing lag-only
// read is a plain read error. It neither counts as a schema error nor ends the window, so
// the full query is not retried early.
func TestReaderLagOnlyFailureInsideTheRetryWindow(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx) // arms the window
	require.EqualValues(t, 1, tr.ReadSchemaErrors())

	queryService.AddQueryPatternOnceWithError(lagOnlyReadPattern, pgErr("57014"))
	tr.readHeartbeat(ctx)
	_, err := tr.Status()
	require.Error(t, err)
	assert.EqualValues(t, 2, tr.ReadErrors(), "the fallback's schema error plus this failure")
	assert.EqualValues(t, 1, tr.ReadSchemaErrors(), "a transient failure is not a schema error")

	// Still inside the window: the full query has no pattern and would fail the read.
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx)
	_, err = tr.Status()
	require.NoError(t, err)
}

// TestReaderRetryStillMissingRearmsTheWindow: when the retry finds the columns still
// missing, the window is armed again and the gap is counted again, not retried every tick.
func TestReaderRetryStillMissingRearmsTheWindow(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx)

	now = now.Add(quorumColumnsRetryInterval + time.Second)
	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 2, tr.ReadSchemaErrors())
	assert.EqualValues(t, 2, tr.Reads())

	now = now.Add(time.Second)
	queryService.AddQueryPatternOnce(lagOnlyReadPattern, lagOnlyReadResult(now))
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 2, tr.ReadSchemaErrors(), "re-armed: no full query inside the new window")
	assert.EqualValues(t, 3, tr.Reads())
}

// TestReaderFallbackTimeoutIsNotASchemaFailure: if the lag-only read fails for a reason
// other than the table (here a timeout), the quorum columns are not declared missing, so
// no retry window is armed and the next read tries the full query again.
func TestReaderFallbackTimeoutIsNotASchemaFailure(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnceWithError(lagOnlyReadPattern, context.DeadlineExceeded)
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 0, tr.Reads())
	assert.EqualValues(t, 1, tr.ReadErrors())
	assert.EqualValues(t, 0, tr.ReadSchemaErrors())

	queryService.AddQueryPatternOnce(fullReadPattern, fullReadResult(now, "0/150"))
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 1, tr.Reads(), "the full query ran again straight away")
	_, haveLSN := tr.QuorumCommitLSN()
	assert.True(t, haveLSN)
}

// TestReaderMissingCoreColumnFailsBothQueries: if ts itself is gone the fallback cannot
// help; the read fails once and is counted once as a schema error.
func TestReaderMissingCoreColumnFailsBothQueries(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	defer tr.Close()

	queryService.AddQueryPatternOnceWithError(fullReadPattern, pgErr("42703"))
	queryService.AddQueryPatternOnceWithError(lagOnlyReadPattern, pgErr("42703"))
	tr.readHeartbeat(t.Context())

	_, err := tr.Status()
	require.Error(t, err)
	assert.EqualValues(t, 0, tr.Reads())
	assert.EqualValues(t, 1, tr.ReadErrors())
	assert.EqualValues(t, 1, tr.ReadSchemaErrors())
}

func TestGetLeadershipView(t *testing.T) {
	const viewPattern = "SELECT leader_id, ts FROM multigres\\.heartbeat"

	t.Run("reads the leader and the heartbeat time", func(t *testing.T) {
		queryService := mock.NewQueryService()
		now := time.Now()
		tr := newTestReader(t, queryService, &now)
		defer tr.Close()
		queryService.AddQueryPattern(viewPattern, mock.MakeQueryResult(
			[]string{"leader_id", "ts"}, [][]any{{"pooler-1", now.Add(-3 * time.Second).UnixNano()}},
		))

		view, err := tr.GetLeadershipView()
		require.NoError(t, err)
		assert.Equal(t, "pooler-1", view.LeaderID)
		assert.Equal(t, 3*time.Second, view.ReplicationLag)
	})

	t.Run("a failing query is an error", func(t *testing.T) {
		queryService := mock.NewQueryService()
		tr := newTestReader(t, queryService, nil)
		defer tr.Close()
		queryService.AddQueryPatternWithError(viewPattern, pgErr("42703"))

		view, err := tr.GetLeadershipView()
		require.Error(t, err)
		assert.Nil(t, view)
		assert.ErrorContains(t, err, "failed to read leadership view")
	})

	t.Run("no heartbeat row is an error", func(t *testing.T) {
		queryService := mock.NewQueryService()
		tr := newTestReader(t, queryService, nil)
		defer tr.Close()
		queryService.AddQueryPattern(viewPattern, mock.MakeQueryResult([]string{"leader_id", "ts"}, nil))

		_, err := tr.GetLeadershipView()
		require.Error(t, err)
		assert.ErrorContains(t, err, "no heartbeat found")
	})

	t.Run("an unparsable timestamp is an error", func(t *testing.T) {
		queryService := mock.NewQueryService()
		tr := newTestReader(t, queryService, nil)
		defer tr.Close()
		queryService.AddQueryPattern(viewPattern, mock.MakeQueryResult(
			[]string{"leader_id", "ts"}, [][]any{{"pooler-1", "not-a-number"}},
		))

		_, err := tr.GetLeadershipView()
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to parse heartbeat timestamp")
	})
}
