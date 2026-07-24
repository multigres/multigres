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
	"log/slog"
	"testing"
	"time"

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

	// Add query result for heartbeat read
	queryService.AddQueryPattern("SELECT ts, pg_last_wal_receive_lsn.*FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts", "receive_lsn"},
		[][]any{{now.Add(-10 * time.Second).UnixNano(), "0/16E5D38"}},
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
