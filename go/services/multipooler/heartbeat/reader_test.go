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

	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/tools/timer"

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
	queryService.AddQueryPattern("SELECT ts FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts"},
		[][]any{{now.Add(-10 * time.Second).UnixNano()}},
	))

	tr.readHeartbeat()
	lag, err := tr.Status()

	require.NoError(t, err)
	expectedLag := 10 * time.Second
	assert.Equal(t, expectedLag, lag, "wrong latest lag")
	assert.EqualValues(t, 1, tr.Reads(), "wrong read count")
	assert.EqualValues(t, 0, tr.ReadErrors(), "wrong read error count")
}

// TestReaderReadHeartbeatError tests that we properly account for errors
// encountered in the reading of heartbeat.
func TestReaderReadHeartbeatError(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tr := newTestReader(t, queryService, &now)
	defer tr.Close()

	// Don't add any query - this will cause an error

	tr.readHeartbeat()
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
	queryService.AddQueryPattern("SELECT ts FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano()}},
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

	queryService.AddQueryPattern("SELECT ts FROM multigres\\.heartbeat WHERE shard_id.*", mock.MakeQueryResult(
		[]string{"ts"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano()}},
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

	tr := NewReader(queryService, logger, shardID)
	// Use 250ms interval for tests to oversample
	tr.interval = 250 * time.Millisecond
	tr.ticks = timer.NewTimer(250 * time.Millisecond)

	if frozenTime != nil {
		tr.now = func() time.Time {
			return *frozenTime
		}
	}

	return tr
}
