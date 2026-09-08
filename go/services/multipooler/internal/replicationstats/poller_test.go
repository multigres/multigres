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

package replicationstats

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

func columns() []string {
	return []string{
		"application_name", "usename", "last_ack_age_seconds",
		"sent_lsn", "replay_lag_seconds", "slot_name", "retained_wal_bytes",
	}
}

func TestPoller_ParsesFullRow(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{
		{"mg-replconn-42", "replicator", "2.5", "0/16E5D38", "1.5", "sub1", 4096},
	}))

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)

	p.poll(t.Context())

	assert.EqualValues(t, 1, p.Polls())
	assert.EqualValues(t, 0, p.PollErrors())

	p.mu.Lock()
	defer p.mu.Unlock()
	advance, ok := p.lastAdvance["42"]
	assert.True(t, ok, "sent_lsn should have registered an advance on first observation")
	assert.WithinDuration(t, time.Now(), advance, time.Second)
}

// TestPoller_SkipsRowMissingApplicationNamePrefix verifies a row that
// somehow doesn't match our tag (shouldn't happen given the WHERE clause,
// but defensive) is skipped rather than aborting the tick.
func TestPoller_SkipsRowMissingApplicationNamePrefix(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{
		{"someone-elses-app", "replicator", "2.5", "0/16E5D38", "1.5", "sub1", 4096},
	}))

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)

	assert.NotPanics(t, func() { p.poll(t.Context()) })
	assert.EqualValues(t, 1, p.Polls())

	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Empty(t, p.lastAdvance, "a row not matching our tag must not be tracked")
}

// TestPoller_TracksLastMessageAdvanceAcrossTicks verifies last_message_age
// is derived from when sent_lsn was last observed to *increase*, not from
// the current tick alone — mirroring heartbeat.Reader's
// lastReceiveLSNAdvanceTime pattern.
//
// Uses AddQueryPatternOnce rather than AddQueryPattern for each tick's
// result: the mock matches patterns in registration order and never
// replaces an earlier one for the same regex, so a plain AddQueryPattern
// call for tick 3 would never be reached — tick 1's pattern would keep
// matching forever. Two identical once-patterns queue up front so ticks 1
// and 2 (same LSN, no advance) each consume one before tick 3's new LSN is
// registered.
func TestPoller_TracksLastMessageAdvanceAcrossTicks(t *testing.T) {
	qs := mock.NewQueryService()
	sameLSNResult := mock.MakeQueryResult(columns(), [][]any{
		{"mg-replconn-1", "replicator", "0", "0/1000000", "0", "", nil},
	})
	qs.AddQueryPatternOnce("FROM pg_stat_replication", sameLSNResult)
	qs.AddQueryPatternOnce("FROM pg_stat_replication", sameLSNResult)

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)

	p.poll(t.Context())
	p.mu.Lock()
	firstAdvance := p.lastAdvance["1"]
	p.mu.Unlock()

	time.Sleep(10 * time.Millisecond)

	// Same LSN again (no new WAL sent) — advance time must NOT move.
	p.poll(t.Context())
	p.mu.Lock()
	secondAdvance := p.lastAdvance["1"]
	p.mu.Unlock()
	assert.Equal(t, firstAdvance, secondAdvance, "advance time must not move when sent_lsn is unchanged")

	// Now advance the LSN — advance time must move forward.
	qs.AddQueryPatternOnce("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{
		{"mg-replconn-1", "replicator", "0", "0/2000000", "0", "", nil},
	}))
	p.poll(t.Context())
	p.mu.Lock()
	thirdAdvance := p.lastAdvance["1"]
	p.mu.Unlock()
	assert.True(t, thirdAdvance.After(secondAdvance), "advance time must move forward when sent_lsn increases")
}

// TestPoller_DropsStaleConnectionsBetweenTicks verifies a connection that
// disappears from the result set (disconnected, or this pooler lost
// leadership) has its advance-tracking state dropped, not left stale
// forever.
//
// Uses AddQueryPatternOnce for the same reason as
// TestPoller_TracksLastMessageAdvanceAcrossTicks: the second tick's result
// must actually replace the first's for the second poll() call.
func TestPoller_DropsStaleConnectionsBetweenTicks(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPatternOnce("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{
		{"mg-replconn-1", "replicator", "0", "0/1000000", "0", "", nil},
	}))

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)
	p.poll(t.Context())

	p.mu.Lock()
	require.Contains(t, p.lastAdvance, "1")
	p.mu.Unlock()

	qs.AddQueryPatternOnce("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))
	p.poll(t.Context())

	p.mu.Lock()
	defer p.mu.Unlock()
	assert.NotContains(t, p.lastAdvance, "1", "a connection no longer in the result set must be dropped")
}

func TestPoller_QueryErrorIsCountedNotFatal(t *testing.T) {
	qs := mock.NewQueryService()
	// No pattern registered -> mock query service returns an error.

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)

	assert.NotPanics(t, func() { p.poll(t.Context()) })
	assert.EqualValues(t, 1, p.PollErrors())
	assert.EqualValues(t, 0, p.Polls())
}

func TestPoller_OpenCloseIsOpen(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 50*time.Millisecond)

	assert.False(t, p.IsOpen())
	p.Open()
	assert.True(t, p.IsOpen())

	time.Sleep(150 * time.Millisecond)
	assert.Greater(t, p.Polls(), int64(0))

	p.Close()
	assert.False(t, p.IsOpen())
}

// TestPoller_StatusReflectsLatestPoll verifies Status (used by the
// multipooler status page) reports the same live data the OTel callback
// does, and that Close clears the reported connections rather than leaving
// the last poll's snapshot visible.
func TestPoller_StatusReflectsLatestPoll(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{
		{"mg-replconn-7", "replicator", "2.5", "0/16E5D38", "1.5", "sub1", 4096},
	}))

	m, err := NewMetrics()
	require.NoError(t, err)
	p := newPoller(qs, m, slog.Default(), 250*time.Millisecond)

	status := p.Status()
	assert.False(t, status.Open)
	assert.Empty(t, status.Connections)

	p.poll(t.Context())
	status = p.Status()
	assert.EqualValues(t, 1, status.Polls)
	require.Len(t, status.Connections, 1)
	assert.Equal(t, "7", status.Connections[0].ConnID)
	assert.Equal(t, "sub1", status.Connections[0].SlotName)

	p.Close()
	status = p.Status()
	assert.False(t, status.Open)
	assert.Empty(t, status.Connections, "Close must clear the reported snapshot, not leave the last poll visible")
}
