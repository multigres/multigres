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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"

	"github.com/stretchr/testify/assert"
)

func TestReplTrackerStartWriting(t *testing.T) {
	queryService := mock.NewQueryService()

	queryService.AddQueryPattern("INSERT INTO multigres", mock.MakeQueryResult([]string{}, [][]any{}))

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(queryService, logger, shardID, poolerID, 250)
	defer rt.Close()

	assert.False(t, rt.isWritingHeartbeats())
	assert.False(t, rt.hw.IsOpen())

	rt.startWriting()
	assert.True(t, rt.isWritingHeartbeats())
	assert.True(t, rt.hw.IsOpen())

	// Wait for some heartbeats to be written
	time.Sleep(1 * time.Second)

	assert.Greater(t, rt.Writes(), int64(0))
	assert.EqualValues(t, 0, rt.WriteErrors())
}

func TestReplTrackerStopWriting(t *testing.T) {
	queryService := mock.NewQueryService()

	queryService.AddQueryPattern("INSERT INTO multigres", mock.MakeQueryResult([]string{}, [][]any{}))
	queryService.AddQueryPattern("SELECT ts FROM multigres", mock.MakeQueryResult(
		[]string{"ts"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano()}},
	))

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(queryService, logger, shardID, poolerID, 250)
	defer rt.Close()

	rt.startWriting()
	assert.True(t, rt.isWritingHeartbeats())
	assert.True(t, rt.hw.IsOpen())

	// Wait for some heartbeats
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))

	rt.stopWriting()
	assert.False(t, rt.isWritingHeartbeats())
	assert.False(t, rt.hw.IsOpen())

	// Capture writes count immediately after stopping to avoid race
	lastWrites := rt.Writes()

	// Wait and verify no more writes happen
	time.Sleep(1 * time.Second)
	assert.EqualValues(t, lastWrites, rt.Writes())
}

func TestReplTrackerEnableHeartbeat(t *testing.T) {
	queryService := mock.NewQueryService()

	queryService.AddQueryPattern("INSERT INTO multigres", mock.MakeQueryResult([]string{}, [][]any{}))

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(queryService, logger, shardID, poolerID, 250)
	defer rt.Close()

	rt.hw.Open()
	defer rt.hw.Close()

	// Manually enable writes
	rt.EnableHeartbeat(true)

	// Wait for heartbeats
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))

	// Disable writes
	rt.EnableHeartbeat(false)

	// Capture writes count immediately after stopping to avoid race
	lastWrites := rt.Writes()

	// Wait and verify no more writes
	time.Sleep(1 * time.Second)
	assert.EqualValues(t, lastWrites, rt.Writes())

	// Re-enable writes
	rt.EnableHeartbeat(true)

	// Wait and verify writes resume
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), lastWrites)
}

// TestReplTrackerOnStateChangeGating verifies the writer runs exactly when this
// pooler is the writable leader (RoutingRolePrimary) — independent of serving
// status. The routing role folds in both the consensus-leader and out-of-recovery
// facts, so a non-writable pooler must NOT run the writer (writes would fail on a
// read-only standby). Serving status is deliberately ignored: heartbeats are an
// internal signal (write-path proof + replica lag), so they keep flowing on a
// writable primary even while user serving is paused (DRAINING/DISABLED).
func TestReplTrackerOnStateChangeGating(t *testing.T) {
	tests := []struct {
		name          string
		routingRole   servingstate.RoutingRole
		servingStatus clustermetadatapb.PoolerServingStatus
		wantWriting   bool
	}{
		{
			name:          "writable leader, serving -> writer runs",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			wantWriting:   true,
		},
		{
			name:          "not the writable leader, serving -> writer stays off",
			routingRole:   servingstate.RoutingRoleReplica,
			servingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			wantWriting:   false,
		},
		{
			name:          "writable leader but draining -> writer still runs (heartbeats are serving-independent)",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_DRAINING,
			wantWriting:   true,
		},
		{
			name:          "writable leader but disabled -> writer still runs (heartbeats are serving-independent)",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
			wantWriting:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryService := mock.NewQueryService()
			queryService.AddQueryPattern("INSERT INTO multigres", mock.MakeQueryResult([]string{}, [][]any{}))
			queryService.AddQueryPattern("SELECT ts FROM multigres", mock.MakeQueryResult(
				[]string{"ts"},
				[][]any{{time.Now().Add(-5 * time.Second).UnixNano()}},
			))

			rt := NewReplTracker(queryService, slog.Default(), []byte("test-shard"), "test-pooler", 250)
			defer rt.Close()

			err := rt.OnStateChange(context.Background(), servingstate.State{RoutingRole: tt.routingRole, ServingStatus: tt.servingStatus})
			assert.NoError(t, err)
			assert.Equal(t, tt.wantWriting, rt.isWritingHeartbeats())
			assert.Equal(t, tt.wantWriting, rt.hw.IsOpen(), "writer is open iff this tracker is writing heartbeats")
			assert.Equal(t, !tt.wantWriting, rt.hr.IsOpen(), "reader runs whenever the writer does not")
		})
	}
}

func TestReplTrackerStartAndStopWriting(t *testing.T) {
	queryService := mock.NewQueryService()

	// Setup queries for both writer and reader
	queryService.AddQueryPattern("INSERT INTO multigres", mock.MakeQueryResult([]string{}, [][]any{}))
	queryService.AddQueryPattern("SELECT ts FROM multigres", mock.MakeQueryResult(
		[]string{"ts"},
		[][]any{{time.Now().Add(-5 * time.Second).UnixNano()}},
	))

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := newReplTrackerWithReaderInterval(queryService, logger, shardID, poolerID, 250, 250*time.Millisecond)
	defer rt.Close()

	// Start writing (writable leader + serving)
	rt.startWriting()
	assert.True(t, rt.isWritingHeartbeats())
	assert.True(t, rt.hw.IsOpen())
	assert.False(t, rt.hr.IsOpen())

	// Wait for some writes
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))
	assert.EqualValues(t, 0, rt.hr.Reads())

	// Switch to reader mode
	rt.stopWriting()
	assert.False(t, rt.isWritingHeartbeats())
	assert.False(t, rt.hw.IsOpen())
	assert.True(t, rt.hr.IsOpen())

	// Wait for some reads
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.hr.Reads(), int64(0))
}
