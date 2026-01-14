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

	"github.com/multigres/multigres/go/services/multipooler/executor/mock"

	"github.com/stretchr/testify/assert"
)

func TestWriteHeartbeat(t *testing.T) {
	queryService := mock.NewQueryService()
	now := time.Now()
	tw := newTestWriter(t, queryService, &now)

	// Add expected heartbeat query (must match with whitespace)
	queryService.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", mock.MakeQueryResult([]string{}, [][]any{}))

	tw.Open()
	defer tw.Close()

	// Write a single heartbeat
	tw.writeHeartbeat()
	lastWrites := tw.Writes()
	assert.GreaterOrEqual(t, lastWrites, int64(1))
	assert.EqualValues(t, 0, tw.WriteErrors())
}

// TestWriteHeartbeatOpen tests that the heartbeat writer writes heartbeats when the writer is open.
func TestWriteHeartbeatOpen(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)

	// Add expected heartbeat query pattern
	queryService.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", mock.MakeQueryResult([]string{}, [][]any{}))

	// Writes should not happen when closed
	tw.writeHeartbeat()
	assert.EqualValues(t, 0, tw.Writes(), "should not write when closed")

	tw.Open()
	defer tw.Close()

	lastWrites := tw.Writes()

	t.Run("open, heartbeats", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				assert.EqualValues(t, 0, tw.WriteErrors())
				currentWrites := tw.Writes()
				assert.Greater(t, currentWrites, lastWrites)
				lastWrites = currentWrites
			}
		}
	})
}

// TestWriteHeartbeatError tests that write errors are logged but don't crash the writer.
func TestWriteHeartbeatError(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)

	tw.Open()
	defer tw.Close()

	// Don't add any expected queries - this will cause an error
	tw.writeHeartbeat()
	assert.EqualValues(t, 0, tw.Writes())
	assert.EqualValues(t, 1, tw.WriteErrors())
}

// TestOpenClose tests the basic open/close lifecycle.
func TestOpenClose(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)

	queryService.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", mock.MakeQueryResult([]string{}, [][]any{}))

	assert.False(t, tw.IsOpen())

	tw.Open()
	assert.True(t, tw.IsOpen())

	// Open should be idempotent
	tw.Open()
	assert.True(t, tw.IsOpen())

	tw.Close()
	assert.False(t, tw.IsOpen())

	// Close should be idempotent
	tw.Close()
	assert.False(t, tw.IsOpen())
}

// TestMultipleWriters tests that multiple writers can run concurrently.
func TestMultipleWriters(t *testing.T) {
	queryService := mock.NewQueryService()

	queryService.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", mock.MakeQueryResult([]string{}, [][]any{}))

	tw1 := newTestWriter(t, queryService, nil)
	tw2 := newTestWriter(t, queryService, nil)

	tw1.Open()
	tw2.Open()

	defer tw1.Close()
	defer tw2.Close()

	// Let them write for a bit
	time.Sleep(3 * time.Second)

	// Both should have written heartbeats
	assert.Greater(t, tw1.Writes(), int64(0))
	assert.Greater(t, tw2.Writes(), int64(0))
	assert.EqualValues(t, 0, tw1.WriteErrors())
	assert.EqualValues(t, 0, tw2.WriteErrors())
}

// TestCloseWhileStuckWriting tests that Close() properly cancels an in-progress write
// and waits for it to complete.
func TestCloseWhileStuckWriting(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)

	writeStarted := make(chan struct{})
	writeUnblocked := make(chan struct{})

	// Add a query pattern that blocks until context is canceled
	queryService.AddQueryPatternWithContextCallback(
		"\\s*INSERT INTO multigres\\.heartbeat.*",
		mock.MakeQueryResult([]string{}, [][]any{}),
		func(ctx context.Context, _ string) {
			close(writeStarted)
			// Block until context is canceled
			<-ctx.Done()
			close(writeUnblocked)
		},
	)

	tw.Open()

	// Wait for the first write to start
	<-writeStarted

	// Close should cancel the context and wait for the write to complete
	closeDone := make(chan struct{})
	go func() {
		tw.Close()
		close(closeDone)
	}()

	// Verify write was unblocked by context cancellation
	select {
	case <-writeUnblocked:
		// Good - context was canceled
	case <-time.After(2 * time.Second):
		t.Fatal("write was not unblocked by context cancellation")
	}

	// Verify Close() completed
	select {
	case <-closeDone:
		// Good - Close completed
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not complete in time")
	}

	assert.False(t, tw.IsOpen())
}

// newTestWriter creates a new heartbeat writer for testing.
func newTestWriter(_ *testing.T, queryService *mock.QueryService, frozenTime *time.Time) *Writer {
	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	// Use 250ms interval for tests to oversample our 1s test ticker
	tw := NewWriter(queryService, logger, shardID, poolerID, 250)

	if frozenTime != nil {
		tw.now = func() time.Time {
			return *frozenTime
		}
	}

	return tw
}
