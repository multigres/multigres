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
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// TODO: add stats for heartbeat reads and writes

// ReplTracker tracks replication lag using heartbeats.
type ReplTracker struct {
	mu sync.Mutex
	// writingHeartbeats is true when this tracker runs the heartbeat writer
	// rather than the reader — i.e. this pooler is the writable routing primary
	// and serving. Derived from the routing role via OnStateChange; it is not a
	// postgres-recovery or topology role.
	writingHeartbeats bool

	hw *Writer
	hr *Reader
}

// NewReplTracker creates a new ReplTracker.
func NewReplTracker(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int) *ReplTracker {
	return &ReplTracker{
		hw: NewWriter(queryService, logger, shardID, poolerID, intervalMs),
		hr: NewReader(queryService, logger, shardID),
	}
}

// newReplTrackerWithReaderInterval creates a ReplTracker with a custom reader interval for testing.
func newReplTrackerWithReaderInterval(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int, readerInterval time.Duration) *ReplTracker {
	return &ReplTracker{
		hw: NewWriter(queryService, logger, shardID, poolerID, intervalMs),
		hr: newReader(queryService, logger, shardID, readerInterval),
	}
}

// HeartbeatWriter returns the heartbeat writer used by this tracker.
func (rt *ReplTracker) HeartbeatWriter() *Writer {
	return rt.hw
}

// HeartbeatReader returns the heartbeat reader used by this tracker.
func (rt *ReplTracker) HeartbeatReader() *Reader {
	return rt.hr
}

// startWriting switches to writer mode: stops the reader, starts the writer.
// Called when this pooler is the writable routing primary and serving.
func (rt *ReplTracker) startWriting() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.writingHeartbeats = true
	rt.hr.Close()
	rt.hw.Open()
}

// stopWriting switches to reader mode: stops the writer, starts the reader.
// Called whenever this pooler is not the writable routing primary (or not serving).
func (rt *ReplTracker) stopWriting() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.writingHeartbeats = false
	rt.hw.Close()
	rt.hr.Open()
}

// OnStateChange transitions the heartbeat tracker based on the routing role: the
// writer runs whenever this pooler is the writable leader (routing role PRIMARY —
// out of recovery AND the active consensus leader); otherwise the reader runs.
// Writability folds in the out-of-recovery requirement, so heartbeats are never
// written to a standby.
//
// It intentionally does NOT gate on ServingStatus. Heartbeats are an internal
// signal — they prove the write path works and let replicas measure replication
// lag — so they must keep flowing on a writable primary even while user serving
// is paused (DRAINING/DISABLED); freezing them would feed replicas a false
// lag/health signal. The writes go through the internal query service (not the
// user-facing serving gate) and are not counted by the drain, so writing while
// not serving is both possible and safe.
func (rt *ReplTracker) OnStateChange(_ context.Context, state servingstate.State) error {
	if state.RoutingRole.Writable() {
		rt.startWriting()
	} else {
		rt.stopWriting()
	}
	return nil
}

// Close closes ReplTracker.
func (rt *ReplTracker) Close() {
	rt.hw.Close()
	rt.hr.Close()
}

// isWritingHeartbeats reports whether this tracker is running the heartbeat
// writer (as opposed to the reader) — true iff this pooler is the writable
// routing primary and serving. Unexported: only the package's own tests inspect it.
func (rt *ReplTracker) isWritingHeartbeats() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.writingHeartbeats
}

// EnableHeartbeat enables or disables writes of heartbeat.
// This functionality is primarily used by tests.
func (rt *ReplTracker) EnableHeartbeat(enable bool) {
	if enable {
		rt.hw.Open()
	} else {
		rt.hw.Close()
	}
}

// Writes returns the count of successful heartbeat writes.
func (rt *ReplTracker) Writes() int64 {
	return rt.hw.Writes()
}

// WriteErrors returns the count of heartbeat write errors.
func (rt *ReplTracker) WriteErrors() int64 {
	return rt.hw.WriteErrors()
}
