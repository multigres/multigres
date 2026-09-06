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
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
)

// TODO: add stats for heartbeat reads and writes

// ReplTracker tracks replication lag using heartbeats. The writer runs
// while this pooler is the writable leader; the reader runs otherwise —
// see switcher.RoleSwitcher for the shared start/stop-on-state-change
// mechanism.
type ReplTracker struct {
	hw *Writer
	hr *Reader
	sw *switcher.RoleSwitcher
}

// NewReplTracker creates a new ReplTracker.
func NewReplTracker(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int) *ReplTracker {
	hw := NewWriter(queryService, logger, shardID, poolerID, intervalMs)
	hr := NewReader(queryService, logger, shardID)
	return &ReplTracker{hw: hw, hr: hr, sw: switcher.NewRoleSwitcher(hw, hr)}
}

// newReplTrackerWithReaderInterval creates a ReplTracker with a custom reader interval for testing.
func newReplTrackerWithReaderInterval(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int, readerInterval time.Duration) *ReplTracker {
	hw := NewWriter(queryService, logger, shardID, poolerID, intervalMs)
	hr := newReader(queryService, logger, shardID, readerInterval)
	return &ReplTracker{hw: hw, hr: hr, sw: switcher.NewRoleSwitcher(hw, hr)}
}

// HeartbeatWriter returns the heartbeat writer used by this tracker.
func (rt *ReplTracker) HeartbeatWriter() *Writer {
	return rt.hw
}

// HeartbeatReader returns the heartbeat reader used by this tracker.
func (rt *ReplTracker) HeartbeatReader() *Reader {
	return rt.hr
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
func (rt *ReplTracker) OnStateChange(ctx context.Context, state servingstate.State) error {
	return rt.sw.OnStateChange(ctx, state)
}

// Close closes both the writer and reader, regardless of which was active.
// Callers on a real shutdown path must call this explicitly — see
// switcher.RoleSwitcher.Close.
func (rt *ReplTracker) Close() {
	rt.sw.Close()
}

// isWritingHeartbeats reports whether this tracker is running the heartbeat
// writer (as opposed to the reader) — true iff this pooler is the writable
// routing primary. Unexported: only the package's own tests inspect it.
func (rt *ReplTracker) isWritingHeartbeats() bool {
	return rt.hw.IsOpen()
}

// EnableHeartbeat enables or disables writes of heartbeat.
// This functionality is primarily used by tests. It bypasses the
// switcher.RoleSwitcher deliberately — it's a manual override for tests,
// not a state-change reaction.
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
