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
	"sync"

	"github.com/multigres/multigres/go/multipooler/executor"
)

// TODO: add stats for heartbeat reads and writes

// ReplTracker tracks replication lag using heartbeats.
type ReplTracker struct {
	mu        sync.Mutex
	isPrimary bool

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

// HeartbeatWriter returns the heartbeat writer used by this tracker.
func (rt *ReplTracker) HeartbeatWriter() *Writer {
	return rt.hw
}

// HeartbeatReader returns the heartbeat reader used by this tracker.
func (rt *ReplTracker) HeartbeatReader() *Reader {
	return rt.hr
}

// MakePrimary must be called if the database becomes a primary.
func (rt *ReplTracker) MakePrimary() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.isPrimary = true
	rt.hr.Close()
	rt.hw.Open()
}

// MakeNonPrimary must be called if the database becomes a non-primary (standby).
func (rt *ReplTracker) MakeNonPrimary() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.isPrimary = false
	rt.hw.Close()
	rt.hr.Open()
}

// Close closes ReplTracker.
func (rt *ReplTracker) Close() {
	rt.hw.Close()
	rt.hr.Close()
}

// IsPrimary returns whether this tracker is in primary mode.
func (rt *ReplTracker) IsPrimary() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.isPrimary
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
