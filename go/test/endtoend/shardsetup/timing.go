// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

import (
	"sync"
	"testing"
	"time"
)

type timingEntry struct {
	label   string
	elapsed time.Duration
	limit   time.Duration
}

// TimingCollector records elapsed durations for timeout-bounded setup operations
// and prints a summary table at test teardown. The percentage column is the key
// signal: values approaching 80–90% are at risk on slow CI runners.
type TimingCollector struct {
	mu      sync.Mutex
	entries []timingEntry
}

// Record adds an elapsed/limit pair for the named operation.
func (c *TimingCollector) Record(label string, elapsed, limit time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, timingEntry{label, elapsed, limit})
}

// Report prints the collected timing table via t.Log. Output appears on failure
// always and with -v always.
func (c *TimingCollector) Report(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 {
		return
	}
	t.Log("Startup timing:")
	for _, e := range c.entries {
		pct := float64(e.elapsed) / float64(e.limit) * 100
		t.Logf("  %-40s %6s / %-6s  %3.0f%%",
			e.label,
			e.elapsed.Round(time.Millisecond),
			e.limit,
			pct,
		)
	}
}
