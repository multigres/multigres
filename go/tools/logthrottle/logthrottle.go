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

// Package logthrottle limits how often a line that can repeat is logged.
package logthrottle

import (
	"sync"
	"time"
)

// Throttle lets one log line through per interval and counts the ones it held back,
// so the next line that is logged can say how many were skipped. It is safe for
// concurrent use.
type Throttle struct {
	interval time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

// New returns a Throttle that allows one line per interval.
func New(interval time.Duration) *Throttle {
	return &Throttle{interval: interval}
}

// Allow reports whether a line may be logged at now. When it may, suppressed is the
// number of lines held back since the last one that was logged.
func (t *Throttle) Allow(now time.Time) (ok bool, suppressed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() && now.Sub(t.last) < t.interval {
		t.suppressed++
		return false, 0
	}
	suppressed = t.suppressed
	t.suppressed = 0
	t.last = now
	return true, suppressed
}
