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

// Package errlog logs a failure that a timer or retry loop observes on every
// pass once, instead of on every pass.
package errlog

import (
	"context"
	"log/slog"
	"sync"
)

// Dedupe tracks the last error text seen per key. A failure is logged when it
// starts or when its error text changes, and a single info line is logged when
// it clears. The zero value is ready to use and safe for concurrent use.
type Dedupe struct {
	mu   sync.Mutex
	last map[string]string
}

// Observe records the outcome of one pass for key. A non-nil err is logged at
// level with failMsg unless it repeats the previous error text for key. A nil
// err logs recoverMsg at info if key was failing, and logs nothing otherwise.
// attrs are attached to whichever line is logged.
func (d *Dedupe) Observe(ctx context.Context, logger *slog.Logger, key string, level slog.Level,
	failMsg, recoverMsg string, err error, attrs ...any,
) {
	d.mu.Lock()
	prev, failing := d.last[key]
	if err == nil {
		delete(d.last, key)
		d.mu.Unlock()
		if failing {
			logger.InfoContext(ctx, recoverMsg, attrs...)
		}
		return
	}
	msg := err.Error()
	if failing && prev == msg {
		d.mu.Unlock()
		return
	}
	if d.last == nil {
		d.last = make(map[string]string)
	}
	d.last[key] = msg
	d.mu.Unlock()
	logger.Log(ctx, level, failMsg, append(attrs, "error", err)...)
}

// Retain forgets every key for which keep returns false, without logging. It
// ends the failure state of keys that disappeared instead of recovering.
func (d *Dedupe) Retain(keep func(key string) bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key := range d.last {
		if !keep(key) {
			delete(d.last, key)
		}
	}
}

// Reset forgets every key, so the next failure is logged afresh.
func (d *Dedupe) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.last)
}
