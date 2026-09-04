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

// Package testpoll provides small, dependency-light polling assertions for
// tests.
package testpoll

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// TestingT is the interface Never and WaitFor need. *testing.T/B/F all
// satisfy it, including Context (Go 1.24+), which is cancelled once the test
// has finished — e.g. a parent test group failed, or the test binary's own
// -timeout is about to fire.
type TestingT interface {
	Errorf(format string, args ...any)
	Context() context.Context
}

type helperT interface {
	Helper()
}

// Never asserts that condition does not become true within waitFor, checking
// every tick. Unlike testify's assert.Never, condition runs synchronously on
// the calling goroutine instead of a fresh one per tick, so it can never
// leak a goroutine that races with whatever the caller does next (see
// https://github.com/stretchr/testify/issues/1611).
func Never(t TestingT, condition func() bool, waitFor, tick time.Duration, msgAndArgs ...any) bool {
	if h, ok := t.(helperT); ok {
		h.Helper()
	}

	done := t.Context().Done()
	deadline := time.Now().Add(waitFor)
	for {
		// condition must return reasonably quickly: nothing here runs
		// concurrently to enforce waitFor as a hard ceiling.
		if condition() {
			t.Errorf("Condition satisfied%s", formatMsg(msgAndArgs))
			return false
		}
		if time.Now().After(deadline) {
			return true
		}

		timer := time.NewTimer(tick)
		select {
		case <-done:
			// Test is already ending for an unrelated reason (sibling
			// failure, -timeout) — nothing more to report here.
			timer.Stop()
			return true
		case <-timer.C:
		}
	}
}

// WaitFor asserts that condition becomes true within waitFor, checking every
// tick. Like [Never], condition runs synchronously instead of on a spawned
// goroutine like testify's assert.Eventually.
func WaitFor(t TestingT, condition func(ctx context.Context) bool, waitFor, tick time.Duration, msgAndArgs ...any) bool {
	if h, ok := t.(helperT); ok {
		h.Helper()
	}

	// Passed to condition every tick (not re-created per attempt) so it can
	// bound its own work, e.g. an RPC call that should give up once the
	// budget is spent rather than block past it.
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()

	for {
		// Safe to call a require.* assertion or t.Fatalf here to fail
		// immediately instead of waiting out the rest of waitFor — e.g. on
		// detecting the condition can never become true (a supervised
		// process died). That would be a misuse from testify's Eventually,
		// whose condition doesn't run on the test's own goroutine.
		if condition(ctx) {
			return true
		}
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Condition never satisfied within %s%s", waitFor, formatMsg(msgAndArgs))
			}
			// Otherwise cancelled for a reason unrelated to this wait (see
			// Never's done-channel case above) — nothing more to report.
			return false
		}

		timer := time.NewTimer(tick)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

// formatMsg renders an optional trailing message the same way testify does:
// no args produces nothing, a single arg is used as-is, and a leading format
// string plus args are sprintf'd.
func formatMsg(msgAndArgs []any) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprintf(": %v", msgAndArgs[0])
	}
	format, ok := msgAndArgs[0].(string)
	if !ok {
		return fmt.Sprintf(": %v", msgAndArgs)
	}
	return ": " + fmt.Sprintf(format, msgAndArgs[1:]...)
}
