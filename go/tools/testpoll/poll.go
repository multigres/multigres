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
	"fmt"
	"time"
)

// TestingT is the interface Never and WaitFor need — Errorf, FailNow, and
// Helper, matching the relevant subset of testing.TB. *testing.T/B/F all
// satisfy it structurally (testing.TB itself can't be used directly here: it
// has an unexported method, so only the real testing package could ever
// implement it, which would rule out a test double for verifying Never/
// WaitFor's own failure behavior without that verification becoming a real,
// permanent test failure).
type TestingT interface {
	Errorf(format string, args ...any)
	FailNow()
	Helper()
}

// Never asserts that condition does not become true within waitFor, checking
// every tick, failing the test immediately via t.FailNow() if it does.
// Unlike testify's assert.Never/require.Never, condition runs synchronously
// on the calling goroutine instead of a fresh one per tick, so it can never
// leak a goroutine that races with whatever the caller does next (see
// https://github.com/stretchr/testify/issues/1611).
func Never(t TestingT, condition func() bool, waitFor, tick time.Duration, msgAndArgs ...any) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for {
		// condition must return reasonably quickly: nothing here runs
		// concurrently to enforce waitFor as a hard ceiling.
		if condition() {
			t.Errorf("Condition satisfied%s", formatMsg(msgAndArgs))
			t.FailNow()
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(tick)
	}
}

// WaitFor asserts that condition becomes true within waitFor, checking every
// tick, failing the test immediately via t.FailNow() if it doesn't. Like
// [Never], condition runs synchronously instead of on a spawned goroutine
// like testify's assert.Eventually — which is also why it's safe for
// condition to call a require.* assertion or t.Fatalf directly to fail even
// earlier, e.g. on detecting the condition can never become true (a
// supervised process died).
func WaitFor(t TestingT, condition func(ctx context.Context) bool, waitFor, tick time.Duration, msgAndArgs ...any) {
	t.Helper()

	// Passed to condition every tick (not re-created per attempt) so it can
	// bound its own work, e.g. an RPC call that should give up once the
	// budget is spent rather than block past it.
	ctx, cancel := context.WithTimeout(context.Background(), waitFor) //nolint:gocritic // deliberate root context; WaitFor owns its own timeout independent of any caller context
	defer cancel()

	for {
		if condition(ctx) {
			return
		}
		if ctx.Err() != nil {
			t.Errorf("Condition never satisfied within %s%s", waitFor, formatMsg(msgAndArgs))
			t.FailNow()
		}
		time.Sleep(tick)
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
