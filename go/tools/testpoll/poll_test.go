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

package testpoll

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

// fakeT is a minimal TestingT that records failures without depending on
// testify or a real *testing.T. FailNow calls runtime.Goexit(), mirroring
// *testing.T's real behavior, so tests that exercise the failure path must
// run Never/WaitFor on their own goroutine (see runAndWaitForExit) — using a
// real *testing.T subtest for this instead would make the intentional
// failure count as a real, permanent test failure for the whole package.
type fakeT struct {
	failed      bool
	failNowCall bool
	message     string
}

func (f *fakeT) Helper() {}

func (f *fakeT) Errorf(format string, args ...any) {
	f.failed = true
	f.message = format
}

func (f *fakeT) FailNow() {
	f.failNowCall = true
	runtime.Goexit()
}

// runAndWaitForExit runs fn on its own goroutine and waits for it to end,
// whether by returning normally or via runtime.Goexit() (triggered by
// fakeT.FailNow()).
func runAndWaitForExit(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func TestNever_PassesWhenConditionNeverTrue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ft := &fakeT{}
		Never(ft, func() bool { return false }, 50*time.Millisecond, 5*time.Millisecond)
		if ft.failed || ft.failNowCall {
			t.Errorf("expected Never to pass when condition is always false, got failed=%v failNowCall=%v", ft.failed, ft.failNowCall)
		}
	})
}

func TestNever_FailsTestAsSoonAsConditionTrue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ft := &fakeT{}
		calls := 0
		start := time.Now()
		runAndWaitForExit(func() {
			Never(ft, func() bool {
				calls++
				return calls >= 3
			}, time.Second, 5*time.Millisecond, "custom message")
		})
		elapsed := time.Since(start)

		if !ft.failed || !ft.failNowCall {
			t.Errorf("expected Never to fail the test once condition becomes true, got failed=%v failNowCall=%v", ft.failed, ft.failNowCall)
		}
		// Should fail on the 3rd check, not wait out the full 1s window.
		if elapsed > 500*time.Millisecond {
			t.Errorf("Never should fail promptly once condition is true, took %s", elapsed)
		}
	})
}

func TestNever_NoGoroutineOutlivesReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Never runs condition synchronously — by construction there is
		// nothing left running once it returns. This documents that
		// guarantee: mutating a plain (non-atomic) variable from condition is
		// safe to read immediately after Never returns, with no
		// synchronization needed.
		ft := &fakeT{}
		var lastCheckTime time.Time
		Never(ft, func() bool {
			lastCheckTime = time.Now()
			return false
		}, 30*time.Millisecond, 5*time.Millisecond)

		if lastCheckTime.IsZero() {
			t.Fatal("condition was never called")
		}
		if time.Since(lastCheckTime) < 0 {
			t.Fatal("lastCheckTime is in the future, something is wrong with the test")
		}
	})
}

func TestWaitFor_PassesOnceConditionTrue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ft := &fakeT{}
		calls := 0
		WaitFor(ft, func(context.Context) bool {
			calls++
			return calls >= 3
		}, time.Second, 5*time.Millisecond)
		if ft.failed || ft.failNowCall {
			t.Errorf("expected WaitFor to pass once condition becomes true, got failed=%v failNowCall=%v", ft.failed, ft.failNowCall)
		}
	})
}

func TestWaitFor_FailsTestAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ft := &fakeT{}
		start := time.Now()
		runAndWaitForExit(func() {
			WaitFor(ft, func(context.Context) bool { return false }, 50*time.Millisecond, 5*time.Millisecond, "custom message")
		})
		elapsed := time.Since(start)

		if !ft.failed || !ft.failNowCall {
			t.Errorf("expected WaitFor to fail the test once waitFor elapses, got failed=%v failNowCall=%v", ft.failed, ft.failNowCall)
		}
		if elapsed > time.Second {
			t.Errorf("WaitFor should fail close to its waitFor budget, took %s", elapsed)
		}
	})
}

func TestWaitFor_PassesContextToCondition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ft := &fakeT{}
		var gotCtx context.Context
		WaitFor(ft, func(ctx context.Context) bool {
			gotCtx = ctx
			return true
		}, time.Second, 5*time.Millisecond)

		if gotCtx == nil {
			t.Fatal("condition was not given a context")
		}
		if _, ok := gotCtx.Deadline(); !ok {
			t.Error("expected condition's context to carry the waitFor deadline")
		}
	})
}

func TestFormatMsg(t *testing.T) {
	cases := []struct {
		name       string
		msgAndArgs []any
		want       string
	}{
		{"no args", nil, ""},
		{"single non-string arg", []any{42}, ": 42"},
		{"format string plus args", []any{"widget %s failed", "w1"}, ": widget w1 failed"},
		{"first arg not a string, multiple args", []any{42, "extra"}, ": [42 extra]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatMsg(tc.msgAndArgs)
			if got != tc.want {
				t.Errorf("formatMsg(%v) = %q, want %q", tc.msgAndArgs, got, tc.want)
			}
		})
	}
}
