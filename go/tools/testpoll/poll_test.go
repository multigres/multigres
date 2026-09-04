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
	"testing"
	"time"
)

// fakeT is a minimal TestingT that records whether Errorf was called,
// without depending on testify or a real *testing.T.
type fakeT struct {
	failed  bool
	message string
	ctx     context.Context //nolint:containedctx // test double mirroring *testing.T.Context()
}

func (f *fakeT) Errorf(format string, args ...any) {
	f.failed = true
	f.message = format
}

func (f *fakeT) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func TestNever_PassesWhenConditionNeverTrue(t *testing.T) {
	ft := &fakeT{}
	ok := Never(ft, func() bool { return false }, 50*time.Millisecond, 5*time.Millisecond)
	if !ok || ft.failed {
		t.Errorf("expected Never to pass when condition is always false, got ok=%v failed=%v", ok, ft.failed)
	}
}

func TestNever_FailsAssoonAsConditionTrue(t *testing.T) {
	ft := &fakeT{}
	calls := 0
	start := time.Now()
	ok := Never(ft, func() bool {
		calls++
		return calls >= 3
	}, time.Second, 5*time.Millisecond, "custom message")
	elapsed := time.Since(start)

	if ok || !ft.failed {
		t.Errorf("expected Never to fail once condition becomes true, got ok=%v failed=%v", ok, ft.failed)
	}
	// Should fail on the 3rd check, not wait out the full 1s window.
	if elapsed > 500*time.Millisecond {
		t.Errorf("Never should fail promptly once condition is true, took %s", elapsed)
	}
}

func TestNever_NoGoroutineOutlivesReturn(t *testing.T) {
	// Never runs condition synchronously — by construction there is nothing
	// left running once it returns. This documents that guarantee: mutating
	// a plain (non-atomic) variable from condition is safe to read
	// immediately after Never returns, with no synchronization needed.
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
}

func TestNever_ReturnsPromptlyWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ft := &fakeT{ctx: ctx}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := Never(ft, func() bool { return false }, 10*time.Second, 5*time.Millisecond)
	elapsed := time.Since(start)

	if !ok || ft.failed {
		t.Errorf("expected Never to return true/unfailed on context cancellation, got ok=%v failed=%v", ok, ft.failed)
	}
	if elapsed > time.Second {
		t.Errorf("Never should return promptly once its context is cancelled, took %s", elapsed)
	}
}

func TestWaitFor_PassesOnceConditionTrue(t *testing.T) {
	ft := &fakeT{}
	calls := 0
	ok := WaitFor(ft, func(context.Context) bool {
		calls++
		return calls >= 3
	}, time.Second, 5*time.Millisecond)
	if !ok || ft.failed {
		t.Errorf("expected WaitFor to pass once condition becomes true, got ok=%v failed=%v", ok, ft.failed)
	}
}

func TestWaitFor_FailsAfterTimeout(t *testing.T) {
	ft := &fakeT{}
	start := time.Now()
	ok := WaitFor(ft, func(context.Context) bool { return false }, 50*time.Millisecond, 5*time.Millisecond, "custom message")
	elapsed := time.Since(start)

	if ok || !ft.failed {
		t.Errorf("expected WaitFor to fail once waitFor elapses, got ok=%v failed=%v", ok, ft.failed)
	}
	if elapsed > time.Second {
		t.Errorf("WaitFor should fail close to its waitFor budget, took %s", elapsed)
	}
}

func TestWaitFor_PassesContextToCondition(t *testing.T) {
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
}

func TestWaitFor_StopsWithoutReportingOnUnrelatedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ft := &fakeT{ctx: ctx}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := WaitFor(ft, func(context.Context) bool { return false }, 10*time.Second, 5*time.Millisecond)
	elapsed := time.Since(start)

	if ok || ft.failed {
		t.Errorf("expected WaitFor to stop quietly (ok=false, no Errorf) on unrelated cancellation, got ok=%v failed=%v", ok, ft.failed)
	}
	if elapsed > time.Second {
		t.Errorf("WaitFor should stop promptly once its context is cancelled, took %s", elapsed)
	}
}
