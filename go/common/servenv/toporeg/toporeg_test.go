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

package toporeg

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/tools/testpoll"
)

func TestRegister_SuccessOnFirstTry(t *testing.T) {
	var alarmMessage string
	var registerCalled bool
	var unregisterCalled bool

	register := func(ctx context.Context) error {
		registerCalled = true
		return nil
	}

	unregister := func(ctx context.Context) error {
		unregisterCalled = true
		return nil
	}

	alarm := func(msg string) {
		alarmMessage = msg
	}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	assert.True(t, registerCalled, "register function should be called")
	assert.False(t, unregisterCalled, "unregister should not be called during successful register")
	assert.Empty(t, alarmMessage, "alarm should not be triggered on success")

	tr.Unregister()
	assert.True(t, unregisterCalled, "unregister should be called during Unregister")
}

func TestRegister_FailureAndRetry(t *testing.T) {
	var registerCallCount atomic.Int32
	var alarmMessages []string
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		count := registerCallCount.Add(1)
		if count < 3 {
			return errors.New("register failed")
		}
		return nil
	}

	unregister := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return registerCallCount.Load() >= 1 &&
			len(alarmMessages) > 0 &&
			strings.Contains(alarmMessages[0], "Failed to register component with topology")
	}, 20*time.Millisecond, 1*time.Millisecond, "Incorrect register call count %d", registerCallCount.Load())

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return registerCallCount.Load() == 3 &&
			alarmMessages[len(alarmMessages)-1] == ""
	}, 200*time.Millisecond, 1*time.Millisecond, "Incorrect register call count %d", registerCallCount.Load())

	tr.Unregister()
}

func TestRegister_ContinuousFailure(t *testing.T) {
	var alarmMessages []string
	var registerCallCount atomic.Int32
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		registerCallCount.Add(1)
		return errors.New("always fails")
	}

	unregister := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return registerCallCount.Load() > 1 && len(alarmMessages) > 1
	}, 100*time.Millisecond, 10*time.Millisecond, "register should be retried multiple times")

	mu.Lock()
	for _, msg := range alarmMessages {
		assert.Contains(t, msg, "Failed to register component with topology", "all alarms should contain error message")
	}
	mu.Unlock()

	tr.Unregister()

	finalCallCount := registerCallCount.Load()

	testpoll.Never(t, func() bool {
		return registerCallCount.Load() > finalCallCount
	}, 100*time.Millisecond, 50*time.Millisecond, "register should stop being called after Unregister")
}

func TestUnregister_WithNilPointer(t *testing.T) {
	var tr *TopoReg
	assert.NotPanics(t, func() {
		tr.Unregister()
	}, "Unregister should handle nil pointer gracefully")
}

// shortenReassert makes the re-assertion loop tick fast enough for tests.
func shortenReassert(t *testing.T) {
	t.Helper()
	old := reassertInterval
	reassertInterval = 10 * time.Millisecond
	t.Cleanup(func() { reassertInterval = old })
}

func TestReassert_RewritesRegistrationPeriodically(t *testing.T) {
	shortenReassert(t)

	var registerCallCount atomic.Int32
	register := func(ctx context.Context) error {
		registerCallCount.Add(1)
		return nil
	}

	tr := Register(register, func(context.Context) error { return nil }, func(string) {}, WithReassert())
	require.NotNil(t, tr)
	defer tr.Unregister()

	// The initial registration counts as one; re-assertion keeps going.
	assert.Eventually(t, func() bool {
		return registerCallCount.Load() >= 4
	}, time.Second, 5*time.Millisecond, "registration should be re-asserted repeatedly")
}

func TestReassert_DisabledByDefault(t *testing.T) {
	shortenReassert(t)

	var registerCallCount atomic.Int32
	register := func(ctx context.Context) error {
		registerCallCount.Add(1)
		return nil
	}

	// Without WithReassert (e.g. multipooler, which maintains its own
	// record) the registration is written exactly once.
	tr := Register(register, func(context.Context) error { return nil }, func(string) {})
	require.NotNil(t, tr)
	defer tr.Unregister()

	testpoll.Never(t, func() bool {
		return registerCallCount.Load() > 1
	}, 200*time.Millisecond, 10*time.Millisecond, "registration must not be rewritten without WithReassert")
}

func TestReassert_StopsBeforeUnregisterDeletes(t *testing.T) {
	shortenReassert(t)

	// Ordering guard: if re-assertion outlived the deregistration, it would
	// rewrite the record the component just removed — re-introducing the
	// stranded entry this whole mechanism exists to prevent.
	var registerCallCount atomic.Int32
	var unregistered atomic.Bool
	var reassertedAfterUnregister atomic.Bool

	register := func(ctx context.Context) error {
		registerCallCount.Add(1)
		if unregistered.Load() {
			reassertedAfterUnregister.Store(true)
		}
		return nil
	}
	unregister := func(ctx context.Context) error {
		unregistered.Store(true)
		return nil
	}

	tr := Register(register, unregister, func(string) {}, WithReassert())
	require.NotNil(t, tr)

	// Let the loop run a few times, then shut down.
	require.Eventually(t, func() bool {
		return registerCallCount.Load() >= 3
	}, time.Second, 5*time.Millisecond)
	tr.Unregister()

	testpoll.Never(t, func() bool {
		return reassertedAfterUnregister.Load()
	}, 200*time.Millisecond, 10*time.Millisecond, "re-assertion must stop before the record is deleted")
}

func TestReassert_SurvivesTransientFailures(t *testing.T) {
	shortenReassert(t)

	var registerCallCount atomic.Int32
	register := func(ctx context.Context) error {
		// Fail every other attempt; the loop must keep going regardless.
		if registerCallCount.Add(1)%2 == 0 {
			return errors.New("topology unavailable")
		}
		return nil
	}

	tr := Register(register, func(context.Context) error { return nil }, func(string) {}, WithReassert())
	require.NotNil(t, tr)
	defer tr.Unregister()

	assert.Eventually(t, func() bool {
		return registerCallCount.Load() >= 6
	}, time.Second, 5*time.Millisecond, "a failed re-assertion must not stop the loop")
}

func TestUnregister_RetriesUntilSuccess(t *testing.T) {
	var unregisterCallCount atomic.Int32

	register := func(ctx context.Context) error {
		return nil
	}

	unregister := func(ctx context.Context) error {
		if unregisterCallCount.Add(1) < 3 {
			return errors.New("unregister failed")
		}
		return nil
	}

	tr := Register(register, unregister, func(msg string) {})
	require.NotNil(t, tr)

	tr.Unregister()

	assert.Equal(t, int32(3), unregisterCallCount.Load(), "unregister should be retried until it succeeds")
}

func TestUnregister_GivesUpAfterBudget(t *testing.T) {
	oldBudget := unregisterBudget
	unregisterBudget = 300 * time.Millisecond
	defer func() { unregisterBudget = oldBudget }()

	var unregisterCallCount atomic.Int32

	register := func(ctx context.Context) error {
		return nil
	}

	unregister := func(ctx context.Context) error {
		unregisterCallCount.Add(1)
		return errors.New("unregister always fails")
	}

	tr := Register(register, unregister, func(msg string) {})
	require.NotNil(t, tr)

	// Must return (error is only logged), and must have retried within the budget.
	tr.Unregister()

	assert.Greater(t, unregisterCallCount.Load(), int32(1), "unregister should be retried before giving up")
}

func TestUnregister_NoNodeIsSuccess(t *testing.T) {
	var unregisterCallCount atomic.Int32

	register := func(ctx context.Context) error {
		return nil
	}

	// NoNode means the registration is already gone (e.g. a previous
	// attempt's delete was applied but its response was lost).
	unregister := func(ctx context.Context) error {
		unregisterCallCount.Add(1)
		return topoclient.NewError(topoclient.NoNode, "gateways/foo")
	}

	tr := Register(register, unregister, func(msg string) {})
	require.NotNil(t, tr)

	tr.Unregister()

	assert.Equal(t, int32(1), unregisterCallCount.Load(), "NoNode should count as success, not be retried")
}

func TestRegister_AlarmBehavior(t *testing.T) {
	var alarmMessages []string
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		return errors.New("specific error message")
	}

	unregister := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(alarmMessages) > 0 && strings.Contains(alarmMessages[0], "specific error message")
	}, 20*time.Millisecond, 10*time.Millisecond, "alarm should be called with specific error message")

	tr.Unregister()
}

func TestRegister_BackoffBehavior(t *testing.T) {
	var registerTimes []time.Time
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		registerTimes = append(registerTimes, time.Now())
		return errors.New("always fails")
	}

	unregister := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(registerTimes) >= 3
	}, 100*time.Millisecond, 50*time.Millisecond, "should have multiple register attempts")

	tr.Unregister()

	mu.Lock()
	if len(registerTimes) >= 3 {
		interval1 := registerTimes[1].Sub(registerTimes[0])
		interval2 := registerTimes[2].Sub(registerTimes[1])

		// With full jitter, the second interval (base 20ms) should generally be longer than first (base 10ms)
		// But with jitter, this isn't guaranteed in every run, so we just check they're both reasonable
		assert.Less(t, interval1, 20*time.Millisecond, "first retry should not be too long")
		assert.Greater(t, interval1, time.Duration(0), "first retry should have some delay")
		assert.Less(t, interval2, 40*time.Millisecond, "second retry should not be too long")

		// Check that on average, intervals increase (the mean of interval2 should be > interval1)
		// But with full jitter, individual samples can vary widely
		t.Logf("Backoff intervals: first=%v, second=%v", interval1, interval2)
	}
	mu.Unlock()
}
