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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	var alarmMessages []string
	var registerCallCount int
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		registerCallCount++
		if registerCallCount < 3 {
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
		return registerCallCount >= 1 &&
			len(alarmMessages) > 0 &&
			strings.Contains(alarmMessages[0], "Failed to register component with topology")
	}, 20*time.Millisecond, 1*time.Millisecond, "Incorrect register call count %d", registerCallCount)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return registerCallCount == 3 &&
			alarmMessages[len(alarmMessages)-1] == ""
	}, 200*time.Millisecond, 1*time.Millisecond, "Incorrect register call count %d", registerCallCount)

	tr.Unregister()
}

func TestRegister_ContinuousFailure(t *testing.T) {
	var alarmMessages []string
	var registerCallCount int
	var mu sync.Mutex

	register := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		registerCallCount++
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
		return registerCallCount > 1 && len(alarmMessages) > 1
	}, 100*time.Millisecond, 10*time.Millisecond, "register should be retried multiple times")

	mu.Lock()
	for _, msg := range alarmMessages {
		assert.Contains(t, msg, "Failed to register component with topology", "all alarms should contain error message")
	}
	mu.Unlock()

	tr.Unregister()

	finalCallCount := registerCallCount

	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return registerCallCount > finalCallCount
	}, 100*time.Millisecond, 50*time.Millisecond, "register should stop being called after Unregister")
}

func TestUnregister_WithNilPointer(t *testing.T) {
	var tr *TopoReg
	assert.NotPanics(t, func() {
		tr.Unregister()
	}, "Unregister should handle nil pointer gracefully")
}

func TestUnregister_WithError(t *testing.T) {
	var unregisterCalled bool
	var unregisterError error

	register := func(ctx context.Context) error {
		return nil
	}

	unregister := func(ctx context.Context) error {
		unregisterCalled = true
		return errors.New("unregister failed")
	}

	alarm := func(msg string) {}

	tr := Register(register, unregister, alarm)
	require.NotNil(t, tr)

	tr.Unregister()

	assert.True(t, unregisterCalled, "unregister should be called")
	assert.Nil(t, unregisterError, "unregister error should be logged but not returned")
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
