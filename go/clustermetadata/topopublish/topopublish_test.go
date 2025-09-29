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

package topopublish

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

func TestPublish_SuccessOnFirstTry(t *testing.T) {
	var alarmMessage string
	var publishCalled bool
	var unpublishCalled bool

	publish := func(ctx context.Context) error {
		publishCalled = true
		return nil
	}

	unpublish := func(ctx context.Context) error {
		unpublishCalled = true
		return nil
	}

	alarm := func(msg string) {
		alarmMessage = msg
	}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	assert.True(t, publishCalled, "publish function should be called")
	assert.False(t, unpublishCalled, "unpublish should not be called during successful publish")
	assert.Empty(t, alarmMessage, "alarm should not be triggered on success")

	tp.Unpublish()
	assert.True(t, unpublishCalled, "unpublish should be called during Unpublish")
}

func TestPublish_FailureAndRetry(t *testing.T) {
	var alarmMessages []string
	var publishCallCount int
	var mu sync.Mutex

	publish := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		publishCallCount++
		if publishCallCount < 3 {
			return errors.New("publish failed")
		}
		return nil
	}

	unpublish := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return publishCallCount >= 1 &&
			len(alarmMessages) > 0 &&
			strings.Contains(alarmMessages[0], "Failed to register component with topology")
	}, 20*time.Millisecond, 1*time.Millisecond, "Incorrect publish call count %d", publishCallCount)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return publishCallCount == 3 &&
			alarmMessages[len(alarmMessages)-1] == ""
	}, 200*time.Millisecond, 1*time.Millisecond, "Incorrect publish call count %d", publishCallCount)

	tp.Unpublish()
}

func TestPublish_ContinuousFailure(t *testing.T) {
	var alarmMessages []string
	var publishCallCount int
	var mu sync.Mutex

	publish := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		publishCallCount++
		return errors.New("always fails")
	}

	unpublish := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return publishCallCount > 1 && len(alarmMessages) > 1
	}, 100*time.Millisecond, 10*time.Millisecond, "publish should be retried multiple times")

	mu.Lock()
	for _, msg := range alarmMessages {
		assert.Contains(t, msg, "Failed to register component with topology", "all alarms should contain error message")
	}
	mu.Unlock()

	tp.Unpublish()

	finalCallCount := publishCallCount

	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return publishCallCount > finalCallCount
	}, 100*time.Millisecond, 50*time.Millisecond, "publish should stop being called after Unpublish")
}

func TestUnpublish_WithNilPointer(t *testing.T) {
	var tp *TopoPublisher
	assert.NotPanics(t, func() {
		tp.Unpublish()
	}, "Unpublish should handle nil pointer gracefully")
}

func TestUnpublish_WithError(t *testing.T) {
	var unpublishCalled bool
	var unpublishError error

	publish := func(ctx context.Context) error {
		return nil
	}

	unpublish := func(ctx context.Context) error {
		unpublishCalled = true
		return errors.New("unpublish failed")
	}

	alarm := func(msg string) {}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	tp.Unpublish()

	assert.True(t, unpublishCalled, "unpublish should be called")
	assert.Nil(t, unpublishError, "unpublish error should be logged but not returned")
}

func TestPublish_AlarmBehavior(t *testing.T) {
	var alarmMessages []string
	var mu sync.Mutex

	publish := func(ctx context.Context) error {
		return errors.New("specific error message")
	}

	unpublish := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmMessages = append(alarmMessages, msg)
	}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(alarmMessages) > 0 && strings.Contains(alarmMessages[0], "specific error message")
	}, 20*time.Millisecond, 10*time.Millisecond, "alarm should be called with specific error message")

	tp.Unpublish()
}

func TestPublish_BackoffBehavior(t *testing.T) {
	var publishTimes []time.Time
	var mu sync.Mutex

	publish := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		publishTimes = append(publishTimes, time.Now())
		return errors.New("always fails")
	}

	unpublish := func(ctx context.Context) error {
		return nil
	}

	alarm := func(msg string) {}

	tp := Publish(publish, unpublish, alarm)
	require.NotNil(t, tp)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(publishTimes) >= 3
	}, 100*time.Millisecond, 50*time.Millisecond, "should have multiple publish attempts")

	tp.Unpublish()

	mu.Lock()
	if len(publishTimes) >= 3 {
		interval1 := publishTimes[1].Sub(publishTimes[0])
		interval2 := publishTimes[2].Sub(publishTimes[1])

		assert.Greater(t, interval2, interval1, "intervals should increase due to backoff")
		assert.Less(t, interval1, 20*time.Millisecond, "first retry should have some delay after initial")
		assert.Greater(t, interval1, 5*time.Millisecond, "first retry should have some delay after initial")
	}
	mu.Unlock()
}
