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

package logthrottle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottle(t *testing.T) {
	const interval = time.Minute
	throttle := New(interval)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	ok, suppressed := throttle.Allow(start)
	assert.True(t, ok, "the first line is always logged")
	assert.Zero(t, suppressed)

	for i := 1; i <= 3; i++ {
		ok, _ = throttle.Allow(start.Add(time.Duration(i) * time.Second))
		assert.False(t, ok, "a line inside the interval is held back")
	}

	ok, suppressed = throttle.Allow(start.Add(interval))
	assert.True(t, ok, "a line at the interval is logged")
	assert.Equal(t, 3, suppressed, "and reports how many were held back")

	ok, suppressed = throttle.Allow(start.Add(interval + time.Second))
	assert.False(t, ok)
	assert.Zero(t, suppressed)

	ok, suppressed = throttle.Allow(start.Add(2 * interval))
	assert.True(t, ok)
	assert.Equal(t, 1, suppressed, "the count restarts after each logged line")
}
