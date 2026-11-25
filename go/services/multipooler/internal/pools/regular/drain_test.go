// Copyright 2026 Supabase, Inc.
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

package regular

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// RunKills runs every kill and joins the errors of those that failed.
func TestRunKills(t *testing.T) {
	notGone := errors.New("backend did not exit")
	var calls atomic.Int32
	ran, err := RunKills(context.Background(), 40, func(_ context.Context, i int) error {
		calls.Add(1)
		if i == 7 {
			return notGone
		}
		return nil
	})
	assert.Equal(t, 40, ran)
	assert.Equal(t, int32(40), calls.Load())
	assert.ErrorIs(t, err, notGone)
}

// Once ctx ends, RunKills starts no more kills and reports ctx's error.
func TestRunKillsStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	ran, err := RunKills(ctx, 1000, func(context.Context, int) error {
		if calls.Add(1) == 1 {
			cancel()
		}
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, ran, 1000, "no kill starts once the drain is cancelled")
	assert.Equal(t, int32(ran), calls.Load())
}
