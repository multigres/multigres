// Copyright 2019 The Vitess Authors.
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

package event

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHooks checks that hooks get triggered.
func TestHooks(t *testing.T) {
	triggered1 := false
	triggered2 := false

	var hooks Hooks
	hooks.Add(func() { triggered1 = true })
	hooks.Add(func() { triggered2 = true })

	hooks.Fire()

	if !triggered1 || !triggered2 {
		t.Errorf("registered hook functions failed to trigger on Fire()")
	}
}

// TestErrorHooks_AllSuccess checks that ErrorHooks returns nil when all hooks succeed.
func TestErrorHooks_AllSuccess(t *testing.T) {
	var count atomic.Int32
	var hooks ErrorHooks

	hooks.Add(func() error { count.Add(1); return nil })
	hooks.Add(func() error { count.Add(1); return nil })
	hooks.Add(func() error { count.Add(1); return nil })

	err := hooks.Fire()
	require.NoError(t, err)
	require.Equal(t, int32(3), count.Load(), "all hooks should have run")
}

// TestErrorHooks_ReturnsError checks that ErrorHooks returns an error when one hook fails.
func TestErrorHooks_ReturnsError(t *testing.T) {
	expectedErr := errors.New("hook failed")
	var hooks ErrorHooks

	hooks.Add(func() error { return nil })
	hooks.Add(func() error { return expectedErr })
	hooks.Add(func() error { return nil })

	err := hooks.Fire()
	require.Error(t, err)
}

// TestErrorHooks_Empty checks that ErrorHooks returns nil for empty list.
func TestErrorHooks_Empty(t *testing.T) {
	var hooks ErrorHooks
	err := hooks.Fire()
	require.NoError(t, err)
}

// TestErrorHooks_ParallelExecution checks that hooks run in parallel.
func TestErrorHooks_ParallelExecution(t *testing.T) {
	var started atomic.Int32
	done := make(chan struct{})
	var hooks ErrorHooks

	// Add hooks that signal when they start and wait for done
	for range 3 {
		hooks.Add(func() error {
			started.Add(1)
			<-done
			return nil
		})
	}

	// Fire in goroutine
	errCh := make(chan error)
	go func() {
		errCh <- hooks.Fire()
	}()

	// Wait for all hooks to start (proves parallel execution)
	require.Eventually(t, func() bool {
		return started.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond, "all hooks should start in parallel")

	// Release all hooks
	close(done)

	// Wait for Fire to complete
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Fire to complete")
	}
}
