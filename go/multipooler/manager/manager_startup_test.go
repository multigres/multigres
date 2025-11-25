// Copyright 2025 Supabase, Inc.

package manager

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitUntilReady_Success verifies that WaitUntilReady returns immediately
// when the manager is already in Ready state
func TestWaitUntilReady_Success(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
	}

	pm := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)

	// Simulate immediate ready state
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.topoLoaded = true
	pm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := pm.WaitUntilReady(ctx)
	require.NoError(t, err)
}

// TestWaitUntilReady_Error verifies that WaitUntilReady returns an error
// when the manager is in Error state
func TestWaitUntilReady_Error(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
	}

	pm := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)

	// Simulate error state
	pm.mu.Lock()
	pm.state = ManagerStateError
	pm.stateError = assert.AnError
	pm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := pm.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is in error state")
}

// TestWaitUntilReady_Timeout verifies that WaitUntilReady returns a context error
// when the manager stays in Starting state and the context times out
func TestWaitUntilReady_Timeout(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
	}

	pm := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)

	// Leave in Starting state - will timeout

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := pm.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context")
}

// TestWaitUntilReady_ConcurrentCalls verifies that multiple goroutines can
// safely call WaitUntilReady concurrently without data races
func TestWaitUntilReady_ConcurrentCalls(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
	}

	pm := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)

	// Start multiple goroutines calling WaitUntilReady
	const numGoroutines = 10
	errChan := make(chan error, numGoroutines)

	ctx := context.Background()

	for range numGoroutines {
		go func() {
			err := pm.WaitUntilReady(ctx)
			errChan <- err
		}()
	}

	// Simulate state transition to Ready after a delay
	time.Sleep(50 * time.Millisecond)
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.topoLoaded = true
	pm.mu.Unlock()

	// Collect results
	for range numGoroutines {
		err := <-errChan
		require.NoError(t, err)
	}
}
