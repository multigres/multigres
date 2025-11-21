// Copyright 2025 Supabase, Inc.
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

package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"github.com/multigres/multigres/go/common/mterrors"
)

// actionLockKey is the context key for storing action lock information
type actionLockKey struct{}

// actionLockValue contains information about a lock acquisition
type actionLockValue struct {
	lockID    uint64
	operation string
	released  *atomic.Bool
}

// ActionLock is a wrapper around a semaphore that tracks ownership via Context.
// It ensures that only one goroutine can hold the lock at a time and provides
// mechanisms to assert that a given context holds the lock.
type ActionLock struct {
	sema      *semaphore.Weighted
	mu        sync.Mutex
	currentID uint64 // ID of current lock holder (0 if unlocked)
	nextID    uint64 // Counter for generating unique IDs
}

// NewActionLock creates a new ActionLock.
func NewActionLock() *ActionLock {
	return &ActionLock{
		sema:   semaphore.NewWeighted(1),
		nextID: 1, // Start at 1 so 0 can represent "unlocked"
	}
}

// Acquire acquires the action lock and returns a new context that proves ownership.
// The operation string is used for debugging/tracking purposes.
// Returns an error if the lock cannot be acquired (e.g., context cancelled) or
// if the provided context already holds the lock.
func (al *ActionLock) Acquire(ctx context.Context, operation string) (context.Context, error) {
	// Check if this context already holds the lock
	if val, ok := ctx.Value(actionLockKey{}).(*actionLockValue); ok {
		if !val.released.Load() {
			return ctx, fmt.Errorf("context already holds the action lock (operation: %s)", val.operation)
		}
	}

	// Try to acquire the semaphore
	if err := al.sema.Acquire(ctx, 1); err != nil {
		return ctx, mterrors.Wrap(err, "failed to acquire action lock")
	}

	// Generate a unique ID for this acquisition
	al.mu.Lock()
	lockID := al.nextID
	al.nextID++
	al.currentID = lockID
	al.mu.Unlock()

	// Create the lock value with a released flag
	releasedFlag := &atomic.Bool{}
	val := &actionLockValue{
		lockID:    lockID,
		operation: operation,
		released:  releasedFlag,
	}

	// Return a new context with the lock info
	return context.WithValue(ctx, actionLockKey{}, val), nil
}

// Release releases the action lock. It validates that the provided context
// holds the lock and panics if it doesn't (which indicates a programming error).
// After releasing, the context is marked as invalid so future assertions will fail.
func (al *ActionLock) Release(ctx context.Context) {
	val, ok := ctx.Value(actionLockKey{}).(*actionLockValue)
	if !ok {
		panic("Release called with context that has no action lock info")
	}

	// Check if already released
	if val.released.Load() {
		panic(fmt.Sprintf("Release called twice with same context (operation: %s)", val.operation))
	}

	// Verify this context holds the current lock
	al.mu.Lock()
	currentID := al.currentID
	al.mu.Unlock()

	if val.lockID != currentID {
		panic(fmt.Sprintf("Release called with context that doesn't hold the lock (operation: %s, lockID: %d, currentID: %d)",
			val.operation, val.lockID, currentID))
	}

	// Mark as released BEFORE actually releasing the semaphore
	// This ensures assertions fail immediately
	val.released.Store(true)

	al.mu.Lock()
	al.currentID = 0
	al.mu.Unlock()

	al.sema.Release(1)
}

// AssertActionLockHeld returns an error if the provided context does not hold
// an action lock or if the lock has been released. This is a global function
// that doesn't require a reference to the ActionLock.
func AssertActionLockHeld(ctx context.Context) error {
	val, ok := ctx.Value(actionLockKey{}).(*actionLockValue)
	if !ok {
		return fmt.Errorf("context does not hold an action lock")
	}

	if val.released.Load() {
		return fmt.Errorf("context's action lock has been released")
	}

	return nil
}
