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

package topoclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConn_Success(t *testing.T) {
	factory := newMockFactory()

	wrapper := NewWrapperConn(factory.newConn, nil)
	require.NotNil(t, wrapper, "Expected wrapper to be created")

	// Verify connection was established
	conn, err := wrapper.getConnection()
	assert.NoError(t, err, "Expected connection to be available")
	assert.NotNil(t, conn, "Expected connection to be non-nil")

	assert.Equal(t, int32(1), factory.getCreateCount(), "Expected 1 connection creation attempt")
}

func TestNewConn_InitialFailure(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	wrapper := NewWrapperConn(factory.newConn, nil)

	// Should not have a connection initially
	conn, err := wrapper.getConnection()
	assert.Error(t, err, "Expected error when no connection available")
	assert.Nil(t, conn, "Expected connection to be nil")
	// Count may be 1 or 2: the initial attempt plus possibly one immediate retry
	// from the background goroutine (first retry has no delay).
	count := factory.getCreateCount()
	assert.True(t, count == 1 || count == 2, "Expected 1 or 2 connection attempts, got %d", count)

	// Allow connection to succeed and wait for retry
	initialCount := factory.getCreateCount()
	factory.setShouldFail(false)
	factory.waitForNewConn(initialCount)

	conn, err = wrapper.getConnection()
	require.Eventually(t, func() bool {
		return err == nil && conn != nil
	}, 5*time.Second, 5*time.Millisecond)
}

func TestGetConnection_NoConnection(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	wrapper := NewWrapperConn(factory.newConn, nil)

	conn, err := wrapper.getConnection()
	assert.Error(t, err, "Expected error when no connection available")
	assert.Nil(t, conn, "Expected connection to be nil")

	assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error")
}

func TestHandleConnectionError_RetriesOnSpecificErrors(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{
			name: "UNAVAILABLE",
			err:  mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "test error"),
		},
		{
			name: "FAILED_PRECONDITION",
			err:  mterrors.Errorf(mtrpc.Code_FAILED_PRECONDITION, "test error"),
		},
		{
			name: "CLUSTER_EVENT",
			err:  mterrors.Errorf(mtrpc.Code_CLUSTER_EVENT, "test error"),
		},
		{
			name: "context deadline exceeded",
			err:  mterrors.Errorf(mtrpc.Code_INVALID_ARGUMENT, "context deadline exceeded"),
		},
		{
			name: "context canceled",
			err:  mterrors.Errorf(mtrpc.Code_INVALID_ARGUMENT, "context canceled"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			factory := newMockFactory()
			wrapper := NewWrapperConn(factory.newConn, nil)

			// Get initial connection
			conn, err := wrapper.getConnection()
			require.NoError(t, err, "Expected initial connection")

			initialCount := factory.getCreateCount()

			wrapper.handleConnectionError(conn, tc.err)

			factory.waitForNewConn(initialCount)
		})
	}
}

func TestHandleConnectionError_DoesNotRetryOnOtherErrors(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")

	initialCount := factory.getCreateCount()

	// Non-retriable error
	err = mterrors.Errorf(mtrpc.Code_INVALID_ARGUMENT, "test error")
	wrapper.handleConnectionError(conn, err)

	// Wait a bit
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, initialCount, factory.getCreateCount(), "Expected no retry for non-retriable error")
}

func TestAllMethods_Success(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	ctx := context.Background()

	// Test all methods for successful execution
	t.Run("ListDir", func(t *testing.T) {
		result, err := wrapper.ListDir(ctx, "/test", true)
		assert.NoError(t, err, "ListDir should not fail")
		assert.Len(t, result, 1, "Expected 1 result")
		assert.Equal(t, "test", result[0].Name, "Expected result name to be 'test'")
	})

	t.Run("Create", func(t *testing.T) {
		version, err := wrapper.Create(ctx, "/test", []byte("content"))
		assert.NoError(t, err, "Create should not fail")
		assert.Equal(t, "1", version.String(), "Expected version '1'")
	})

	t.Run("Update", func(t *testing.T) {
		version, err := wrapper.Update(ctx, "/test", []byte("content"), &mockVersion{version: "1"})
		assert.NoError(t, err, "Update should not fail")
		assert.Equal(t, "2", version.String(), "Expected version '2'")
	})

	t.Run("Get", func(t *testing.T) {
		data, version, err := wrapper.Get(ctx, "/test")
		assert.NoError(t, err, "Get should not fail")
		assert.Equal(t, "content", string(data), "Expected data to be 'content'")
		assert.Equal(t, "1", version.String(), "Expected version '1'")
	})

	t.Run("GetVersion", func(t *testing.T) {
		data, err := wrapper.GetVersion(ctx, "/test", 1)
		assert.NoError(t, err, "GetVersion should not fail")
		assert.Equal(t, "test", string(data), "Expected data to be 'test'")
	})

	t.Run("List", func(t *testing.T) {
		result, err := wrapper.List(ctx, "/test")
		assert.NoError(t, err, "List should not fail")
		assert.Len(t, result, 1, "Expected 1 result")
		assert.Equal(t, "key", string(result[0].Key), "Expected key to be 'key'")
	})

	t.Run("Delete", func(t *testing.T) {
		err := wrapper.Delete(ctx, "/test", &mockVersion{version: "1"})
		assert.NoError(t, err, "Delete should not fail")
	})

	t.Run("Lock", func(t *testing.T) {
		lock, err := wrapper.Lock(ctx, "/test", "content")
		assert.NoError(t, err, "Lock should not fail")
		assert.NotNil(t, lock, "Expected non-nil lock")
	})

	t.Run("LockWithTTL", func(t *testing.T) {
		lock, err := wrapper.LockWithTTL(ctx, "/test", "content", time.Second)
		assert.NoError(t, err, "LockWithTTL should not fail")
		assert.NotNil(t, lock, "Expected non-nil lock")
	})

	t.Run("LockName", func(t *testing.T) {
		lock, err := wrapper.LockName(ctx, "/test", "content")
		assert.NoError(t, err, "LockName should not fail")
		assert.NotNil(t, lock, "Expected non-nil lock")
	})

	t.Run("TryLock", func(t *testing.T) {
		lock, err := wrapper.TryLock(ctx, "/test", "content")
		assert.NoError(t, err, "TryLock should not fail")
		assert.NotNil(t, lock, "Expected non-nil lock")
	})

	t.Run("Watch", func(t *testing.T) {
		current, changes, err := wrapper.Watch(ctx, "/test")
		assert.NoError(t, err, "Watch should not fail")
		assert.NotNil(t, current, "Expected non-nil current data")
		assert.NotNil(t, changes, "Expected non-nil changes channel")
	})

	t.Run("WatchRecursive", func(t *testing.T) {
		current, changes, err := wrapper.WatchRecursive(ctx, "/test")
		assert.NoError(t, err, "WatchRecursive should not fail")
		assert.Len(t, current, 1, "Expected 1 current item")
		assert.NotNil(t, changes, "Expected non-nil changes channel")
	})
}

func TestAllMethods_NoConnection(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)

	ctx := context.Background()

	// Test all methods return error when no connection
	methods := []struct {
		name string
		fn   func() error
	}{
		{"ListDir", func() error {
			_, err := wrapper.ListDir(ctx, "/test", true)
			return err
		}},
		{"Create", func() error {
			_, err := wrapper.Create(ctx, "/test", []byte("content"))
			return err
		}},
		{"Update", func() error {
			_, err := wrapper.Update(ctx, "/test", []byte("content"), &mockVersion{version: "1"})
			return err
		}},
		{"Get", func() error {
			_, _, err := wrapper.Get(ctx, "/test")
			return err
		}},
		{"GetVersion", func() error {
			_, err := wrapper.GetVersion(ctx, "/test", 1)
			return err
		}},
		{"List", func() error {
			_, err := wrapper.List(ctx, "/test")
			return err
		}},
		{"Delete", func() error {
			return wrapper.Delete(ctx, "/test", &mockVersion{version: "1"})
		}},
		{"Lock", func() error {
			_, err := wrapper.Lock(ctx, "/test", "content")
			return err
		}},
		{"LockWithTTL", func() error {
			_, err := wrapper.LockWithTTL(ctx, "/test", "content", time.Second)
			return err
		}},
		{"LockName", func() error {
			_, err := wrapper.LockName(ctx, "/test", "content")
			return err
		}},
		{"TryLock", func() error {
			_, err := wrapper.TryLock(ctx, "/test", "content")
			return err
		}},
		{"Watch", func() error {
			_, _, err := wrapper.Watch(ctx, "/test")
			return err
		}},
		{"WatchRecursive", func() error {
			_, _, err := wrapper.WatchRecursive(ctx, "/test")
			return err
		}},
	}

	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			err := method.fn()
			assert.Error(t, err, "Expected error for %s when no connection", method.name)
			assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error for %s", method.name)
		})
	}
}

func TestAllMethods_ConnectionError(t *testing.T) {
	ctx := context.Background()

	// Test all methods trigger retry on connection error
	methods := []struct {
		name string
		fn   func(wrapper *WrapperConn) error
	}{
		{"ListDir", func(wrapper *WrapperConn) error {
			_, err := wrapper.ListDir(ctx, "/test", true)
			return err
		}},
		{"Create", func(wrapper *WrapperConn) error {
			_, err := wrapper.Create(ctx, "/test", []byte("content"))
			return err
		}},
		{"Update", func(wrapper *WrapperConn) error {
			_, err := wrapper.Update(ctx, "/test", []byte("content"), &mockVersion{version: "1"})
			return err
		}},
		{"Get", func(wrapper *WrapperConn) error {
			_, _, err := wrapper.Get(ctx, "/test")
			return err
		}},
		{"GetVersion", func(wrapper *WrapperConn) error {
			_, err := wrapper.GetVersion(ctx, "/test", 1)
			return err
		}},
		{"List", func(wrapper *WrapperConn) error {
			_, err := wrapper.List(ctx, "/test")
			return err
		}},
		{"Delete", func(wrapper *WrapperConn) error {
			return wrapper.Delete(ctx, "/test", &mockVersion{version: "1"})
		}},
		{"Lock", func(wrapper *WrapperConn) error {
			_, err := wrapper.Lock(ctx, "/test", "content")
			return err
		}},
		{"LockWithTTL", func(wrapper *WrapperConn) error {
			_, err := wrapper.LockWithTTL(ctx, "/test", "content", time.Second)
			return err
		}},
		{"LockName", func(wrapper *WrapperConn) error {
			_, err := wrapper.LockName(ctx, "/test", "content")
			return err
		}},
		{"TryLock", func(wrapper *WrapperConn) error {
			_, err := wrapper.TryLock(ctx, "/test", "content")
			return err
		}},
		{"Watch", func(wrapper *WrapperConn) error {
			_, _, err := wrapper.Watch(ctx, "/test")
			return err
		}},
		{"WatchRecursive", func(wrapper *WrapperConn) error {
			_, _, err := wrapper.WatchRecursive(ctx, "/test")
			return err
		}},
	}

	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			// Create a new factory for every test to avoid shared state
			// between sub-tests that can cause race conditions with createCount
			factory := newMockFactory()

			// Create a new wrapper for every test because
			// a call to handleConnection will affect the code path of
			// subsequent tests.
			wrapper := NewWrapperConn(factory.newConn, nil)
			defer wrapper.Close()

			// Get the connection and make it fail
			conn, _ := wrapper.getConnection()
			mockConn := conn.(*mockConn)
			mockConn.setShouldFailCalls(true)

			initialCount := factory.getCreateCount()
			err := method.fn(wrapper)
			assert.Error(t, err, "Expected error for %s when connection fails", method.name)
			assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error for %s", method.name)

			factory.waitForNewConn(initialCount)
		})
	}
}

func TestClose(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	// Verify connection exists
	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection")

	mockConn := conn.(*mockConn)

	// Close wrapper
	err = wrapper.Close()
	assert.NoError(t, err, "Close should not fail")

	// Verify underlying connection was closed
	assert.True(t, mockConn.closed, "Expected underlying connection to be closed")

	// Verify wrapper has no connection
	conn, err = wrapper.getConnection()
	assert.Error(t, err, "Expected error after close")
	assert.Nil(t, conn, "Expected nil connection after close")
}

func TestClose_NoConnection(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)

	// Should not fail even with no connection
	err := wrapper.Close()
	assert.NoError(t, err, "Close should not fail when no connection")
}

func TestHandleConnectionError_NilError(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection")

	initialCount := factory.getCreateCount()

	// Nil error should not trigger retry
	wrapper.handleConnectionError(conn, nil)

	// Wait a bit
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, initialCount, factory.getCreateCount(), "Expected no retry for nil error")
}

// mockConnWithDelayedFailure is a mock that can be configured to fail after N calls
type mockConnWithDelayedFailure struct {
	*mockConn
	failAfterCalls int32
	callsSoFar     int32
}

func newMockConnWithDelayedFailure(id int, failAfterCalls int32) *mockConnWithDelayedFailure {
	return &mockConnWithDelayedFailure{
		mockConn:       newMockConn(id),
		failAfterCalls: failAfterCalls,
	}
}

func (m *mockConnWithDelayedFailure) checkErrorWithDelay() error {
	calls := atomic.AddInt32(&m.callsSoFar, 1)
	if calls > m.failAfterCalls {
		return mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "delayed failure")
	}
	return m.checkError()
}

func (m *mockConnWithDelayedFailure) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return []DirEntry{{Name: "test"}}, nil
}

func (m *mockConnWithDelayedFailure) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, nil, err
	}
	return []byte("test"), &mockVersion{version: "1"}, nil
}

func (m *mockConnWithDelayedFailure) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "1"}, nil
}

func (m *mockConnWithDelayedFailure) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "2"}, nil
}

func (m *mockConnWithDelayedFailure) Delete(ctx context.Context, filePath string, version Version) error {
	return m.checkErrorWithDelay()
}

// mockFactoryWithDelayedFailure creates connections that fail after a certain number of calls
type mockFactoryWithDelayedFailure struct {
	mu             sync.Mutex
	createCount    int32
	connections    []*mockConnWithDelayedFailure
	failAfterCalls int32
}

func newMockFactoryWithDelayedFailure(failAfterCalls int32) *mockFactoryWithDelayedFailure {
	return &mockFactoryWithDelayedFailure{
		failAfterCalls: failAfterCalls,
	}
}

func (f *mockFactoryWithDelayedFailure) getCreateCount() int32 {
	return atomic.LoadInt32(&f.createCount)
}

func (f *mockFactoryWithDelayedFailure) newConn() (Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := atomic.AddInt32(&f.createCount, 1)
	conn := newMockConnWithDelayedFailure(int(count), f.failAfterCalls)
	f.connections = append(f.connections, conn)
	return conn, nil
}

func (f *mockFactoryWithDelayedFailure) waitForNewConn(currentCount int32) {
	for f.getCreateCount() == currentCount {
		time.Sleep(time.Millisecond)
	}
}

func TestOperationsTriggersHandleConnectionError(t *testing.T) {
	// Create a connection that will fail after 2 calls
	factory := newMockFactoryWithDelayedFailure(2)
	wrapper := NewWrapperConn(factory.newConn, nil)

	ctx := context.Background()

	// First call should succeed
	_, err := wrapper.ListDir(ctx, "/test", true)
	assert.NoError(t, err, "First ListDir should succeed")

	// Second call should succeed
	_, err = wrapper.ListDir(ctx, "/test", true)
	assert.NoError(t, err, "Second ListDir should succeed")

	initialCount := factory.getCreateCount()

	// Third call should fail and trigger retry
	_, err = wrapper.ListDir(ctx, "/test", true)
	assert.Error(t, err, "Third ListDir should fail")
	assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error")

	factory.waitForNewConn(initialCount)

	// New connection should work
	_, err = wrapper.ListDir(ctx, "/test", true)
	assert.NoError(t, err, "ListDir with new connection should succeed")
}

func TestMultipleOperationsWithConnectionErrors(t *testing.T) {
	// Test multiple different operations trigger handleConnectionError
	operations := []struct {
		name string
		fn   func(*WrapperConn, context.Context) error
	}{
		{"ListDir", func(c *WrapperConn, ctx context.Context) error {
			_, err := c.ListDir(ctx, "/test", true)
			return err
		}},
		{"Get", func(c *WrapperConn, ctx context.Context) error {
			_, _, err := c.Get(ctx, "/test")
			return err
		}},
		{"Create", func(c *WrapperConn, ctx context.Context) error {
			_, err := c.Create(ctx, "/test", []byte("data"))
			return err
		}},
		{"Update", func(c *WrapperConn, ctx context.Context) error {
			_, err := c.Update(ctx, "/test", []byte("data"), &mockVersion{version: "1"})
			return err
		}},
		{"Delete", func(c *WrapperConn, ctx context.Context) error {
			return c.Delete(ctx, "/test", &mockVersion{version: "1"})
		}},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			// Create a connection that fails immediately
			factory := newMockFactoryWithDelayedFailure(0)
			wrapper := NewWrapperConn(factory.newConn, nil)

			ctx := context.Background()
			initialCount := factory.getCreateCount()

			// Operation should fail and trigger retry
			err := op.fn(wrapper, ctx)
			assert.Error(t, err, "Operation %s should fail", op.name)
			assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error for %s", op.name)

			factory.waitForNewConn(initialCount)
		})
	}
}

func TestRetryConnection_PreventsMultipleRetries(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	// Set factory to fail connections and make the wrapper go into retry
	factory.setShouldFail(true)
	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")

	// Trigger a retry by simulating a connection error
	wrapper.handleConnectionError(conn, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "test error"))

	// Wait for retry to start
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.retrying
	}, 50*time.Millisecond, 5*time.Millisecond, "Expected retrying to become true")

	// Change c.wrapped to a non-nil value
	mockConn := newMockConn(999)
	wrapper.mu.Lock()
	wrapper.wrapped = mockConn
	wrapper.mu.Unlock()

	// Generate another failure to make it try to retry
	wrapper.handleConnectionError(mockConn, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "another error"))

	// Give time for retry to be invoked.
	time.Sleep(1 * time.Millisecond)

	// Verify that c.wrapped is still non-nil (retry was prevented)
	wrapper.mu.Lock()
	currentConn := wrapper.wrapped
	wrapper.mu.Unlock()
	assert.Equal(t, mockConn, currentConn, "Connection should still be the same - retry was prevented")

	// Add a third retry attempt to demonstrate the bug
	// This will reset the retrying flag due to the defer at the top of retryConnection
	thirdMockConn := newMockConn(1000)
	wrapper.mu.Lock()
	wrapper.wrapped = thirdMockConn
	wrapper.mu.Unlock()

	// Generate a third failure - this should demonstrate that the bug
	// is now fixed where the retry flag was getting always reset, even
	// if it was already on.
	wrapper.handleConnectionError(thirdMockConn, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "third error"))

	// Give time for the third retry to process
	time.Sleep(1 * time.Millisecond)

	// Verify the third retry was also prevented (fix working correctly)
	wrapper.mu.Lock()
	finalConn := wrapper.wrapped
	retryingStatus := wrapper.retrying
	wrapper.mu.Unlock()

	// With the fix, the third retry should also be prevented
	// The connection should still be thirdMockConn (not reset to nil)
	assert.Equal(t, thirdMockConn, finalConn, "Third retry should have been prevented - connection should remain intact")
	assert.True(t, retryingStatus, "Should still be retrying")

	// Reset c.wrapped to nil
	wrapper.mu.Lock()
	wrapper.wrapped = nil
	wrapper.mu.Unlock()

	// Set factory to succeed
	factory.setShouldFail(false)

	// Verify that c.retrying becomes false (retry completes successfully)
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return !wrapper.retrying && wrapper.wrapped != nil
	}, 100*time.Millisecond, 5*time.Millisecond, "Expected retrying to become false and connection established")

	wrapper.Close()
}

func TestRetryConnection_TerminatesWhenClosed(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)

	// Get the initial connection and manually trigger a retry with it
	_, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")
	initialCount := factory.getCreateCount()

	// Ensure that retry will keep failing.
	factory.setShouldFail(true)

	// Start retryConnection manually in a goroutine
	done := make(chan bool, 1)
	go func() {
		wrapper.retryConnection(errors.New("test error"))
		done <- true
	}()

	factory.waitForNewConn(initialCount)

	// Close the wrapper - this should terminate retryConnection
	err = wrapper.Close()
	assert.NoError(t, err, "Close should not fail")

	// Wait for retryConnection to complete or timeout
	select {
	case <-done:
		// retryConnection completed successfully - this is what we want
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "retryConnection did not terminate within expected time after close")
	}

	// Verify wrapper is closed
	_, err = wrapper.getConnection()
	assert.Error(t, err, "Expected error after close")
	assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error after close")
}

func TestRetryConnection_TerminatesWhenSuccessful(t *testing.T) {
	factory := newMockFactory()

	// Start with failures
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)
	initialCount := factory.getCreateCount()
	// Wait for retryConnection to start attempting
	factory.waitForNewConn(initialCount)

	// Allow connections to succeed
	initialCount = factory.getCreateCount()
	factory.setShouldFail(false)

	// Wait for retryConnection to terminate
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return !wrapper.retrying
	}, 100*time.Millisecond, time.Millisecond)

	// Wait for successful connection
	factory.waitForNewConn(initialCount)

	// Verify connection is available
	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection after factory allows success")
	require.NotNil(t, conn, "Expected non-nil connection after factory allows success")

	successCount := factory.getCreateCount()

	// Wait for retryConnection to terminate
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return !wrapper.retrying
	}, 100*time.Millisecond, time.Millisecond)

	// Verify no additional connection attempts were made
	assert.LessOrEqual(t, factory.getCreateCount(), successCount, "retryConnection should have terminated after successful connection")
}

func TestAlarm_CalledOnRetryStart(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	alarmCalls := make([]string, 0)
	var mu sync.Mutex
	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmCalls = append(alarmCalls, msg)
	}

	wrapper := NewWrapperConn(factory.newConn, alarm)

	// Wait for retry to start
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(alarmCalls) > 0
	}, 100*time.Millisecond, 5*time.Millisecond, "Expected alarm to be called")

	// Verify alarm was called with error message
	mu.Lock()
	assert.Greater(t, len(alarmCalls), 0, "Alarm should be called")
	assert.NotEmpty(t, alarmCalls[0], "First alarm call should have error message")
	assert.Contains(t, alarmCalls[0], "factory error", "Alarm should contain error message")
	// Don't defer. It causes deadlock with wrapper.Close().
	mu.Unlock()

	wrapper.Close()
}

func TestAlarm_ResetOnRetrySuccess(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	alarmCalls := make([]string, 0)
	var mu sync.Mutex
	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmCalls = append(alarmCalls, msg)
	}

	wrapper := NewWrapperConn(factory.newConn, alarm)

	// Wait for retry to start
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(alarmCalls) > 0
	}, 100*time.Millisecond, 5*time.Millisecond, "Expected alarm to be called")

	// Allow connection to succeed
	factory.setShouldFail(false)

	// Wait for connection to be established and alarm to be reset
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		// Check if we have at least 2 calls and the last one is empty
		if len(alarmCalls) < 2 {
			return false
		}
		return alarmCalls[len(alarmCalls)-1] == ""
	}, 2*time.Second, 10*time.Millisecond, "Expected alarm to be reset")

	// Verify alarm was called with error and then reset with empty string
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(alarmCalls), 2, "Alarm should be called at least twice")
	assert.NotEmpty(t, alarmCalls[0], "First call should have error message")
	assert.Empty(t, alarmCalls[len(alarmCalls)-1], "Last call should be empty string to reset alarm")

	wrapper.Close()
}

func TestAlarm_CalledOnConnectionError(t *testing.T) {
	factory := newMockFactory()

	alarmCalls := make([]string, 0)
	var mu sync.Mutex
	alarm := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		alarmCalls = append(alarmCalls, msg)
	}

	wrapper := NewWrapperConn(factory.newConn, alarm)
	defer wrapper.Close()

	// Get the connection and make it fail
	conn, err := wrapper.getConnection()
	require.NoError(t, err)

	mockConn := conn.(*mockConn)
	mockConn.setShouldFailCalls(true)

	ctx := context.Background()

	// Trigger a connection error
	_, err = wrapper.ListDir(ctx, "/test", true)
	assert.Error(t, err, "Expected error from failing connection")

	// Wait for alarm to be called
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(alarmCalls) > 0
	}, 100*time.Millisecond, 5*time.Millisecond, "Expected alarm to be called after connection error")

	// Verify alarm was called with error
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, len(alarmCalls), 0, "Alarm should be called")
	assert.NotEmpty(t, alarmCalls[0], "Alarm should have error message")
	assert.Contains(t, alarmCalls[0], "connection error", "Alarm should contain connection error")
}

func TestAlarm_NilAlarmDoesNotPanic(t *testing.T) {
	factory := newMockFactory()

	// Create wrapper with nil alarm - should not panic
	wrapper := NewWrapperConn(factory.newConn, nil)
	defer wrapper.Close()

	// Verify it works normally
	conn, err := wrapper.getConnection()
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestRetryConnection_ClosesStrayConnectionWhenWrapperClosed(t *testing.T) {
	factory := newMockFactory()
	// Make NewConn go into a retry loop.
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)
	initialCount := factory.getCreateCount()

	// Wait for at least one more connection attempt to be sure
	// that we are well into the retry loop.
	factory.waitForNewConn(initialCount)

	// Now block creation of a new connection by obtaining the lock
	// and set it to succeed on the next attempt.
	factory.mu.Lock()
	factory.shouldFail = false
	initialCount = factory.getCreateCount()
	connectionCount := len(factory.connections)

	// Before releasing the factory lock, close the wrapper connection.
	_ = wrapper.Close()

	// Now release the lock. This will open a successful connection,
	// notice that the wrapper was closed. But we have to ensure that
	// the stray connection was closed.
	factory.mu.Unlock()

	factory.waitForNewConn(initialCount)

	// Give time for the retry routine to close the connection.
	time.Sleep(5 * time.Millisecond)

	factory.mu.Lock()
	defer factory.mu.Unlock()
	conn := factory.connections[connectionCount]
	conn.mu.Lock()
	defer conn.mu.Unlock()
	assert.True(t, conn.closed, "Connection should be closed")
}

// typedNilMockFactory mimics the behavior of etcd's factory which can return
// a typed nil (*etcdtopo)(nil) that becomes a non-nil interface value.
type typedNilMockFactory struct {
	mu         sync.Mutex
	shouldFail bool
	connID     int32
}

func newTypedNilMockFactory() *typedNilMockFactory {
	return &typedNilMockFactory{}
}

func (f *typedNilMockFactory) setShouldFail(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shouldFail = fail
}

func (f *typedNilMockFactory) newConn() (Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Declare conn as concrete type to create typed nil
	var conn *mockConn
	if f.shouldFail {
		// Returns typed nil (*mockConn)(nil) wrapped in Conn interface.
		// When this is assigned to Conn interface, it becomes a non-nil
		// interface with nil underlying value - the classic Go gotcha.
		return conn, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "factory error")
	}

	id := atomic.AddInt32(&f.connID, 1)
	conn = newMockConn(int(id))
	return conn, nil
}

// TestRetryConnection_HandlesTypedNilInterface is a regression test for a bug
// where closing the wrapper during retry could panic if the factory returned
// a typed nil interface. This mimics the behavior of etcd's factory.Create()
// which returns (*etcdtopo, error) and can return (*etcdtopo)(nil) on error.
func TestRetryConnection_HandlesTypedNilInterface(t *testing.T) {
	factory := newTypedNilMockFactory()
	// Make newConn return typed nil and go into retry loop
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)

	// Wait for retry to start
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.retrying
	}, 100*time.Millisecond, time.Millisecond, "Expected retry to start")

	// Lock factory and prepare to return typed nil on next attempt
	factory.mu.Lock()
	// Keep shouldFail=true so next attempt returns typed nil

	// Close wrapper before releasing lock
	_ = wrapper.Close()

	// Release lock - this will create a typed nil conn with err != nil
	factory.mu.Unlock()

	// Verify retry loop terminates without panic.
	// Before the fix, this would panic trying to call Close() on typed nil.
	// After the fix, it checks err first and never calls Close().
	require.Eventually(t, func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		// Retry should stop when wrapper is closed
		return !wrapper.retrying
	}, 100*time.Millisecond, time.Millisecond, "Expected retry to terminate after wrapper closed")
}
