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

package topowrapper

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConn is a mock implementation of topo.Conn for testing
type mockConn struct {
	id              int
	closed          bool
	shouldFailCalls bool
	mu              sync.Mutex
}

func newMockConn(id int) *mockConn {
	return &mockConn{id: id}
}

func (m *mockConn) setShouldFailCalls(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFailCalls = fail
}

func (m *mockConn) checkError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldFailCalls {
		return mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "connection error")
	}
	if m.closed {
		return mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "connection closed")
	}
	return nil
}

func (m *mockConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []topo.DirEntry{{Name: "test"}}, nil
}

func (m *mockConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "1"}, nil
}

func (m *mockConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "2"}, nil
}

func (m *mockConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	return []byte("test"), &mockVersion{version: "1"}, nil
}

func (m *mockConn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []byte("test"), nil
}

func (m *mockConn) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []topo.KVInfo{{Key: []byte("key"), Value: []byte("value")}}, nil
}

func (m *mockConn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	return m.checkError()
}

func (m *mockConn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, err error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	ch := make(chan *topo.WatchData, 1)
	close(ch)
	return &topo.WatchData{Contents: []byte("test")}, ch, nil
}

func (m *mockConn) WatchRecursive(ctx context.Context, path string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	ch := make(chan *topo.WatchDataRecursive, 1)
	close(ch)
	return []*topo.WatchDataRecursive{{Path: "test"}}, ch, nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// mockVersion implements topo.Version
type mockVersion struct {
	version string
}

func (v *mockVersion) String() string {
	return v.version
}

// mockLockDescriptor implements topo.LockDescriptor
type mockLockDescriptor struct{}

func (l *mockLockDescriptor) Check(ctx context.Context) error {
	return nil
}

func (l *mockLockDescriptor) Unlock(ctx context.Context) error {
	return nil
}

// mockFactory creates mock connections with controllable failure behavior
type mockFactory struct {
	mu          sync.Mutex
	shouldFail  bool
	createCount int32
	connections []*mockConn
}

func newMockFactory() *mockFactory {
	return &mockFactory{}
}

func (f *mockFactory) setShouldFail(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shouldFail = fail
}

func (f *mockFactory) getCreateCount() int32 {
	return atomic.LoadInt32(&f.createCount)
}

func (f *mockFactory) getConnections() []*mockConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*mockConn, len(f.connections))
	copy(result, f.connections)
	return result
}

func (f *mockFactory) newConn() (topo.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := atomic.AddInt32(&f.createCount, 1)

	if f.shouldFail {
		return nil, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "factory error")
	}

	conn := newMockConn(int(count))
	f.connections = append(f.connections, conn)
	return conn, nil
}

func TestNewConn_Success(t *testing.T) {
	factory := newMockFactory()

	wrapper := NewConn(factory.newConn)
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

	wrapper := NewConn(factory.newConn)

	// Should not have a connection initially
	conn, err := wrapper.getConnection()
	assert.Error(t, err, "Expected error when no connection available")
	assert.Nil(t, conn, "Expected connection to be nil")
	assert.Equal(t, int32(1), factory.getCreateCount(), "Expected 1 connection creation attempt")

	// Allow connection to succeed and wait for retry
	factory.setShouldFail(false)

	// Wait for retry to succeed
	time.Sleep(5 * time.Millisecond)

	conn, err = wrapper.getConnection()
	assert.NoError(t, err, "Expected connection to be available after retry")
	assert.NotNil(t, conn, "Expected connection to be non-nil after retry")

	assert.GreaterOrEqual(t, factory.getCreateCount(), int32(2), "Expected at least 2 connection attempts")
}

func TestGetConnection_NoConnection(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	wrapper := NewConn(factory.newConn)

	conn, err := wrapper.getConnection()
	assert.Error(t, err, "Expected error when no connection available")
	assert.Nil(t, conn, "Expected connection to be nil")

	assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error")
}

func TestHandleConnectionError_RetriesOnSpecificErrors(t *testing.T) {
	testCases := []mtrpc.Code{
		mtrpc.Code_UNAVAILABLE,
		mtrpc.Code_FAILED_PRECONDITION,
		mtrpc.Code_CLUSTER_EVENT,
	}

	for _, code := range testCases {
		t.Run(code.String(), func(t *testing.T) {
			factory := newMockFactory()
			wrapper := NewConn(factory.newConn)

			// Get initial connection
			conn, err := wrapper.getConnection()
			require.NoError(t, err, "Expected initial connection")

			initialCount := factory.getCreateCount()

			err = mterrors.Errorf(code, "test error")
			wrapper.handleConnectionError(conn, err)

			// Wait a bit for retry to kick in
			time.Sleep(5 * time.Millisecond)

			assert.Greater(t, factory.getCreateCount(), initialCount, "Expected retry for error code %v", code)
		})
	}
}

func TestHandleConnectionError_DoesNotRetryOnOtherErrors(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

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

func TestRetryConnection_DoesNotReplaceNewerConnection(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

	// Get initial connection
	oldConn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")

	// Manually set a new connection
	newConn, err := factory.newConn()
	require.NoError(t, err, "Failed to create new connection")
	wrapper.mu.Lock()
	wrapper.wrapped = newConn
	wrapper.mu.Unlock()

	initialCount := factory.getCreateCount()

	// Try to retry with old connection - should do nothing
	wrapper.retryConnection(oldConn)

	// Verify no new connections were created
	assert.Equal(t, initialCount, factory.getCreateCount(), "Expected no new connections")

	// Verify current connection is still the new one
	currentConn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected to get current connection")
	assert.Equal(t, newConn, currentConn, "Expected connection to remain the newer one")
}

func TestAllMethods_Success(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

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
		assert.Equal(t, "test", string(data), "Expected data to be 'test'")
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
	wrapper := NewConn(factory.newConn)

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
	factory := newMockFactory()

	ctx := context.Background()

	// Test all methods trigger retry on connection error
	methods := []struct {
		name string
		fn   func(wrapper *Conn) error
	}{
		{"ListDir", func(wrapper *Conn) error {
			_, err := wrapper.ListDir(ctx, "/test", true)
			return err
		}},
		{"Create", func(wrapper *Conn) error {
			_, err := wrapper.Create(ctx, "/test", []byte("content"))
			return err
		}},
		{"Update", func(wrapper *Conn) error {
			_, err := wrapper.Update(ctx, "/test", []byte("content"), &mockVersion{version: "1"})
			return err
		}},
		{"Get", func(wrapper *Conn) error {
			_, _, err := wrapper.Get(ctx, "/test")
			return err
		}},
		{"GetVersion", func(wrapper *Conn) error {
			_, err := wrapper.GetVersion(ctx, "/test", 1)
			return err
		}},
		{"List", func(wrapper *Conn) error {
			_, err := wrapper.List(ctx, "/test")
			return err
		}},
		{"Delete", func(wrapper *Conn) error {
			return wrapper.Delete(ctx, "/test", &mockVersion{version: "1"})
		}},
		{"Lock", func(wrapper *Conn) error {
			_, err := wrapper.Lock(ctx, "/test", "content")
			return err
		}},
		{"LockWithTTL", func(wrapper *Conn) error {
			_, err := wrapper.LockWithTTL(ctx, "/test", "content", time.Second)
			return err
		}},
		{"LockName", func(wrapper *Conn) error {
			_, err := wrapper.LockName(ctx, "/test", "content")
			return err
		}},
		{"TryLock", func(wrapper *Conn) error {
			_, err := wrapper.TryLock(ctx, "/test", "content")
			return err
		}},
		{"Watch", func(wrapper *Conn) error {
			_, _, err := wrapper.Watch(ctx, "/test")
			return err
		}},
		{"WatchRecursive", func(wrapper *Conn) error {
			_, _, err := wrapper.WatchRecursive(ctx, "/test")
			return err
		}},
	}

	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			// Create a new wrapper for every test because
			// a call to handleConnection will affect the code path of
			// subsequent tests.
			wrapper := NewConn(factory.newConn)
			defer wrapper.Close()

			// Get the connection and make it fail
			conn, _ := wrapper.getConnection()
			mockConn := conn.(*mockConn)
			mockConn.setShouldFailCalls(true)

			initialCount := factory.getCreateCount()
			err := method.fn(wrapper)
			assert.Error(t, err, "Expected error for %s when connection fails", method.name)
			assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error for %s", method.name)

			// Wait for retries to happen
			time.Sleep(5 * time.Millisecond)

			// Should have attempted retries
			assert.Greater(t, factory.getCreateCount(), initialCount, "Expected retry attempts after connection errors")
		})
	}
}

func TestClose(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

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
	wrapper := NewConn(factory.newConn)

	// Should not fail even with no connection
	err := wrapper.Close()
	assert.NoError(t, err, "Close should not fail when no connection")
}

func TestHandleConnectionError_NilError(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection")

	initialCount := factory.getCreateCount()

	// Nil error should not trigger retry
	wrapper.handleConnectionError(conn, nil)

	// Wait a bit
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, initialCount, factory.getCreateCount(), "Expected no retry for nil error")
}

func TestHandleConnectionError_NonRetriableError(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection")

	initialCount := factory.getCreateCount()

	err = mterrors.Errorf(mtrpc.Code_INVALID_ARGUMENT, "test error")
	wrapper.handleConnectionError(conn, err)

	// Wait a bit
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, initialCount, factory.getCreateCount(), "Expected no retry for error code %v", mtrpc.Code_INVALID_ARGUMENT)
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

func (m *mockConnWithDelayedFailure) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return []topo.DirEntry{{Name: "test"}}, nil
}

func (m *mockConnWithDelayedFailure) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, nil, err
	}
	return []byte("test"), &mockVersion{version: "1"}, nil
}

func (m *mockConnWithDelayedFailure) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "1"}, nil
}

func (m *mockConnWithDelayedFailure) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if err := m.checkErrorWithDelay(); err != nil {
		return nil, err
	}
	return &mockVersion{version: "2"}, nil
}

func (m *mockConnWithDelayedFailure) Delete(ctx context.Context, filePath string, version topo.Version) error {
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

func (f *mockFactoryWithDelayedFailure) newConn() (topo.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := atomic.AddInt32(&f.createCount, 1)
	conn := newMockConnWithDelayedFailure(int(count), f.failAfterCalls)
	f.connections = append(f.connections, conn)
	return conn, nil
}

func TestOperationsTriggersHandleConnectionError(t *testing.T) {
	// Create a connection that will fail after 2 calls
	factory := newMockFactoryWithDelayedFailure(2)
	wrapper := NewConn(factory.newConn)

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

	// Wait for retry to kick in
	time.Sleep(5 * time.Millisecond)

	// Should have created a new connection
	assert.Greater(t, factory.getCreateCount(), initialCount, "Expected retry to create new connection")

	// New connection should work
	_, err = wrapper.ListDir(ctx, "/test", true)
	assert.NoError(t, err, "ListDir with new connection should succeed")
}

func TestMultipleOperationsWithConnectionErrors(t *testing.T) {
	// Test multiple different operations trigger handleConnectionError
	operations := []struct {
		name string
		fn   func(*Conn, context.Context) error
	}{
		{"ListDir", func(c *Conn, ctx context.Context) error {
			_, err := c.ListDir(ctx, "/test", true)
			return err
		}},
		{"Get", func(c *Conn, ctx context.Context) error {
			_, _, err := c.Get(ctx, "/test")
			return err
		}},
		{"Create", func(c *Conn, ctx context.Context) error {
			_, err := c.Create(ctx, "/test", []byte("data"))
			return err
		}},
		{"Update", func(c *Conn, ctx context.Context) error {
			_, err := c.Update(ctx, "/test", []byte("data"), &mockVersion{version: "1"})
			return err
		}},
		{"Delete", func(c *Conn, ctx context.Context) error {
			return c.Delete(ctx, "/test", &mockVersion{version: "1"})
		}},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			// Create a connection that fails immediately
			factory := newMockFactoryWithDelayedFailure(0)
			wrapper := NewConn(factory.newConn)

			ctx := context.Background()
			initialCount := factory.getCreateCount()

			// Operation should fail and trigger retry
			err := op.fn(wrapper, ctx)
			assert.Error(t, err, "Operation %s should fail", op.name)
			assert.Equal(t, mtrpc.Code_UNAVAILABLE, mterrors.Code(err), "Expected UNAVAILABLE error for %s", op.name)

			// Wait for retry
			time.Sleep(5 * time.Millisecond)

			// Should have attempted retry
			assert.Greater(t, factory.getCreateCount(), initialCount, "Expected retry for operation %s", op.name)
		})
	}
}

func TestRetryConnection_TerminatesWhenClosed(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

	// Get the initial connection and manually trigger a retry with it
	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")
	initialCount := factory.getCreateCount()

	// Start retryConnection manually in a goroutine
	done := make(chan bool, 1)
	go func() {
		wrapper.retryConnection(conn)
		done <- true
	}()

	// Give retryConnection a moment to start
	time.Sleep(10 * time.Millisecond)

	// Ensure retry tried at least once.
	assert.Greater(t, factory.getCreateCount(), initialCount, "Expected retry count to increase")

	// Close the wrapper - this should terminate retryConnection
	err = wrapper.Close()
	assert.NoError(t, err, "Close should not fail")

	// Wait for retryConnection to complete or timeout
	select {
	case <-done:
		// retryConnection completed successfully - this is what we want
	case <-time.After(5 * time.Millisecond):
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
	wrapper := NewConn(factory.newConn)

	// Wait for retryConnection to start attempting
	time.Sleep(5 * time.Millisecond)

	// Allow connections to succeed
	factory.setShouldFail(false)

	// Wait for successful connection
	time.Sleep(10 * time.Millisecond)

	// Verify connection is available
	conn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection after factory allows success")
	require.NotNil(t, conn, "Expected non-nil connection after factory allows success")

	successCount := factory.getCreateCount()

	// Wait a bit more to ensure retryConnection has stopped
	time.Sleep(20 * time.Millisecond)

	// Verify no additional connection attempts were made
	assert.LessOrEqual(t, factory.getCreateCount(), successCount, "retryConnection should have terminated after successful connection")
}

func TestRetryConnection_TerminatesWhenConnectionReplaced(t *testing.T) {
	factory := newMockFactory()
	wrapper := NewConn(factory.newConn)

	// Get initial connection
	oldConn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected initial connection")

	// Create a new connection manually
	newConn, err := factory.newConn()
	require.NoError(t, err, "Failed to create new connection")

	// Replace the connection
	wrapper.mu.Lock()
	wrapper.wrapped = newConn
	wrapper.mu.Unlock()

	initialCount := factory.getCreateCount()

	// Start retryConnection with the old connection - should terminate immediately
	wrapper.retryConnection(oldConn)

	// Wait briefly
	time.Sleep(10 * time.Millisecond)

	// Verify no new connections were created
	assert.LessOrEqual(t, factory.getCreateCount(), initialCount, "retryConnection should have terminated when connection was already replaced")

	// Verify current connection is still the new one
	currentConn, err := wrapper.getConnection()
	require.NoError(t, err, "Expected connection")
	assert.Equal(t, newConn, currentConn, "Expected connection to remain the newer one")
}

func TestRetryConnection_ClosesStrayConnectionWhenWrapperClosed(t *testing.T) {
	factory := newMockFactory()
	// Make NewConn go into a retry loop.
	factory.setShouldFail(true)
	wrapper := NewConn(factory.newConn)
	initialCount := factory.getCreateCount()

	// Wait for at least one more connection attempt to be sure
	// that we are well into the retry loop.
	for factory.getCreateCount() <= initialCount {
		time.Sleep(time.Millisecond)
	}

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

	// Give time for the retry routine to close the connection.
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, initialCount+1, factory.getCreateCount(), "Unexpected create count")

	conn := factory.connections[connectionCount]
	assert.True(t, conn.closed, "Connection should be closed")
}
