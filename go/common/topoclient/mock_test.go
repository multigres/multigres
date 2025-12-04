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
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
)

// mockConn is a mock implementation of Conn for testing
type mockConn struct {
	id              int
	closed          bool
	shouldFailCalls bool
	mu              sync.Mutex
	data            map[string][]byte
}

func newMockConn(id int) *mockConn {
	return &mockConn{
		id:   id,
		data: make(map[string][]byte),
	}
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

func (m *mockConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []DirEntry{{Name: "test"}}, nil
}

func (m *mockConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.data[filePath] = contents
	m.mu.Unlock()
	return &mockVersion{version: "1"}, nil
}

func (m *mockConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.data[filePath] = contents
	m.mu.Unlock()
	return &mockVersion{version: "2"}, nil
}

func (m *mockConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	m.mu.Lock()
	data, ok := m.data[filePath]
	m.mu.Unlock()
	if !ok {
		return nil, nil, &TopoError{Code: NoNode}
	}
	return data, &mockVersion{version: "1"}, nil
}

func (m *mockConn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []byte("test"), nil
}

func (m *mockConn) List(ctx context.Context, filePathPrefix string) ([]KVInfo, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return []KVInfo{{Key: []byte("key"), Value: []byte("value")}}, nil
}

func (m *mockConn) Delete(ctx context.Context, filePath string, version Version) error {
	return m.checkError()
}

func (m *mockConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) LockName(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) LockNameWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) TryLock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) TryLockName(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	if err := m.checkError(); err != nil {
		return nil, err
	}
	return &mockLockDescriptor{}, nil
}

func (m *mockConn) Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	ch := make(chan *WatchData, 1)
	close(ch)
	return &WatchData{Contents: []byte("test")}, ch, nil
}

func (m *mockConn) WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
	if err := m.checkError(); err != nil {
		return nil, nil, err
	}
	ch := make(chan *WatchDataRecursive, 1)
	close(ch)
	return []*WatchDataRecursive{{Path: "test"}}, ch, nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// mockVersion implements Version
type mockVersion struct {
	version string
}

func (v *mockVersion) String() string {
	return v.version
}

// mockLockDescriptor implements LockDescriptor
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

func (f *mockFactory) newConn() (Conn, error) {
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

// Create implements Factory interface for store tests
func (f *mockFactory) Create(topoName, root string, serverAddrs []string) (Conn, error) {
	return f.newConn()
}

func (f *mockFactory) waitForNewConn(currentCount int32) {
	for f.getCreateCount() == currentCount {
		time.Sleep(time.Millisecond)
	}
}

// mockFactoryWithSelectiveFailure fails only for specific cells
type mockFactoryWithSelectiveFailure struct {
	mu          sync.Mutex
	createCount int32
	failForCell string
}

func (f *mockFactoryWithSelectiveFailure) newConn(topoName string) (Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := atomic.AddInt32(&f.createCount, 1)

	if topoName == f.failForCell {
		return nil, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "factory error")
	}

	return newMockConn(int(count)), nil
}

// Create implements Factory interface
func (f *mockFactoryWithSelectiveFailure) Create(topoName, root string, serverAddrs []string) (Conn, error) {
	return f.newConn(topoName)
}
