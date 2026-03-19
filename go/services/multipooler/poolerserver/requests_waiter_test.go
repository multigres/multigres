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

package poolerserver

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestsWaiter_AddDoneCount(t *testing.T) {
	rw := NewRequestsWaiter()

	assert.Equal(t, int64(0), rw.GetCount())

	rw.Add(1)
	assert.Equal(t, int64(1), rw.GetCount())

	rw.Add(1)
	assert.Equal(t, int64(2), rw.GetCount())

	rw.Done()
	assert.Equal(t, int64(1), rw.GetCount())

	rw.Done()
	assert.Equal(t, int64(0), rw.GetCount())
}

func TestRequestsWaiter_ZeroChanAlreadyClosed(t *testing.T) {
	rw := NewRequestsWaiter()

	// ZeroChan should be immediately readable when count is 0
	select {
	case <-rw.ZeroChan():
		// expected: channel is already closed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ZeroChan should be closed when count is 0")
	}
}

func TestRequestsWaiter_ZeroChanFiresOnDrain(t *testing.T) {
	rw := NewRequestsWaiter()

	rw.Add(1)

	// ZeroChan should NOT be readable while count > 0
	select {
	case <-rw.ZeroChan():
		t.Fatal("ZeroChan should not be closed while count > 0")
	case <-time.After(50 * time.Millisecond):
		// expected: not closed yet
	}

	// Complete the request
	rw.Done()

	// Now ZeroChan should fire
	select {
	case <-rw.ZeroChan():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ZeroChan should be closed after count reaches 0")
	}
}

func TestRequestsWaiter_ZeroChanResetsOnNewAdd(t *testing.T) {
	rw := NewRequestsWaiter()

	// Start with count=0, ZeroChan is closed
	select {
	case <-rw.ZeroChan():
	default:
		t.Fatal("ZeroChan should be closed initially")
	}

	// Add a request — creates new open channel
	rw.Add(1)

	select {
	case <-rw.ZeroChan():
		t.Fatal("ZeroChan should not be closed after Add")
	default:
		// expected: channel is open
	}

	rw.Done()

	// Should be closed again
	select {
	case <-rw.ZeroChan():
	default:
		t.Fatal("ZeroChan should be closed after Done brings count to 0")
	}
}

func TestRequestsWaiter_ConcurrentAddDone(t *testing.T) {
	rw := NewRequestsWaiter()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			rw.Add(1)
			// simulate work
			time.Sleep(time.Millisecond)
			rw.Done()
		}()
	}

	wg.Wait()

	require.Equal(t, int64(0), rw.GetCount())

	// ZeroChan should be closed
	select {
	case <-rw.ZeroChan():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ZeroChan should be closed after all concurrent requests complete")
	}
}
