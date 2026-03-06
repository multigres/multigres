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

package topoclient

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controlledConn embeds *mockConn and overrides WatchRecursive for fine-grained control.
type controlledConn struct {
	*mockConn
	watchRecursiveFn func(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error)
}

func (c *controlledConn) WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
	if c.watchRecursiveFn != nil {
		return c.watchRecursiveFn(ctx, path)
	}
	ch := make(chan *WatchDataRecursive)
	close(ch)
	return nil, ch, nil
}

// staticConnProvider always returns the same Conn for any cell.
type staticConnProvider struct {
	conn Conn
}

func (s *staticConnProvider) ConnForCell(_ context.Context, _ string) (Conn, error) {
	return s.conn, nil
}

func (s *staticConnProvider) Status() map[string]string { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWatchPathWithRetry_InitialSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initial := []*WatchDataRecursive{
		{Path: "poolers/node1/Pooler"},
		{Path: "poolers/node2/Pooler"},
	}

	changes := make(chan *WatchDataRecursive)

	conn := &controlledConn{mockConn: newMockConn(0)}
	conn.watchRecursiveFn = func(_ context.Context, _ string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
		return initial, changes, nil
	}

	var gotInitial []*WatchDataRecursive
	initialDone := make(chan struct{})

	go watchPathWithRetry(ctx, &staticConnProvider{conn: conn}, "zone1", "poolers",
		discardLogger(),
		func(items []*WatchDataRecursive) {
			gotInitial = items
			close(initialDone)
		},
		func(ch <-chan *WatchDataRecursive) {
			<-ctx.Done()
		},
	)

	select {
	case <-initialDone:
	case <-time.After(time.Second):
		t.Fatal("onInitial was not called within 1s")
	}

	require.Len(t, gotInitial, 2)
	assert.Equal(t, "poolers/node1/Pooler", gotInitial[0].Path)
	assert.Equal(t, "poolers/node2/Pooler", gotInitial[1].Path)
}

func TestWatchPathWithRetry_ChangeEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := make(chan *WatchDataRecursive, 1)
	ch <- &WatchDataRecursive{Path: "poolers/node1/Pooler"}

	conn := &controlledConn{mockConn: newMockConn(0)}
	conn.watchRecursiveFn = func(_ context.Context, _ string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
		return nil, ch, nil
	}

	gotEvent := make(chan *WatchDataRecursive, 1)

	go watchPathWithRetry(ctx, &staticConnProvider{conn: conn}, "zone1", "poolers",
		discardLogger(),
		func(_ []*WatchDataRecursive) {},
		func(changes <-chan *WatchDataRecursive) {
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-changes:
					if !ok {
						return
					}
					select {
					case gotEvent <- ev:
					default:
					}
				}
			}
		},
	)

	select {
	case ev := <-gotEvent:
		assert.Equal(t, "poolers/node1/Pooler", ev.Path)
	case <-time.After(time.Second):
		t.Fatal("watchFn did not receive event within 1s")
	}
}

func TestWatchPathWithRetry_ReconnectsOnChannelClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var callCount atomic.Int32
	reconnected := make(chan struct{})

	conn := &controlledConn{mockConn: newMockConn(0)}
	conn.watchRecursiveFn = func(_ context.Context, _ string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
		n := callCount.Add(1)
		ch := make(chan *WatchDataRecursive)
		if n == 1 {
			// First call: close channel immediately to trigger reconnect.
			close(ch)
		} else {
			// Second call: signal reconnected and keep channel open until ctx done.
			select {
			case reconnected <- struct{}{}:
			default:
			}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
		}
		return nil, ch, nil
	}

	go watchPathWithRetry(ctx, &staticConnProvider{conn: conn}, "zone1", "poolers",
		discardLogger(),
		func(_ []*WatchDataRecursive) {},
		func(changes <-chan *WatchDataRecursive) {
			for range changes {
			}
		},
	)

	select {
	case <-reconnected:
		require.GreaterOrEqual(t, int(callCount.Load()), 2)
	case <-time.After(3 * time.Second):
		t.Fatal("did not reconnect within 3s")
	}
}

func TestWatchPathWithRetry_ShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	conn := &controlledConn{mockConn: newMockConn(0)}
	conn.watchRecursiveFn = func(_ context.Context, _ string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
		ch := make(chan *WatchDataRecursive) // never closes
		return nil, ch, nil
	}

	done := make(chan struct{})
	go func() {
		watchPathWithRetry(ctx, &staticConnProvider{conn: conn}, "zone1", "poolers",
			discardLogger(),
			func(_ []*WatchDataRecursive) {},
			func(changes <-chan *WatchDataRecursive) {
				select {
				case <-ctx.Done():
				case <-changes:
				}
			},
		)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchPathWithRetry did not return after ctx cancel within 1s")
	}
}

func TestExtractCellFromPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"relative path", "cells/zone1/Cell", "zone1"},
		{"absolute etcd path", "/multigres/global/cells/zone1/Cell", "zone1"},
		{"no cell segment", "poolers/foo/Pooler", ""},
		{"cells prefix only", "cells/", ""},
		{"another cell", "cells/us-east-1/Cell", "us-east-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractCellFromPath(tc.path))
		})
	}
}
