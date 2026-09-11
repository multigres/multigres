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

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestCachedListClonesInvalidatesAndFreshAPIsBypass(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "calls")
	json := `[{"backup":[{"label":"backup-one","type":"full","annotation":{"table_group":"tg1","shard":"0"}}]}]`
	stubPgbackrest(t, strings.Replace(pgbackrestInfoStub(json), "#!/bin/bash\n", fmt.Sprintf("#!/bin/bash\nprintf x >> %q\n", counter), 1))
	dir := t.TempDir()
	e, _ := newTestEngine(t, dir, "tg1", "0", "/tmp/backups")
	config := setupMockPgBackRestConfig(t, dir)
	e.SetConfigPath(config)
	first, err := e.CachedList(t.Context(), 0)
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].BackupId = "mutated"
	second, err := e.CachedList(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, "backup-one", second[0].BackupId)
	b, err := os.ReadFile(counter)
	require.NoError(t, err)
	require.Equal(t, "x", string(b))
	_, err = e.ListBackups(t.Context())
	require.NoError(t, err)
	_, err = e.List(t.Context(), 0)
	require.NoError(t, err)
	e.SetConfigPath(config)
	_, err = e.CachedList(t.Context(), 0)
	require.NoError(t, err)
	b, err = os.ReadFile(counter)
	require.NoError(t, err)
	require.Equal(t, "xxxx", string(b))
}

// listOf builds a distinguishable list for the coalescing tests below.
func listOf(n int) []*multipoolermanagerdata.BackupMetadata {
	return make([]*multipoolermanagerdata.BackupMetadata, n)
}

func TestListCacheCoalescesAndExpires(t *testing.T) {
	var c listCache
	var calls atomic.Int32
	load := func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		return listOf(int(calls.Add(1))), nil
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			v, err := c.get(t.Context(), time.Hour, time.Second, load)
			if err != nil || len(v) != 1 {
				t.Errorf("get=(%d,%v)", len(v), err)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, calls.Load())
	c.mu.Lock()
	c.expires = time.Now().Add(-time.Second)
	c.mu.Unlock()
	v, err := c.get(t.Context(), time.Hour, time.Second, load)
	require.NoError(t, err)
	require.Len(t, v, 2)
}

func TestListCacheCancellationDoesNotCancelSharedLoad(t *testing.T) {
	var c listCache
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		_, err := c.get(ctx, time.Hour, time.Second, func(ctx context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
			close(started)
			<-release
			return listOf(7), ctx.Err()
		})
		done <- err
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)
	v, err := c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		t.Error("duplicate load")
		return nil, nil
	})
	require.NoError(t, err)
	require.Len(t, v, 7)
}

func TestListCacheInvalidationDetachesOldLoad(t *testing.T) {
	var c listCache
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
			close(started)
			<-release
			return listOf(1), nil
		})
	}()
	<-started
	c.Invalidate()
	v, err := c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) { return listOf(2), nil })
	require.NoError(t, err)
	require.Len(t, v, 2)
	close(release)
	<-done
	v, err = c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		t.Error("cache overwritten by detached load")
		return listOf(3), nil
	})
	require.NoError(t, err)
	require.Len(t, v, 2)
}

func TestListCacheFailureTimeoutAndEmptyResult(t *testing.T) {
	var c listCache
	boom := errors.New("failed")
	_, err := c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) { return nil, boom })
	require.ErrorIs(t, err, boom)
	// A failure is not cached: the next call loads again.
	v, err := c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) { return listOf(0), nil })
	require.NoError(t, err)
	require.Empty(t, v)
	// An empty success is cached like any other value.
	_, err = c.get(t.Context(), time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		t.Error("empty result not cached")
		return nil, nil
	})
	require.NoError(t, err)
	c.Invalidate()
	_, err = c.get(t.Context(), time.Hour, time.Millisecond, func(ctx context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		<-ctx.Done()
		return listOf(1), nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.get(ctx, time.Hour, time.Second, func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
		t.Error("called after cancellation")
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
