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
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// listCacheTTL bounds how stale the informational backup list may be. A
// backup or expire performed by another pooler on the shared repository can
// take this long to show up through GetBackups.
const listCacheTTL = 5 * time.Second

// listCache memoizes the most recent `pgbackrest info` result for
// informational callers and coalesces concurrent misses into one run.
//
// ponytail: single slot, one key. Re-extract into a generic helper only if a
// second informational read needs the same behaviour.
type listCache struct {
	mu      sync.Mutex
	value   []*multipoolermanagerdata.BackupMetadata
	expires time.Time
	flight  *listFlight
}

type listFlight struct {
	done  chan struct{}
	value []*multipoolermanagerdata.BackupMetadata
	err   error
}

// get returns the cached list, or runs load once for all concurrent callers.
// Successful results are kept for ttl; failures are not cached. A caller's
// cancellation does not cancel the shared load, which is always bounded by
// timeout even if every caller leaves.
func (c *listCache) get(ctx context.Context, ttl, timeout time.Duration, load func(context.Context) ([]*multipoolermanagerdata.BackupMetadata, error)) ([]*multipoolermanagerdata.BackupMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !c.expires.IsZero() && time.Now().Before(c.expires) {
		value := c.value
		c.mu.Unlock()
		return value, nil
	}
	f := c.flight
	if f == nil {
		f = &listFlight{done: make(chan struct{})}
		c.flight = f
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		go func() {
			defer cancel()
			value, err := load(loadCtx)
			if err == nil {
				err = loadCtx.Err()
			}
			c.mu.Lock()
			f.value, f.err = value, err
			// Invalidate detaches in-flight loads too: a result from before the
			// mutation can never repopulate the slot or be joined afterwards.
			if c.flight == f {
				c.flight = nil
				if err == nil {
					c.value, c.expires = value, time.Now().Add(ttl)
				}
			}
			close(f.done)
			c.mu.Unlock()
		}()
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return f.value, f.err
	}
}

// Invalidate drops the stored list and detaches any in-flight load.
func (c *listCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value, c.expires, c.flight = nil, time.Time{}, nil
}

// CachedList is for informational status RPCs only. Recovery, expiration and
// job-completion decisions continue to use the uncached List/ListBackups APIs.
// Cloning prevents callers from mutating a cached protobuf or its slice.
func (e *Engine) CachedList(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	values, err := e.listCache.get(ctx, listCacheTTL, commonbackup.InfoTimeout, e.ListBackups)
	if err != nil {
		return nil, err
	}
	if limit > 0 && uint64(limit) < uint64(len(values)) {
		values = values[:limit]
	}
	result := make([]*multipoolermanagerdata.BackupMetadata, len(values))
	for i, v := range values {
		result[i] = proto.Clone(v).(*multipoolermanagerdata.BackupMetadata)
	}
	return result, nil
}
