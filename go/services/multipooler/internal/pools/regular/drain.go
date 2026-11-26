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

package regular

import (
	"context"
	"errors"
	"sync"
	"time"
)

// killParallelism is how many backends a drain terminates at once.
const killParallelism = 16

// RunKills runs kill for each of n items, killParallelism at a time, and
// returns how many it ran and the errors of those that failed, joined. Once
// ctx ends it starts no more, and the returned error includes ctx's.
func RunKills(ctx context.Context, n int, kill func(ctx context.Context, i int) error) (int, error) {
	var (
		mu   sync.Mutex
		errs []error
		ran  int
		wg   sync.WaitGroup
		sem  = make(chan struct{}, killParallelism)
	)
	for i := range n {
		if ctx.Err() != nil {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		ran++
		wg.Go(func() {
			defer func() { <-sem }()
			if err := kill(ctx, i); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	return ran, errors.Join(errs...)
}

// Seal stops the pool from lending connections until Unseal (see
// connpool.Pool.Seal).
func (p *Pool) Seal(err error) {
	p.pool.Seal(err)
}

// Unseal lets a sealed pool lend connections again.
func (p *Pool) Unseal() {
	p.pool.Unseal()
}

// KillOpen terminates the backend of every open connection of the pool and
// waits up to timeout for each to exit (see Conn.KillAndWait). In a sealed
// pool those are the connections still lent. It returns how many it
// terminated, and an error if any backend may still be running or ctx ended
// first.
func (p *Pool) KillOpen(ctx context.Context, timeout time.Duration) (int, error) {
	conns := p.pool.OpenConns()
	return RunKills(ctx, len(conns), func(ctx context.Context, i int) error {
		if conns[i].IsClosed() {
			return nil
		}
		return conns[i].KillAndWait(ctx, timeout)
	})
}
