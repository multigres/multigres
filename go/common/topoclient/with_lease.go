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
	"errors"
	"time"
)

// ErrLeaseLost is set as the context cause (via context.WithCancelCause)
// when an ephemeral lease is lost (stolen or expired). Callers can check
// context.Cause(ctx) to distinguish lease loss from other cancellations.
var ErrLeaseLost = errors.New("lease lost")

// DefaultLeaseCheckInterval is how often WithLease polls to detect lease loss.
const DefaultLeaseCheckInterval = 10 * time.Second

// LeaseAcquirer attempts to acquire a lease. Returns a context containing lock
// info, an unlock function, and an error if acquisition fails.
type LeaseAcquirer func(ctx context.Context, action string) (context.Context, func(*error), error)

// LeaseRevoker forcefully revokes an existing lease (used for steal semantics).
type LeaseRevoker func(ctx context.Context) error

// LeaseChecker checks whether the lease is still held. Returns an error if not.
type LeaseChecker func(ctx context.Context) error

// LeaseOption configures WithLease behavior.
type LeaseOption func(*leaseOptions)

type leaseOptions struct {
	stealGracePeriod time.Duration // 0 = don't steal, fail fast
	checkInterval    time.Duration
}

// WithStealGracePeriod enables steal semantics: if the lease is already held,
// revoke it, wait this duration for the old holder to clean up, then reacquire.
// If 0 (default), WithLease fails immediately when the lease is held.
func WithStealGracePeriod(d time.Duration) LeaseOption {
	return func(o *leaseOptions) { o.stealGracePeriod = d }
}

// WithLeaseCheckInterval sets how often to poll the checker to detect lease loss.
// Defaults to DefaultLeaseCheckInterval (10s).
func WithLeaseCheckInterval(d time.Duration) LeaseOption {
	return func(o *leaseOptions) { o.checkInterval = d }
}

// WithLease acquires a lease, runs fn, and releases the lease when fn returns —
// even on panic. The context passed to fn is cancelled (with ErrLeaseLost as
// cause) if the lease is lost.
//
// If WithStealGracePeriod is set and the lease is already held, it revokes the
// existing lease, waits the grace period, then acquires a new one.
func WithLease(
	ctx context.Context,
	action string,
	acquire LeaseAcquirer,
	revoke LeaseRevoker,
	check LeaseChecker,
	fn func(context.Context) error,
	opts ...LeaseOption,
) error {
	var options leaseOptions
	options.checkInterval = DefaultLeaseCheckInterval
	for _, opt := range opts {
		opt(&options)
	}

	// Try to acquire
	lockCtx, unlock, err := acquire(ctx, action)
	if err != nil {
		if options.stealGracePeriod <= 0 {
			return err
		}
		// Steal: revoke, wait, reacquire
		if revokeErr := revoke(ctx); revokeErr != nil {
			return revokeErr
		}
		select {
		case <-time.After(options.stealGracePeriod):
		case <-ctx.Done():
			return ctx.Err()
		}
		lockCtx, unlock, err = acquire(ctx, action)
		if err != nil {
			return err
		}
	}

	// Guaranteed release
	defer func() {
		var unlockErr error
		unlock(&unlockErr)
	}()

	// Monitor lease health and cancel context on loss
	fnCtx, cancelCause := context.WithCancelCause(lockCtx)
	defer cancelCause(nil)

	go func() {
		ticker := time.NewTicker(options.checkInterval)
		defer ticker.Stop()
		for {
			select {
			case <-fnCtx.Done():
				return
			case <-ticker.C:
				checkCtx := context.WithoutCancel(fnCtx)
				if err := check(checkCtx); err != nil {
					cancelCause(ErrLeaseLost)
					return
				}
			}
		}
	}()

	return fn(fnCtx)
}
