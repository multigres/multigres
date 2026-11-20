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
	"log/slog"
	"time"
)

// ErrLeaseLost is set as the context cause (via context.WithCancelCause)
// when an ephemeral lease is lost (stolen or expired). Callers can check
// context.Cause(ctx) to distinguish lease loss from other cancellations.
var ErrLeaseLost = errors.New("lease lost")

// ErrLeaseGone is wrapped by a LeaseChecker's error when the lease is known to
// be gone: no longer the holder's, or revoked by a steal. A topo NoNode error is
// read the same way: the store reports a lease it no longer has (expired or
// revoked) as not found. Any other checker error is treated as transient.
var ErrLeaseGone = errors.New("lease gone")

func leaseGone(err error) bool {
	return errors.Is(err, ErrLeaseGone) || errors.Is(err, &TopoError{Code: NoNode})
}

// AbandonLease wraps err so that, when fn returns it from WithLease, the lease
// is not unlocked: renewal stops and the lease expires on its own after its TTL,
// as if the process had died. It is for a caller that cannot prove the work it
// started under the lease has ended, and must not hand the lease to the next
// holder before the TTL has passed. WithLease returns err itself (it still
// matches errors.Is/As through the wrapper).
func AbandonLease(err error) error {
	if err == nil {
		return nil
	}
	return &abandonLeaseError{err: err}
}

// IsLeaseAbandon reports whether err carries an AbandonLease request, for a
// caller that must rebuild an error as text and keep the request.
func IsLeaseAbandon(err error) bool {
	var ab *abandonLeaseError
	return errors.As(err, &ab)
}

type abandonLeaseError struct{ err error }

func (e *abandonLeaseError) Error() string { return e.err.Error() }
func (e *abandonLeaseError) Unwrap() error { return e.err }

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
	ttl              time.Duration // 0 = three check intervals
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

// WithLeaseTTL tells WithLease the TTL of the lease it acquires, which bounds
// how long it keeps retrying a failing check before declaring the lease lost.
// Defaults to three check intervals.
func WithLeaseTTL(d time.Duration) LeaseOption {
	return func(o *leaseOptions) { o.ttl = d }
}

// WithLease acquires a lease, runs fn, and releases the lease when fn returns,
// even on panic, unless fn's error carries AbandonLease, in which case the lease
// is left to expire. The context passed to fn is cancelled (with ErrLeaseLost as
// cause) if the lease is lost. Acquisition is bounded by ctx; once acquired, the
// lease is renewed until fn returns whether or not ctx is cancelled meanwhile.
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
	if options.ttl <= 0 {
		options.ttl = 3 * options.checkInterval
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

	// Guaranteed release, unless fn asked to abandon the lease.
	abandon := false
	defer func() {
		if abandon {
			slog.WarnContext(ctx, "abandoning the lease without unlocking it; it expires after its TTL",
				"action", action, "ttl", options.ttl)
			return
		}
		var unlockErr error
		unlock(&unlockErr)
	}()

	// Monitor lease health: each check also renews the lease (etcd KeepAliveOnce).
	//
	// The monitor, and so the renewal, belongs to the lease and not to the
	// caller's context: once the lease is acquired it keeps being renewed until fn
	// has returned, even if the caller cancels ctx while fn is still settling
	// work that must finish under the lease (fn may wait on something it
	// started). Cancelling ctx only cancels fnCtx.
	//
	// Each check runs under its own timeout, the smaller of a third of the TTL and
	// half a check interval, so a check that times out ends before the next tick
	// and leaves room for another. A check that fails is retried at the next
	// interval, and the lease is declared lost (the context passed to fn is
	// cancelled with ErrLeaseLost and renewal stops) in exactly two cases:
	//   - the check reports the lease gone (an error wrapping ErrLeaseGone, or a
	//     topo NoNode: not found in the store, expired or revoked); or
	//   - a failed check ends at least lossAfter after the start of the last
	//     successful check (after acquisition, before the first success), where
	//     lossAfter is the TTL minus one check interval minus half a check timeout.
	//
	// The store extended the lease no earlier than the start of the last
	// successful check, so it expires no earlier than a full TTL after that
	// instant, and the loss window ends one interval before that. Failures that
	// return quickly inside the window are retried, and at the shipped ratio (TTL
	// three intervals) one check that runs to its timeout is tolerated and two
	// are not. How much lead the signal has over the store's expiry depends on
	// the tick phase and on how long the failing checks ran: a check that ends
	// late in a tick consumes part of the lead, and the signal is never later
	// than the first failed check that ends once the window has passed.
	fnCtx, cancelCause := context.WithCancelCause(lockCtx)

	// Stop the monitor and wait for it before the deferred unlock above runs, so
	// a check cannot observe our own release and report it as a lost lease.
	fnReturned := make(chan struct{})
	monitorDone := make(chan struct{})
	defer func() {
		close(fnReturned)
		<-monitorDone
		cancelCause(nil)
	}()

	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(options.checkInterval)
		defer ticker.Stop()
		lastOK := time.Now()
		checkTimeout := min(options.ttl/3, options.checkInterval/2)
		// The loss window is a TTL minus one check interval minus half a check
		// timeout. The half timeout keeps the threshold off the tick lattice: with
		// a TTL that is a whole number of intervals, a bare TTL minus one interval
		// would sit exactly on a tick, and a successful check that started a little
		// late (a delayed tick) would let the second quick failure fall just short
		// of it and be retried, so the lease would end a full interval later, at or
		// after the store's earliest expiry. With the offset a second failed check
		// of any kind ends the lease, while one check that ran to its timeout still
		// ends inside the window and is retried.
		lossAfter := options.ttl - options.checkInterval - checkTimeout/2
		for {
			select {
			case <-fnReturned:
				return
			case <-ticker.C:
				started := time.Now()
				checkCtx, cancel := context.WithTimeout(context.WithoutCancel(fnCtx), checkTimeout)
				err := check(checkCtx)
				cancel()
				if err == nil {
					lastOK = started
					continue
				}
				select {
				case <-fnReturned:
					return
				default:
				}
				if !leaseGone(err) && time.Since(lastOK) < lossAfter {
					slog.WarnContext(fnCtx, "lease check failed; retrying", "action", action,
						"error", err, "since_last_success", time.Since(lastOK).Round(time.Millisecond))
					continue
				}
				recordLeaseLost(fnCtx)
				slog.WarnContext(fnCtx, "lease lost", "action", action, "error", err)
				cancelCause(ErrLeaseLost)
				return
			}
		}
	}()

	err = fn(fnCtx)
	var ab *abandonLeaseError
	abandon = errors.As(err, &ab)
	return err
}
