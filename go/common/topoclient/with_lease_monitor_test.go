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

package topoclient_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
)

// leaseScript is a fake lease: an acquirer that always succeeds and a checker
// whose answer the test scripts per call, all in virtual time.
type leaseScript struct {
	mu      sync.Mutex
	calls   int
	unlocks int
	// answer decides one check from its 1-based call number, the time since the
	// lease was acquired and the check's context. It may block on the context.
	answer func(call int, since time.Duration, ctx context.Context) error
	began  time.Time
	// deadlines holds each check's time left when it started.
	deadlines []time.Duration
}

func (l *leaseScript) acquire(ctx context.Context, _ string) (context.Context, func(*error), error) {
	l.began = time.Now()
	return ctx, func(*error) { l.mu.Lock(); l.unlocks++; l.mu.Unlock() }, nil
}

func (l *leaseScript) revoke(context.Context) error { return errors.New("no steal") }

func (l *leaseScript) check(ctx context.Context) error {
	l.mu.Lock()
	l.calls++
	call := l.calls
	if dl, ok := ctx.Deadline(); ok {
		l.deadlines = append(l.deadlines, time.Until(dl))
	} else {
		l.deadlines = append(l.deadlines, -1)
	}
	l.mu.Unlock()
	return l.answer(call, time.Since(l.began), ctx)
}

func (l *leaseScript) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *leaseScript) unlockCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unlocks
}

const (
	monitorInterval = time.Second
	monitorTTL      = 6 * time.Second
)

func (l *leaseScript) run(ctx context.Context, fn func(context.Context) error) error {
	return topoclient.WithLease(ctx, "scripted", l.acquire, l.revoke, l.check, fn,
		topoclient.WithLeaseCheckInterval(monitorInterval), topoclient.WithLeaseTTL(monitorTTL))
}

// A run of successes, a few failures that stay inside the tolerance, and more
// successes never end the lease: the tolerance is counted from the last success,
// not from acquisition.
func TestWithLeaseMonitor_FailuresAfterSuccessesDoNotEndTheLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &leaseScript{answer: func(call int, _ time.Duration, _ context.Context) error {
			if call >= 11 && call <= 14 {
				return errors.New("etcd unreachable")
			}
			return nil
		}}
		err := l.run(t.Context(), func(fnCtx context.Context) error {
			time.Sleep(40 * time.Second)
			return context.Cause(fnCtx)
		})
		require.NoError(t, err, "the callback was never told the lease was lost")
		assert.Equal(t, 1, l.unlockCount())
	})
}

// A check that blocks is interrupted at its timeout, the smaller of a third of
// the TTL and half an interval, and the loss is signalled when the loss window
// (ttl minus one interval minus half a timeout) has passed since the last
// success started: not before, and not much after.
func TestWithLeaseMonitor_BlockedChecksAreBoundedAndLossIsTimed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &leaseScript{answer: func(call int, _ time.Duration, ctx context.Context) error {
			if call <= 3 {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}}
		var lostAt time.Duration
		err := l.run(t.Context(), func(fnCtx context.Context) error {
			select {
			case <-fnCtx.Done():
				lostAt = time.Since(l.began)
				return context.Cause(fnCtx)
			case <-time.After(10 * monitorTTL):
				return errors.New("the loss was never signalled")
			}
		})
		require.ErrorIs(t, err, topoclient.ErrLeaseLost)
		// The last success started at 3s; the loss is due at 3s + (6s - 1s) = 8s,
		// and a blocked check holds the monitor for up to its timeout past that.
		lastSuccess := 3 * monitorInterval
		due := lastSuccess + monitorTTL - monitorInterval
		assert.GreaterOrEqual(t, lostAt, due, "never before ttl minus one interval since the last success")
		assert.LessOrEqual(t, lostAt, due+monitorInterval/2+monitorInterval)
		l.mu.Lock()
		defer l.mu.Unlock()
		for i, d := range l.deadlines {
			assert.Equal(t, monitorInterval/2, d, "check %d runs under half an interval", i+1)
		}
	})
}

// A check that reports the lease gone ends it at that check, however recently
// the last success was.
func TestWithLeaseMonitor_GoneEndsItAtThatCheck(t *testing.T) {
	for name, gone := range map[string]error{
		"ErrLeaseGone": fmt.Errorf("%w: stolen", topoclient.ErrLeaseGone),
		"NoNode":       topoclient.NewError(topoclient.NoNode, "lease"),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := &leaseScript{answer: func(call int, _ time.Duration, _ context.Context) error {
					if call == 3 {
						return gone
					}
					return nil
				}}
				var lostAt time.Duration
				err := l.run(t.Context(), func(fnCtx context.Context) error {
					select {
					case <-fnCtx.Done():
						lostAt = time.Since(l.began)
						return context.Cause(fnCtx)
					case <-time.After(10 * monitorTTL):
						return errors.New("the loss was never signalled")
					}
				})
				require.ErrorIs(t, err, topoclient.ErrLeaseLost)
				assert.Equal(t, 3*monitorInterval, lostAt)
			})
		})
	}
}

// Cancelling the caller's context does not stop the checks: they keep running,
// uncancelled, until fn returns, and none runs after WithLease has returned.
func TestWithLeaseMonitor_ChecksOutliveTheCallerUntilFnReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cancelledChecks atomic.Int32
		l := &leaseScript{answer: func(_ int, _ time.Duration, ctx context.Context) error {
			if ctx.Err() != nil {
				cancelledChecks.Add(1)
			}
			return nil
		}}
		ctx, cancel := context.WithCancel(t.Context())
		var atCancel int
		err := l.run(ctx, func(fnCtx context.Context) error {
			time.Sleep(2 * monitorInterval)
			atCancel = l.callCount()
			cancel()
			assert.Error(t, fnCtx.Err(), "fn's context follows the caller")
			time.Sleep(10 * monitorInterval)
			return nil
		})
		require.NoError(t, err)
		returned := l.callCount()
		assert.GreaterOrEqual(t, returned-atCancel, 9, "checks continued while fn settled")
		assert.Zero(t, cancelledChecks.Load(), "no check ran under a cancelled context")
		assert.Equal(t, 1, l.unlockCount())
		time.Sleep(5 * monitorTTL)
		assert.Equal(t, returned, l.callCount(), "no check after WithLease returned")
	})
}

// A callback that abandons the lease leaves it locked: renewal stops and nothing
// unlocks it, and the error comes back as the callback returned it.
func TestWithLeaseMonitor_AbandonDoesNotUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &leaseScript{answer: func(int, time.Duration, context.Context) error { return nil }}
		cause := errors.New("shutting down with a delete unsettled")
		err := l.run(t.Context(), func(context.Context) error {
			time.Sleep(2 * monitorInterval)
			return topoclient.AbandonLease(cause)
		})
		require.ErrorIs(t, err, cause)
		assert.Zero(t, l.unlockCount(), "the lease is left to expire")
		returned := l.callCount()
		time.Sleep(5 * monitorTTL)
		assert.Equal(t, returned, l.callCount(), "and is no longer renewed")

		l2 := &leaseScript{answer: func(int, time.Duration, context.Context) error { return nil }}
		require.NoError(t, l2.run(t.Context(), func(context.Context) error { return nil }))
		assert.Equal(t, 1, l2.unlockCount())
	})
}

// The cases below run at the ratio the backup lease ships with (TTL three
// intervals, 30s and 10s), so the timings are the production ones: the check
// timeout is 5s, the loss window is 20s from the start of the last successful
// check, and the store's earliest expiry is 30s after that.

func (l *leaseScript) runBackupValues(ctx context.Context, fn func(context.Context) error) error {
	return topoclient.WithLease(ctx, "scripted", l.acquire, l.revoke, l.check, fn,
		topoclient.WithLeaseCheckInterval(topoclient.BackupLeaseCheckInterval), topoclient.WithLeaseTTL(topoclient.BackupLeaseTTL))
}

func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// waitForLoss is fn for a callback that expects to be told the lease is lost,
// and records when.
func (l *leaseScript) waitForLoss(lostAt *time.Duration) func(context.Context) error {
	return func(fnCtx context.Context) error {
		select {
		case <-fnCtx.Done():
			*lostAt = time.Since(l.began)
			return context.Cause(fnCtx)
		case <-time.After(10 * topoclient.BackupLeaseTTL):
			return errors.New("the loss was never signalled")
		}
	}
}

func TestWithLeaseMonitor_BackupValues(t *testing.T) {
	interval, ttl := topoclient.BackupLeaseCheckInterval, topoclient.BackupLeaseTTL

	t.Run("one timed-out check is tolerated", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &leaseScript{answer: func(call int, _ time.Duration, ctx context.Context) error {
				if call == 3 {
					return blockUntilDone(ctx)
				}
				return nil
			}}
			err := l.runBackupValues(t.Context(), func(fnCtx context.Context) error {
				time.Sleep(10 * interval)
				return context.Cause(fnCtx)
			})
			require.NoError(t, err)
		})
	})

	t.Run("two timed-out checks end it before the store's earliest expiry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &leaseScript{answer: func(call int, _ time.Duration, ctx context.Context) error {
				if call >= 3 {
					return blockUntilDone(ctx)
				}
				return nil
			}}
			var lostAt time.Duration
			err := l.runBackupValues(t.Context(), l.waitForLoss(&lostAt))
			require.ErrorIs(t, err, topoclient.ErrLeaseLost)
			// The last success started at 20s; the second timed-out check ends at
			// 45s, and the store expires the lease no earlier than 50s.
			assert.Equal(t, 45*time.Second, lostAt)
			assert.Less(t, lostAt, 20*time.Second+ttl)
			l.mu.Lock()
			defer l.mu.Unlock()
			for i, d := range l.deadlines {
				assert.Equal(t, interval/2, d, "check %d runs under half an interval", i+1)
			}
		})
	})

	t.Run("a slow success counts from its start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &leaseScript{answer: func(call int, _ time.Duration, _ context.Context) error {
				switch call {
				case 1:
					return nil
				case 2:
					time.Sleep(4500 * time.Millisecond) // a success that ends at 24.5s
					return nil
				}
				return errors.New("etcd unreachable")
			}}
			var lostAt time.Duration
			err := l.runBackupValues(t.Context(), l.waitForLoss(&lostAt))
			require.ErrorIs(t, err, topoclient.ErrLeaseLost)
			// The window is counted from 20s, the start of the second check: the
			// failing check at 30s is retried and the one at 40s ends it. Counted
			// from the end of the success it would last until 50s.
			assert.Equal(t, 40*time.Second, lostAt)
		})
	})

	t.Run("immediate failures from the start end it at the loss window", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &leaseScript{answer: func(int, time.Duration, context.Context) error { return errors.New("etcd unreachable") }}
			var lostAt time.Duration
			err := l.runBackupValues(t.Context(), l.waitForLoss(&lostAt))
			require.ErrorIs(t, err, topoclient.ErrLeaseLost)
			assert.Equal(t, ttl-interval, lostAt, "the first failure is retried, the second ends it")
		})
	})

	t.Run("immediate failures after successes end it a window after the last start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &leaseScript{answer: func(call int, _ time.Duration, _ context.Context) error {
				if call <= 2 {
					return nil
				}
				return errors.New("etcd unreachable")
			}}
			var lostAt time.Duration
			err := l.runBackupValues(t.Context(), l.waitForLoss(&lostAt))
			require.ErrorIs(t, err, topoclient.ErrLeaseLost)
			assert.Equal(t, 2*interval+ttl-interval, lostAt)
		})
	})
}

// The abandon request is read with errors.As: it survives wrapping and joining,
// is not conjured by an error that merely reads like it, and a nil request or a
// panic leaves the normal unlock in place.
func TestWithLease_AbandonRequestIsRecognisedThroughWrapping(t *testing.T) {
	cause := errors.New("shutting down with a delete unsettled")
	for name, tc := range map[string]struct {
		fn          func(context.Context) error
		wantUnlocks int
		wantErr     error
		wantPanic   bool
	}{
		"direct":  {fn: func(context.Context) error { return topoclient.AbandonLease(cause) }, wantErr: cause},
		"wrapped": {fn: func(context.Context) error { return fmt.Errorf("outer: %w", topoclient.AbandonLease(cause)) }, wantErr: cause},
		"wrapped twice": {fn: func(context.Context) error {
			return fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", topoclient.AbandonLease(cause)))
		}, wantErr: cause},
		"joined with a loss": {fn: func(context.Context) error {
			return errors.Join(topoclient.ErrLeaseLost, topoclient.AbandonLease(cause))
		}, wantErr: cause},
		"ordinary error with similar text": {
			fn:          func(context.Context) error { return fmt.Errorf("abandon the lease: %w", cause) },
			wantUnlocks: 1, wantErr: cause,
		},
		"a nil request": {fn: func(context.Context) error { return topoclient.AbandonLease(nil) }, wantUnlocks: 1},
		"no error":      {fn: func(context.Context) error { return nil }, wantUnlocks: 1},
		"a panic":       {fn: func(context.Context) error { panic("boom") }, wantUnlocks: 1, wantPanic: true},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := &leaseScript{answer: func(int, time.Duration, context.Context) error { return nil }}
				var err error
				run := func() {
					err = l.run(t.Context(), func(ctx context.Context) error {
						time.Sleep(2 * monitorInterval)
						return tc.fn(ctx)
					})
				}
				if tc.wantPanic {
					assert.Panics(t, run)
				} else {
					run()
				}
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
				} else if !tc.wantPanic {
					require.NoError(t, err)
				}
				assert.Equal(t, tc.wantUnlocks, l.unlockCount())
				returned := l.callCount()
				time.Sleep(5 * monitorTTL)
				assert.Equal(t, returned, l.callCount(), "renewal stopped on every path")
			})
		})
	}
}

// The loss threshold is off the tick lattice. A check that ignores its timeout
// makes the monitor receive a tick late, so the next successful check starts
// two seconds after its tick; the second quick failure after it is then 18s after
// that start, short of a bare TTL minus an interval (20s) and past the real
// threshold (17.5s). It must end the lease at that failure.
func TestWithLeaseMonitor_ALateSuccessDoesNotPushTheSecondFailurePastTheWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &leaseScript{answer: func(call int, _ time.Duration, _ context.Context) error {
			switch call {
			case 1:
				time.Sleep(12 * time.Second) // ignores its deadline; ends at 22s
				return nil
			case 2:
				return nil // the buffered 20s tick, received at 22s
			}
			return errors.New("etcd unreachable")
		}}
		var lostAt time.Duration
		err := l.runBackupValues(t.Context(), l.waitForLoss(&lostAt))
		require.ErrorIs(t, err, topoclient.ErrLeaseLost)
		assert.Equal(t, 40*time.Second, lostAt)
		assert.Less(t, lostAt, 22*time.Second+topoclient.BackupLeaseTTL, "before the store's earliest expiry")
	})
}
