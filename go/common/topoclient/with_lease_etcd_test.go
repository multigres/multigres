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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
)

// A lease that WithLease acquired keeps being renewed until fn returns, even
// when the caller cancels its context while fn is still running. Without that,
// the key expires one TTL after the cancel and a competing holder acquires the
// lease while the first callback is still settling work that needs it.
func TestWithLeaseRenewsAfterCallerCancellationUntilFnReturns(t *testing.T) {
	if testing.Short() {
		t.Skip("starts etcd")
	}
	clientAddr, _, _ := etcdtopo.StartEtcd(t)
	const (
		key = "locks/renewal"
		ttl = 2 * time.Second
	)
	newConn := func() topoclient.Conn {
		conn, err := etcdtopo.Factory{}.Create("global", "/topo", []string{clientAddr})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	holder, competitor := newConn(), newConn()

	var desc topoclient.LockDescriptor
	acquire := func(ctx context.Context, action string) (context.Context, func(*error), error) {
		// Acquisition is bounded by its own context, which ends when it returns,
		// the way the topo client's lock call does.
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		d, err := holder.TryLockWithLease(actx, key, action, ttl)
		if err != nil {
			return nil, nil, err
		}
		desc = d
		return ctx, func(*error) { _ = d.Unlock(context.WithoutCancel(ctx)) }, nil
	}
	revoke := func(context.Context) error { return errors.New("no steal in this test") }
	check := func(ctx context.Context) error { return desc.Check(ctx) }

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fnRunning := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- topoclient.WithLease(ctx, "settling", acquire, revoke, check, func(fnCtx context.Context) error {
			close(fnRunning)
			cancel() // the caller goes away; fn is still settling
			<-release
			return nil
		}, topoclient.WithLeaseCheckInterval(200*time.Millisecond))
	}()
	<-fnRunning

	// More than two TTLs after the cancel, nobody else can take the lease.
	deadline := time.Now().Add(3 * ttl)
	for time.Now().Before(deadline) {
		d, err := competitor.TryLockWithLease(t.Context(), key, "competitor", ttl)
		if err == nil {
			_ = d.Unlock(t.Context())
			t.Fatal("a competing holder acquired the lease while the callback was still running")
		}
		time.Sleep(250 * time.Millisecond)
	}

	close(release)
	require.NoError(t, <-done)
	require.Eventually(t, func() bool {
		d, err := competitor.TryLockWithLease(t.Context(), key, "competitor", ttl)
		if err != nil {
			return false
		}
		_ = d.Unlock(t.Context())
		return true
	}, 5*time.Second, 100*time.Millisecond, "the lease is released once fn has returned")
	assert.NotNil(t, desc)
}

const leaseFixtureInterval = 200 * time.Millisecond

// leaseFixture is one etcd-backed lease whose check can be wrapped.
type leaseFixture struct {
	key        string
	ttl        time.Duration
	holder     topoclient.Conn
	competitor topoclient.Conn
	check      func(context.Context) error // the real check; set once acquired
}

func newLeaseFixture(t *testing.T, ttl time.Duration) *leaseFixture {
	t.Helper()
	clientAddr, _, _ := etcdtopo.StartEtcd(t)
	newConn := func() topoclient.Conn {
		conn, err := etcdtopo.Factory{}.Create("global", "/topo", []string{clientAddr})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	return &leaseFixture{key: "locks/monitor", ttl: ttl, holder: newConn(), competitor: newConn()}
}

// run holds the lease through WithLease with checkFn wrapped around the real
// check, and calls fn with the callback context.
func (f *leaseFixture) run(ctx context.Context, wrap func(real func(context.Context) error) func(context.Context) error, fn func(context.Context) error) error {
	acquire := func(ctx context.Context, action string) (context.Context, func(*error), error) {
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		d, err := f.holder.TryLockWithLease(actx, f.key, action, f.ttl)
		if err != nil {
			return nil, nil, err
		}
		f.check = d.Check
		return ctx, func(*error) { _ = d.Unlock(context.WithoutCancel(ctx)) }, nil
	}
	revoke := func(context.Context) error { return errors.New("no steal in this test") }
	check := func(ctx context.Context) error { return wrap(f.check)(ctx) }
	return topoclient.WithLease(ctx, "monitored", acquire, revoke, check, fn,
		topoclient.WithLeaseCheckInterval(leaseFixtureInterval), topoclient.WithLeaseTTL(f.ttl))
}

func (f *leaseFixture) competitorCanAcquire(t *testing.T) bool {
	t.Helper()
	d, err := f.competitor.TryLockWithLease(t.Context(), f.key, "competitor", f.ttl)
	if err != nil {
		return false
	}
	_ = d.Unlock(t.Context())
	return true
}

// Checks that fail for a while, within the loss window of the last success, do
// not end the lease: it is not declared lost and nobody else can take it.
func TestWithLeaseSurvivesTransientCheckFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("starts etcd")
	}
	f := newLeaseFixture(t, 3*time.Second)
	start := time.Now()
	wrap := func(real func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			if time.Since(start) < 1500*time.Millisecond {
				return errors.New("etcd unreachable")
			}
			return real(ctx)
		}
	}
	err := f.run(t.Context(), wrap, func(fnCtx context.Context) error {
		deadline := time.Now().Add(2 * f.ttl)
		for time.Now().Before(deadline) {
			require.NoError(t, context.Cause(fnCtx), "a transient failure must not cancel the callback")
			require.False(t, f.competitorCanAcquire(t), "the lease was taken during the callback")
			time.Sleep(250 * time.Millisecond)
		}
		return nil
	})
	require.NoError(t, err)
}

// A lease etcd no longer has (revoked) is declared lost at the next check.
func TestWithLeaseRevokeEndsItPromptly(t *testing.T) {
	if testing.Short() {
		t.Skip("starts etcd")
	}
	f := newLeaseFixture(t, 10*time.Second)
	held := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.run(t.Context(), func(real func(context.Context) error) func(context.Context) error { return real },
			func(fnCtx context.Context) error {
				close(held)
				<-fnCtx.Done()
				return context.Cause(fnCtx)
			})
	}()
	<-held
	revoked := time.Now()
	require.NoError(t, f.competitor.RevokeLockWithLease(t.Context(), f.key))
	select {
	case err := <-done:
		require.ErrorIs(t, err, topoclient.ErrLeaseLost)
		assert.Less(t, time.Since(revoked), 2*time.Second, "far sooner than the TTL")
	case <-time.After(8 * time.Second):
		t.Fatal("the revoke was not noticed")
	}
}

// Checks that keep failing end the lease between a TTL minus one interval and a
// TTL after the last success, and the test fails, instead of hanging, when the
// loss is never signalled.
func TestWithLeaseSustainedCheckFailureEndsItWithinAboutOneTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("starts etcd")
	}
	f := newLeaseFixture(t, 2*time.Second)
	wrap := func(func(context.Context) error) func(context.Context) error {
		return func(context.Context) error { return errors.New("etcd unreachable") }
	}
	began := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- f.run(t.Context(), wrap, func(fnCtx context.Context) error {
			select {
			case <-fnCtx.Done():
				return context.Cause(fnCtx)
			case <-time.After(f.ttl + time.Second):
				return errors.New("lease loss was not signalled within a TTL and a second")
			}
		})
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, topoclient.ErrLeaseLost)
	case <-time.After(f.ttl + 3*time.Second):
		t.Fatal("WithLease did not return")
	}
	took := time.Since(began)
	assert.GreaterOrEqual(t, took, f.ttl-leaseFixtureInterval, "not before a TTL minus one interval without a success")
	assert.Less(t, took, f.ttl+time.Second)
}
