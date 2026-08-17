// Copyright 2025 Supabase, Inc.
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

// Package toporeg manages the registration of components to the topoclient.
//
// TODO: Consider adding senv.RegisterWithTopology(register, unregister, alarm) to servenv
// to simplify the current pattern where services manually wire up OnRun/OnClose hooks.
// This would consolidate the registration lifecycle management into servenv.
package toporeg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/tools/retry"
)

// TopoReg contains the metadata of the component being registered.
type TopoReg struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger

	unregister func(ctx context.Context) error
	reassert   bool
}

// reassertInterval is how often a WithReassert registration rewrites its
// record. It bounds how long a component stays missing from the topology
// after its entry is lost, so it should stay well under the topology
// backend's liveness TTL (--topo-etcd-lease-ttl, 30s by default).
// It is a var only so tests can shorten it.
var reassertInterval = 10 * time.Second

// Option configures a registration.
type Option func(*TopoReg)

// WithReassert makes a registration self-healing: the component rewrites its
// record periodically, so the entry reappears on its own if anything removes
// it while the process is alive.
//
// Use it for ephemeral (liveness-bound) registrations, whose entry can vanish
// under the component in ways it never learns about: the backend expiring a
// lease after a topology outage, a reconnection replacing the underlying
// connection, or a cell reconfiguration replacing it wholesale. Without
// re-assertion the component would stay silently absent from the topology
// until it restarts.
//
// Do NOT use it for components that already maintain their own record on a
// schedule — multipooler publishes its lifecycle state continuously, and a
// second writer would both duplicate that and churn every watcher of those
// keys.
func WithReassert() Option {
	return func(tp *TopoReg) { tp.reassert = true }
}

// Register registers the component using the register function. If the register function
// returns an error, it will be retried with exponential backoff until successful.
// The alarm will be invoked with the latest error message during retries. If the
// registration succeeds, the alarm will be invoked with an empty string.
func Register(register func(ctx context.Context) error, unregister func(ctx context.Context) error, alarm func(string), opts ...Option) *TopoReg {
	tp := &TopoReg{}
	tp.ctx, tp.cancel = context.WithCancel(context.TODO())
	tp.logger = servenv.GetLogger()
	tp.unregister = unregister
	for _, opt := range opts {
		opt(tp)
	}

	// Use tp's ctx to abort retries if Unregister gets called.
	ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
	defer cancel()

	if err := register(ctx); err == nil {
		tp.logger.Info("successfully registered component with topology")
		tp.startReassert(register)
		return tp
	} else {
		alarm(fmt.Sprintf("Failed to register component with topology: %v", err))
		tp.logger.Error("failed to register component with topology", "error", err)
	}
	tp.wg.Go(func() {
		// We've already tried once. Use WithInitialDelay to wait before retrying.
		r := retry.New(10*time.Millisecond, 30*time.Second, retry.WithInitialDelay())
		for _, err := range r.Attempts(tp.ctx) {
			if err != nil {
				// Context cancelled
				return
			}

			ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
			if err := register(ctx); err == nil {
				tp.logger.Info("successfully registered component with topology")
				alarm("")
				cancel()
				tp.startReassert(register)
				return
			} else {
				// Just call alarm. No need to spam logs.
				alarm(fmt.Sprintf("Failed to register component with topology: %v", err))
			}
			cancel()
		}
	})
	return tp
}

// startReassert runs the re-assertion loop for WithReassert registrations.
// It is a no-op otherwise. The loop lives on tp.wg and stops on tp.ctx, so
// Unregister's cancel-then-wait halts it before the deregistration runs —
// re-assertion can never resurrect a record the component just removed.
func (tp *TopoReg) startReassert(register func(ctx context.Context) error) {
	if !tp.reassert {
		return
	}
	tp.wg.Go(func() {
		ticker := time.NewTicker(reassertInterval)
		defer ticker.Stop()
		for {
			select {
			case <-tp.ctx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
				err := register(ctx)
				cancel()
				if err != nil {
					// The next tick tries again; a component missing from
					// topology is visible in its own logs either way.
					tp.logger.Warn("failed to re-assert topology registration", "error", err)
				}
			}
		}
	})
}

// RegisterSynchronous registers the component synchronously, retrying with
// exponential backoff and jitter until successful or the context expires.
// Unlike Register, it blocks until registration succeeds and returns an error
// if it cannot complete within the context deadline.
//
// Use this when the caller must know registration succeeded before proceeding
// (e.g., claiming a PID prefix that other components depend on).
func RegisterSynchronous(ctx context.Context, register func(ctx context.Context) error, unregister func(ctx context.Context) error, opts ...Option) (*TopoReg, error) {
	tp := &TopoReg{}
	tp.ctx, tp.cancel = context.WithCancel(context.TODO())
	tp.logger = servenv.GetLogger()
	tp.unregister = unregister
	for _, opt := range opts {
		opt(tp)
	}

	r := retry.New(50*time.Millisecond, 1*time.Second)
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			return nil, fmt.Errorf("registration failed: %w", err)
		}

		regCtx, cancel := context.WithTimeout(ctx, time.Second)
		err = register(regCtx)
		cancel()
		if err == nil {
			tp.logger.InfoContext(ctx, "successfully registered component with topology")
			tp.startReassert(register)
			return tp, nil
		}
	}

	return nil, errors.New("registration failed")
}

// unregisterBudget bounds the total time Unregister spends retrying the
// deregistration. It is deliberately smaller than servenv's OnClose window
// (default 10s) so the caller's remaining shutdown steps still get a share
// of that window. A longer budget would buy nothing anyway: if
// deregistration is still failing after several seconds, the topology is
// unreachable, and lease expiry cleans up the registration instead.
// It is a var only so tests can shorten it.
var unregisterBudget = 5 * time.Second

// Unregister unregisters the component from topology, retrying with backoff
// until it succeeds or the budget expires. Registration retries until it
// succeeds; a deregistration that gives up after one attempt would leave the
// component visible until its lease expires, so it deserves the same
// persistence.
// It will terminate any retry goroutines that are still running.
// It is safe to call Unregister with a nil TopoReg.
func (tp *TopoReg) Unregister() {
	// Safety
	if tp == nil {
		return
	}

	tp.cancel()
	tp.wg.Wait()

	// Use standalone ctx because tp.ctx is already canceled.
	ctx, cancel := context.WithTimeout(context.TODO(), unregisterBudget)
	defer cancel()

	r := retry.New(100*time.Millisecond, 2*time.Second)
	var lastErr error
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			tp.logger.Error("failed to deregister component from topology",
				"error", lastErr, "budget", unregisterBudget)
			return
		}

		attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Second)
		lastErr = tp.unregister(attemptCtx)
		attemptCancel()
		if lastErr == nil || errors.Is(lastErr, &topoclient.TopoError{Code: topoclient.NoNode}) {
			// NoNode counts as success: a previous attempt's delete may
			// have been applied even though its response timed out.
			tp.logger.Info("successfully deregistered component from topology")
			return
		}
	}
}
