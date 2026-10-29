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

package pgbackrestd

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pgServerMinBackoff = time.Second
	pgServerMaxBackoff = time.Minute
	// pgServerUpGrace is how long a server child must survive before it is
	// reported up, so a child that dies on a bad config does not flap the
	// gauge and Status on every attempt.
	pgServerUpGrace = time.Second
)

// pgServer supervises the 8432 server child. The zero value is usable; tests
// shorten the timings before startPgServer.
type pgServer struct {
	mu      sync.Mutex
	paused  int
	cancel  context.CancelFunc // cancels the run in flight, nil between runs
	done    chan struct{}      // closed when the run in flight exits, nil between runs
	resumed chan struct{}      // closed on the last resume, nil when nobody waits

	// bg tracks the goroutines Run must wait for on shutdown: the supervisor
	// and the boot-time start retry. A supervisor left running would orphan
	// its child process group, which under the local provisioner keeps the
	// 8432 port bound.
	bg sync.WaitGroup

	up         atomic.Bool
	restarts   atomic.Int64
	startedAt  atomic.Int64
	minBackoff time.Duration
	maxBackoff time.Duration
	upGrace    time.Duration
	// stableRun is how long a child must run for its backoff to reset;
	// zero means maxBackoff.
	stableRun time.Duration
	// sleep waits d or until ctx ends, reporting whether the full wait
	// elapsed; tests replace it to observe the backoff sequence.
	sleep func(ctx context.Context, d time.Duration) bool

	startRetryMin time.Duration
	startRetryMax time.Duration
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (p *pgServer) timings() (minBackoff, maxBackoff, upGrace time.Duration) {
	minBackoff, maxBackoff, upGrace = p.minBackoff, p.maxBackoff, p.upGrace
	if minBackoff == 0 {
		minBackoff = pgServerMinBackoff
	}
	if maxBackoff == 0 {
		maxBackoff = pgServerMaxBackoff
	}
	if upGrace == 0 {
		upGrace = pgServerUpGrace
	}
	return minBackoff, maxBackoff, upGrace
}

// startPgServer supervises the pgBackRest TLS server child until ctx ends.
// It never returns an error for a child that cannot run (missing certificates,
// a crash): the sidecar must stay up so postgres can archive and restore
// without 8432, and the child is retried with capped backoff, forever. A loop
// that gave up after a few attempts would leave standby backups broken for the
// life of the container.
//
// A server that cannot start, a missing certificate directory included, is
// reported only through the pgbackrest.server.up gauge and Status.server_up.
// The health probe deliberately ignores it: the probe gates postgres from
// starting, and a server problem must never block the primary.
func (s *Server) startPgServer(ctx context.Context) error {
	s.registerPgServerMetrics()
	s.goBackground(func() { s.superviseServer(ctx) })
	return nil
}

// stopBackground waits for the supervisor and the start retry to finish. The
// caller must have cancelled the context they run on; cancelling kills the
// child's process group, so the wait is short.
func (s *Server) stopBackground() {
	s.bgMu.Lock()
	s.bgStopped = true
	s.bgMu.Unlock()
	s.pg.bg.Wait()
}

func (s *Server) superviseServer(ctx context.Context) {
	p := &s.pg
	minBackoff, maxBackoff, upGrace := p.timings()
	stableRun := p.stableRun
	if stableRun == 0 {
		stableRun = maxBackoff
	}
	sleep := p.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	backoff := minBackoff
	for {
		if !s.waitUnpaused(ctx) {
			return
		}
		p.mu.Lock()
		// Re-check under the lock that registers the run, so a pause either
		// sees this run's cancel func or has already made us wait.
		if p.paused > 0 {
			p.mu.Unlock()
			continue
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		p.cancel, p.done = cancel, done
		p.mu.Unlock()

		started := time.Now()
		grace := time.AfterFunc(upGrace, func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.done == done {
				p.startedAt.Store(time.Now().UnixNano())
				p.up.Store(true)
			}
		})
		code, err := s.runChild(runCtx, KindServer, cmdServer, nil, os.Stdout, os.Stderr)
		grace.Stop()

		p.mu.Lock()
		p.up.Store(false)
		p.startedAt.Store(0)
		p.cancel, p.done = nil, nil
		wasPaused := p.paused > 0
		p.mu.Unlock()
		cancel()
		close(done)

		if ctx.Err() != nil {
			return
		}
		if wasPaused {
			continue
		}
		if time.Since(started) > stableRun {
			backoff = minBackoff
		}
		s.logger.WarnContext(ctx, "pgbackrest server exited, restarting", "exit_code", code, "error", err, "backoff", backoff)
		p.restarts.Add(1)
		if !sleep(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// waitUnpaused blocks while the server is paused. It reports false once ctx
// ends.
func (s *Server) waitUnpaused(ctx context.Context) bool {
	p := &s.pg
	for {
		p.mu.Lock()
		if p.paused == 0 {
			p.mu.Unlock()
			return ctx.Err() == nil
		}
		if p.resumed == nil {
			p.resumed = make(chan struct{})
		}
		ch := p.resumed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-ch:
		}
	}
}

// serverUp reports whether the pgBackRest server child is running.
func (s *Server) serverUp() bool { return s.pg.up.Load() }

// pauseServer stops the server child, waits for it to exit and keeps it down
// until the returned function is called. Calls nest: the server restarts when
// every pause has been resumed. The wait ends because a cancelled run kills
// the child's whole process group.
func (s *Server) pauseServer() (resume func()) {
	p := &s.pg
	p.mu.Lock()
	p.paused++
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.paused--
			if p.paused == 0 && p.resumed != nil {
				close(p.resumed)
				p.resumed = nil
			}
		})
	}
}
