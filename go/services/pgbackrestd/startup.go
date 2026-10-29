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
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/multigres/multigres/go/common/safefs"
)

const (
	// startupStartTimeout bounds `pgbackrest start`, so a wedged child cannot
	// hold the listeners closed.
	startupStartTimeout = 30 * time.Second
	maxStartLogBytes    = 2048

	startRetryMin = 5 * time.Second
	startRetryMax = 5 * time.Minute
)

// runStartupDuties runs the work that must finish before any listener opens.
//
// A failed `pgbackrest start` is logged, not returned: the container must
// come up so postgres can start, and a crashloop here would hide the cause
// while blocking everything. It is retried in the background instead, because
// a stop file left by an interrupted restore refuses every archive-push until
// something clears it.
func (s *Server) runStartupDuties(ctx context.Context) error {
	if err := s.clearTmp(); err != nil {
		return err
	}

	// A restore the sidecar died in the middle of leaves its stop file
	// behind, which would refuse every archive-push after the restart.
	if !s.runStart(ctx) {
		s.goBackground(func() { s.retryStart(ctx) })
	}
	return nil
}

// runStart runs `pgbackrest start` once and reports whether it succeeded.
func (s *Server) runStart(ctx context.Context) bool {
	startCtx, cancel := context.WithTimeout(ctx, startupStartTimeout)
	defer cancel()
	var out bytes.Buffer
	code, err := s.runChild(startCtx, KindS3Command, cmdStart, nil, &out, &out)
	switch {
	case err != nil:
		s.logger.ErrorContext(ctx, "pgbackrest start failed; a stale stop file may remain", "error", err)
	case code != 0:
		s.logger.ErrorContext(ctx, "pgbackrest start failed; a stale stop file may remain", "exit_code", code, "output", truncate(out.String(), maxStartLogBytes))
	default:
		return true
	}
	return false
}

// retryStart keeps trying `start` with capped backoff until one succeeds or
// ctx ends. Each attempt holds the request gate: a restore keeps the gate
// closed from before its `stop` until after its own `start`, so an attempt
// either finds the gate closed and waits, or finishes before the restore's
// Close returns. It can therefore never remove a live restore's stop file.
func (s *Server) retryStart(ctx context.Context) {
	p := &s.pg
	backoff, maxBackoff := p.startRetryMin, p.startRetryMax
	if backoff == 0 {
		backoff = startRetryMin
	}
	if maxBackoff == 0 {
		maxBackoff = startRetryMax
	}
	sleep := p.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for sleep(ctx, backoff) {
		backoff = min(backoff*2, maxBackoff)
		release, ok := s.gate.TryEnter()
		if !ok {
			continue
		}
		done := s.runStart(ctx)
		release()
		if done {
			s.logger.InfoContext(ctx, "pgbackrest start succeeded on retry")
			return
		}
	}
}

// clearTmp empties tmp/. A previous container run's scratch must not leak
// into this one, and children can plant links there, so it goes through
// safefs.
func (s *Server) clearTmp() error {
	root, err := safefs.OpenRoot(s.cfg.StateDir)
	if err != nil {
		return fmt.Errorf("open state dir: %w", err)
	}
	defer root.Close()
	if err := root.RemoveAll(dirTmp); err != nil {
		return fmt.Errorf("clear tmp: %w", err)
	}
	tmp := s.cfg.stateSub(dirTmp)
	if err := os.Mkdir(tmp, 0o700); err != nil {
		return fmt.Errorf("recreate tmp: %w", err)
	}
	if err := os.Chmod(tmp, 0o700); err != nil {
		return fmt.Errorf("chmod tmp: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
