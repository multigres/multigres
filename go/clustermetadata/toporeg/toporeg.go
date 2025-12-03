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

// Package toporeg manages the registration of components to the topo.
package toporeg

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/tools/retry"
)

// TopoReg contains the metadata of the component being registered.
type TopoReg struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger

	unregister func(ctx context.Context) error
}

// Register registers the component using the register function. If the register function
// returns an error, it will be retried with exponential backoff until successful.
// The alarm will be invoked with the latest error message during retries. If the
// registration succeeds, the alarm will be invoked with an empty string.
func Register(register func(ctx context.Context) error, unregister func(ctx context.Context) error, alarm func(string)) *TopoReg {
	tp := &TopoReg{}
	tp.ctx, tp.cancel = context.WithCancel(context.Background())
	tp.logger = servenv.GetLogger()
	tp.unregister = unregister

	// Use tp's ctx to abort retries if Unregister gets called.
	ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
	defer cancel()

	if err := register(ctx); err == nil {
		tp.logger.Info("Successfully registered component with topology")
		return tp
	} else {
		alarm(fmt.Sprintf("Failed to register component with topology: %v", err))
		tp.logger.Error("Failed to register component with topology", "error", err)
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
				tp.logger.Info("Successfully registered component with topology")
				alarm("")
				cancel()
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

// Unregister unregisters the component from topology.
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := tp.unregister(ctx); err != nil {
		tp.logger.Error("Failed to deregister component from topology", "error", err)
	} else {
		tp.logger.Info("Successfully deregistered component from topology")
	}
}
