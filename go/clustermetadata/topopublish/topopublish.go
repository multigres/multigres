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

// Package topopublish manages the publishing of components to the topo.
package topopublish

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/timertools"
)

// TopoPublisher contains the metadata of the component being published.
type TopoPublisher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger

	unpublish func(ctx context.Context) error
}

// Publish publishes the component using the publish function. If the publish function
// returns an error, it will be retried with exponential backoff until successful.
// The alarm will be invoked with the latest error message during retries. If the
// publishing succeeds, the alarm will be invoked with an empty string.
func Publish(publish func(ctx context.Context) error, unpublish func(ctx context.Context) error, alarm func(string)) *TopoPublisher {
	tp := &TopoPublisher{}
	tp.ctx, tp.cancel = context.WithCancel(context.Background())
	tp.logger = servenv.GetLogger()
	tp.unpublish = unpublish

	// Use tp's ctx to abort retries if Unpublish gets called.
	ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
	defer cancel()

	if err := publish(ctx); err == nil {
		tp.logger.Info("Successfully registered component with topology")
		return tp
	} else {
		alarm(fmt.Sprintf("Failed to register component with topology: %v", err))
		tp.logger.Error("Failed to register component with topology", "error", err)
	}
	tp.wg.Go(func() {
		ticker := timertools.NewBackoffTicker(10*time.Millisecond, 30*time.Second)
		for {
			select {
			case <-ticker.C:
				// Use tp's ctx to abort retries if Unpublish gets called.
				ctx, cancel := context.WithTimeout(tp.ctx, time.Second)
				if err := publish(ctx); err == nil {
					tp.logger.Info("Successfully registered component with topology")
					alarm("")
					cancel()
					return
				} else {
					// Just call alarm. No need to spam logs.
					alarm(fmt.Sprintf("Failed to register component with topology: %v", err))
				}
				cancel()
			case <-tp.ctx.Done():
				return
			}
		}
	})
	return tp
}

// Unpublish unpublished the component from topology.
// It will terminate any retry goroutines that are still running.
// It is safe to call Unpublish with a nil TopoPublisher.
func (tp *TopoPublisher) Unpublish() {
	// Safety
	if tp == nil {
		return
	}

	tp.cancel()
	tp.wg.Wait()

	// Use standalone ctx because tp.ctx is already canceled.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := tp.unpublish(ctx); err != nil {
		tp.logger.Error("Failed to deregister component from topology", "error", err)
	} else {
		tp.logger.Info("Successfully deregistered component from topology")
	}
}
