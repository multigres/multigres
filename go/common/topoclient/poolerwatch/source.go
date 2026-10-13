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

package poolerwatch

import (
	"context"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/common/topoclient"
)

// Source delivers pooler events to a PoolerCache, from the topology
// (TopoSource) or, for a pooler in the same process, from the pooler itself.
type Source interface {
	// Start delivers events to handlers until ctx ends or Stop. Called once.
	Start(ctx context.Context, logger *slog.Logger, handlers Handlers)

	// Stop stops delivery and waits for in-flight callbacks.
	Stop()
}

type TopoSource struct {
	store topoclient.ConnProvider

	mu    sync.Mutex
	watch *topoWatch
}

// NewTopoSource watches the topology for poolers in every cell.
func NewTopoSource(store topoclient.ConnProvider) *TopoSource {
	return &TopoSource{store: store}
}

func (s *TopoSource) Start(ctx context.Context, logger *slog.Logger, handlers Handlers) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watch = newTopoWatch(ctx, s.store, logger, handlers)
	s.watch.Start()
}

func (s *TopoSource) Stop() {
	s.mu.Lock()
	watch := s.watch
	s.mu.Unlock()
	if watch != nil {
		watch.Stop()
	}
}

// sync waits for every queued event; test-only, through SyncForTest.
func (s *TopoSource) sync(ctx context.Context) error {
	s.mu.Lock()
	watch := s.watch
	s.mu.Unlock()
	if watch == nil {
		return nil
	}
	return watch.sync(ctx)
}
