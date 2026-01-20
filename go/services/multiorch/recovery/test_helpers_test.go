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

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/coordinator"
)

// testEngineOptions contains options for creating a test engine.
type testEngineOptions struct {
	fakeClient   *rpcclient.FakeClient
	watchTargets []config.WatchTarget
}

// TestEngineOption is a functional option for newTestEngine.
type TestEngineOption func(*testEngineOptions)

// WithFakeClient sets a custom FakeClient for the test engine.
func WithFakeClient(client *rpcclient.FakeClient) TestEngineOption {
	return func(opts *testEngineOptions) {
		opts.fakeClient = client
	}
}

// WithWatchTargets sets watch targets for the test engine.
func WithWatchTargets(targets []config.WatchTarget) TestEngineOption {
	return func(opts *testEngineOptions) {
		opts.watchTargets = targets
	}
}

// newTestEngine creates a test Engine with a non-nil coordinator.
// This is a shared helper for all recovery package tests.
func newTestEngine(ctx context.Context, t *testing.T, opts ...TestEngineOption) *Engine {
	t.Helper()

	options := &testEngineOptions{
		fakeClient:   &rpcclient.FakeClient{},
		watchTargets: []config.WatchTarget{},
	}
	for _, opt := range opts {
		opt(options)
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	coordID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coordinator",
	}
	coord := coordinator.NewCoordinator(coordID, ts, options.fakeClient, logger)
	return NewEngine(ts, logger, cfg, options.watchTargets, options.fakeClient, coord)
}

// newTestCoordinator creates a coordinator for tests that need one but manage their own Engine creation.
func newTestCoordinator(ts topoclient.Store, rpcClient rpcclient.MultiPoolerClient, cell string) *coordinator.Coordinator {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	coordID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIORCH,
		Cell:      cell,
		Name:      "test-coordinator",
	}
	return coordinator.NewCoordinator(coordID, ts, rpcClient, logger)
}
