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

// Package multiorch provides multiorch functionality.
package multiorch

import (
	"context"
	"log/slog"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topopublish"
	"github.com/multigres/multigres/go/servenv"
)

var (
	cell string

	ts     topo.Store
	logger *slog.Logger

	tp *topopublish.TopoPublisher
)

// Register flags that are specific to multiorch.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&cell, "cell", cell, "cell to use")
}

// Init initializes the multiorch. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()

	// This doesn't change
	serverStatus.Cell = cell

	logger.Info("multiorch starting up",
		"cell", cell,
		"http_port", servenv.HTTPPort(),
		"grpc_port", servenv.GRPCPort(),
	)

	// Create MultiOrch instance for topo registration
	// TODO(sougou): Is serviceID needed? It's sent as empty string for now.
	multiorch := topo.NewMultiOrch("", cell, servenv.Hostname)
	multiorch.PortMap["grpc"] = int32(servenv.GRPCPort())
	multiorch.PortMap["http"] = int32(servenv.HTTPPort())

	tp = topopublish.Publish(
		func(ctx context.Context) error { return ts.InitMultiOrch(ctx, multiorch, true) },
		func(ctx context.Context) error { return ts.DeleteMultiOrch(ctx, multiorch.Id) },
		func(s string) { serverStatus.InitError = s },
	)
}

func Shutdown() {
	logger.Info("multiorch shutting down")
	tp.Unpublish()
	ts.Close()
}
