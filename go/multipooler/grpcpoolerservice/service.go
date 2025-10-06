// Copyright 2025 Supabase, Inc.
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

// Package grpcpoolerservice implements the gRPC server for MultiPooler
package grpcpoolerservice

import (
	"log/slog"

	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	"github.com/multigres/multigres/go/servenv"
)

func init() {
	// Register ourselves to be invoked when the pooler server registration is triggered
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, func(logger *slog.Logger, config *manager.Config) {
		if servenv.GRPCCheckServiceMap("pooler") {
			poolerServer := poolerserver.NewMultiPoolerServer(logger, config)
			poolerServer.RegisterWithGRPCServer(servenv.GRPCServer)
			logger.Info("MultiPooler gRPC service registered with servenv")
		}
	})
}
