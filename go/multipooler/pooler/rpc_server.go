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

package pooler

import (
	"log/slog"

	"github.com/multigres/multigres/go/multipooler/manager"
)

// RegisterPoolerServiceFunc is used to delay registration of pooler gRPC servers until we have all the objects.
// Following Vitess pattern from vttablet/tabletmanager/rpc_server.go
type RegisterPoolerServiceFunc func(*slog.Logger, *manager.Config)

// RegisterPoolerServices is a list of functions to call when the delayed gRPC registration is triggered.
var RegisterPoolerServices []RegisterPoolerServiceFunc

// registerGRPCServices will register all the pooler gRPC service instances.
func (s *MultiPoolerServer) registerGRPCServices() {
	for _, f := range RegisterPoolerServices {
		f(s.logger, s.config)
	}
}
