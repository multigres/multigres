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

// Package manager implements the core MultiPoolerManager business logic
package manager

import (
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
)

// Config holds configuration for the MultiPoolerManager
type Config struct {
	SocketFilePath      string
	TopoClient          topoclient.Store
	HeartbeatIntervalMs int
	PgctldAddr          string                  // Address of pgctld gRPC service
	ConsensusEnabled    bool                    // Whether consensus gRPC service is enabled
	ConnPoolConfig      *connpoolmanager.Config // Connection pool config (manager created in MultiPoolerManager)
	// pgBackRest TLS certificate paths (used for both server and client authentication)
	PgBackRestCertFile string
	PgBackRestKeyFile  string
	PgBackRestCAFile   string
	PgBackRestPort     int // pgBackRest TLS server port
}
