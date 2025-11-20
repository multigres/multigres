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
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Config holds configuration for the MultiPoolerManager
type Config struct {
	SocketFilePath      string
	PoolerDir           string
	PgPort              int
	Database            string
	TopoClient          topo.Store
	ServiceID           *clustermetadatapb.ID
	HeartbeatIntervalMs int
	PgctldAddr          string // Address of pgctld gRPC service
	PgBackRestStanza    string // pgBackRest stanza name (defaults to service ID if empty)
	ConsensusEnabled    bool   // Whether consensus gRPC service is enabled

	// Connection Pool Configuration
	PoolEnabled     bool          // Enable connection pooling (default: true)
	PoolCapacity    int           // Max connections (default: 100)
	PoolMaxIdle     int           // Max idle connections (default: 50)
	PoolIdleTimeout time.Duration // Idle timeout (default: 5min)
	PoolMaxLifetime time.Duration // Max connection lifetime (default: 1hr)
}
