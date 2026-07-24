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

// Package manager implements the core MultipoolerManager business logic
package manager

import (
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
)

// Config holds configuration for the MultipoolerManager
type Config struct {
	SocketFilePath             string
	TopoClient                 topoclient.Store
	HeartbeatIntervalMs        int
	PgctldAddr                 string                  // Address of pgctld gRPC service
	ConsensusEnabled           bool                    // Whether consensus gRPC service is enabled
	ConnPoolConfig             *connpoolmanager.Config // Connection pool config (manager created in MultipoolerManager)
	BackendVpidTrackingEnabled bool                    // Whether to write active gateway-vpid/backend-pid mappings

	// StandbyStuckDivergenceThreshold is how long a standby must stay unable to
	// stream from its correctly-recorded leader before the monitor concludes its
	// WAL diverged and self-heals via pg_rewind. Zero selects the built-in default
	// (standbyStuckDivergenceThreshold). Not wired to a CLI flag — it exists as an
	// internal, programmatic override for tests; production uses the default.
	StandbyStuckDivergenceThreshold time.Duration

	// pgBackRest TLS certificate paths for connecting to primary's pgBackRest server
	PgBackRestCertFile string // TLS client certificate file path
	PgBackRestKeyFile  string // TLS client key file path
	PgBackRestCAFile   string // TLS CA certificate file path

	// BackupCipherKeys holds the backup repository cipher keys resolved from
	// the key file at startup (nil when no key file is configured). Loaded
	// once at process start, like the postgres password.
	BackupCipherKeys backup.CipherKeys
}
