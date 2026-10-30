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

	"github.com/multigres/multigres/go/common/sandbox"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
)

// Config holds configuration for the MultipoolerManager
type Config struct {
	SocketFilePath      string
	TopoClient          topoclient.Store
	HeartbeatIntervalMs int
	// HealthStreamStalenessTimeout overrides the staleness window this pooler
	// advertises to the gateway (RecommendedStalenessTimeout). Zero keeps the
	// built-in default (defaultRecommendedStalenessTimeout).
	HealthStreamStalenessTimeout time.Duration
	// ReplicationStatsPollIntervalMs configures how often the
	// replicationstats poller queries pg_stat_replication/pg_replication_slots
	// for logical-replication connection metrics. Zero selects the built-in
	// default (10s).
	ReplicationStatsPollIntervalMs int
	PgctldAddr                     string // Address of pgctld gRPC service
	ConsensusEnabled               bool   // Whether consensus gRPC service is enabled
	// MigrationTargetAdvertiseHost is the gateway address an external source uses
	// to reach this Multigres shard for the EXPORT-direction reverse subscription.
	// It must be the multigateway, whose replication=database tunnel proxies the
	// stream to the current primary and re-pins it across failover, not this
	// pooler's cluster-internal topo Hostname, which an external source cannot
	// resolve. Empty makes EXPORT unavailable.
	MigrationTargetAdvertiseHost string
	// MigrationTargetAdvertisePort is the Postgres-facing port paired with
	// MigrationTargetAdvertiseHost. Zero selects the gateway's default Postgres
	// protocol port.
	MigrationTargetAdvertisePort int
	ConnPoolConfig               *connpoolmanager.Config // Connection pool config (manager created in MultipoolerManager)
	BackendVpidTrackingEnabled   bool                    // Whether to write active gateway-vpid/backend-pid mappings
	// SlotBasedReplicationEnabled reads whether slot-based physical replication is
	// enabled. It is a getter (not a bool) so the value is read live: the backing
	// flag is dynamic and reloadable at runtime. Nil reads as disabled (see
	// MultipoolerManager.slotBasedReplicationEnabled).
	SlotBasedReplicationEnabled func() bool

	// StandbyStuckDivergenceThreshold is how long a standby must stay unable to
	// stream from its correctly-recorded leader before the monitor concludes its
	// WAL diverged and self-heals via pg_rewind. Zero selects the built-in default
	// (standbyStuckDivergenceThreshold). Not wired to a CLI flag — it exists as an
	// internal, programmatic override for tests; production uses the default.
	StandbyStuckDivergenceThreshold time.Duration

	// PostgresUnrecoverableTimeout is how long postgres may continuously fail to
	// start/rewind/restore before the monitor gives up and quarantines the pooler
	// (LIFECYCLE_QUARANTINED) so it is replaced rather than FATAL-looping forever.
	// The timeout is the primary gate; a small minimum-attempts floor
	// (unrecoverableMinAttempts) additionally guards against quarantining on too
	// few real attempts (e.g. a single hung attempt). 0 disables the classifier
	// (the monitor keeps retrying indefinitely, the pre-quarantine behaviour).
	PostgresUnrecoverableTimeout time.Duration

	// PostgresUnrecoverableMinAttempts is the minimum-attempts floor described
	// above. <= 0 falls back to defaultUnrecoverableMinAttempts.
	PostgresUnrecoverableMinAttempts int

	// PgBackRestControlSocket is the pgbackrestd control socket (required). WAL
	// archive and restore go through the sidecar and the shared data volume is
	// accessed only through mount-anchored safefs.
	PgBackRestControlSocket string
	// PgBackRestWALSocket is the sidecar's WAL socket as postgres sees it (required). It is
	// only a path rendered into archive_command and restore_command; multipooler
	// has no mount for it.
	PgBackRestWALSocket string
	// DataVolumeRoot is the mount root of the volume shared with postgres. Empty
	// selects DefaultDataVolumeRoot.
	DataVolumeRoot string
	// DataVolumeAnchor selects how DataVolumeRoot is verified. The zero value
	// requires a mount point; sandbox.AnchorDir is for tests and the local
	// provisioner only.
	DataVolumeAnchor sandbox.AnchorMode

	// StaticLeader makes this pooler its shard's leader without consensus, for a
	// shard served by a single pooler (Minigres). Routing-role derivation uses a
	// fixed consensus status naming this pooler as leader, and the postgres
	// monitor starts postgres as a primary rather than a standby.
	StaticLeader bool
}
