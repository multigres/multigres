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
	"context"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
)

// Config holds configuration for the MultiPoolerManager
type Config struct {
	SocketFilePath      string
	TopoClient          topoclient.Store
	HeartbeatIntervalMs int
	PgctldAddr          string                  // Address of pgctld gRPC service
	ConsensusEnabled    bool                    // Whether consensus gRPC service is enabled
	ConnPoolConfig      *connpoolmanager.Config // Connection pool config (manager created in MultiPoolerManager)
	// pgBackRest TLS certificate paths for connecting to primary's pgBackRest server
	PgBackRestCertFile string // TLS client certificate file path
	PgBackRestKeyFile  string // TLS client key file path
	PgBackRestCAFile   string // TLS CA certificate file path
	// VpidStampEnabled toggles stamping of multigres_vpid:<id> on PostgreSQL
	// backends (and the matching application_name filter on pool settings).
	VpidStampEnabled bool

	// Graceful shutdown timeouts. The escalation budget (Fast + Immediate) plus
	// FinalSnapshotTimeout must fit inside the announced shutdown deadline.
	// Drain is handled by --connpool-drain-grace-period before the announce,
	// so it is not part of the announced budget.
	GracefulShutdownFastTimeout          time.Duration // Bound on the fast-mode pgctld.Stop call before escalating to immediate.
	GracefulShutdownImmediateTimeout     time.Duration // Bound on the immediate-mode pgctld.Stop call.
	GracefulShutdownFinalSnapshotTimeout time.Duration // Time allowed to deliver the final STOPPED snapshot before forcing stream close.

	// FlushTelemetry is invoked by the graceful-shutdown safety-net just
	// before it force-exits the process when the announced shutdown deadline
	// is exceeded. The hook is expected to flush whatever pipelines its
	// caller owns (OTel tracer/meter/log providers) under the supplied
	// bounded context. May be nil; the safety-net then skips the flush.
	//
	// Wired from init.go to telemetry.ShutdownTelemetry. Kept as a callback
	// rather than a typed field so the manager package does not depend on
	// the telemetry package.
	FlushTelemetry func(ctx context.Context) error
}
