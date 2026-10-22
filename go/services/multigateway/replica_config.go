// Copyright 2026 Supabase, Inc.
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

package multigateway

import (
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// replicaConfig holds the settings for replica reads. A nil *replicaConfig
// means replica reads are unavailable: no flags, port 0 and no lag thresholds.
type replicaConfig struct {
	// port is the optional port for replica-reads connections.
	// When set, a second listener accepts connections that are allowed to read from replicas.
	port viperutil.Value[int]
	// lowLagMs is the preferred replication lag threshold (ms) for replicas.
	// Replicas at or below this lag are considered "healthy" and preferred.
	lowLagMs viperutil.Value[int]
	// highLagToleranceMs is the absolute maximum replication lag (ms).
	// Replicas above this are never selected. 0 means no upper bound.
	highLagToleranceMs viperutil.Value[int]
}

func newReplicaConfig(reg *viperutil.Registry) *replicaConfig {
	return &replicaConfig{
		port: viperutil.Configure(reg, "pg-replica-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "pg-replica-port",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_REPLICA_PORT"},
		}),
		lowLagMs: viperutil.Configure(reg, "low-replication-lag-ms", viperutil.Options[int]{
			Default:  30000,
			FlagName: "low-replication-lag-ms",
			Dynamic:  false,
			EnvVars:  []string{"MT_LOW_REPLICATION_LAG_MS"},
		}),
		highLagToleranceMs: viperutil.Configure(reg, "high-replication-lag-tolerance-ms", viperutil.Options[int]{
			Default:  0,
			FlagName: "high-replication-lag-tolerance-ms",
			Dynamic:  false,
			EnvVars:  []string{"MT_HIGH_REPLICATION_LAG_TOLERANCE_MS"},
		}),
	}
}

func (rc *replicaConfig) registerFlags(fs *pflag.FlagSet) {
	if rc == nil {
		return
	}
	fs.Int("pg-replica-port", rc.port.Default(), "optional port for replica-reads connections; 0 disables the replica listener")
	fs.Int("low-replication-lag-ms", rc.lowLagMs.Default(), "replicas at or below this lag (milliseconds) are preferred; 0 treats all replicas equally")
	fs.Int("high-replication-lag-tolerance-ms", rc.highLagToleranceMs.Default(), "absolute max lag (milliseconds) for replicas; 0 means no upper bound")
	viperutil.BindFlags(fs, rc.port, rc.lowLagMs, rc.highLagToleranceMs)
}

// Port returns the replica-reads port; 0 disables the replica listener.
func (rc *replicaConfig) Port() int {
	if rc == nil {
		return 0
	}
	return rc.port.Get()
}

// LowLag returns the preferred replication-lag threshold.
func (rc *replicaConfig) LowLag() time.Duration {
	if rc == nil {
		return 0
	}
	return time.Duration(rc.lowLagMs.Get()) * time.Millisecond
}

// HighTolerance returns the absolute maximum replication lag.
func (rc *replicaConfig) HighTolerance() time.Duration {
	if rc == nil {
		return 0
	}
	return time.Duration(rc.highLagToleranceMs.Get()) * time.Millisecond
}
