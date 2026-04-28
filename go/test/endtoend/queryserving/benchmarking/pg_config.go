// Copyright 2026 Supabase, Inc.
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

package benchmarking

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// bumpPostgresMaxConnections rewrites postgresql.auto.conf on every pgctld
// in the cluster to set max_connections=n, then restarts each postgres via
// pgctld so the new value takes effect.
//
// Required when running pgbench with more clients than the default
// postgres max_connections (60).
func bumpPostgresMaxConnections(ctx context.Context, t *testing.T, setup *shardsetup.ShardSetup, n int) {
	t.Helper()

	for name, inst := range setup.Multipoolers {
		if err := writeMaxConnectionsOverride(inst.Pgctld.PoolerDir, n); err != nil {
			t.Fatalf("write max_connections override on %s: %v", name, err)
		}
		t.Logf("Wrote max_connections=%d override on %s (data dir: %s)", n, name, inst.Pgctld.PoolerDir)

		// Disable the multipooler monitor's automatic postgres restart so it
		// doesn't race with our pgctld Restart.
		mp, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Fatalf("connect to multipooler on %s: %v", name, err)
		}
		_, err = mp.Manager.SetPostgresRestartsEnabled(ctx,
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		mp.Close()
		if err != nil {
			t.Fatalf("disable monitor restarts on %s: %v", name, err)
		}

		pg, err := shardsetup.NewPgctldClient(inst.Pgctld.GrpcPort)
		if err != nil {
			t.Fatalf("connect to pgctld on %s: %v", name, err)
		}

		restartCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		_, err = pg.Restart(restartCtx, &pgctldpb.RestartRequest{Mode: "fast"})
		cancel()
		pg.Close()
		if err != nil {
			t.Fatalf("pgctld Restart on %s: %v", name, err)
		}

		// Re-enable the monitor.
		mp2, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Fatalf("re-connect to multipooler on %s: %v", name, err)
		}
		_, err = mp2.Manager.SetPostgresRestartsEnabled(ctx,
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		mp2.Close()
		if err != nil {
			t.Fatalf("re-enable monitor restarts on %s: %v", name, err)
		}

		t.Logf("Restarted postgres on %s with max_connections=%d", name, n)
	}
}

const maxConnectionsMarker = "# multigres_pgbench_override\n"

// writeMaxConnectionsOverride appends (idempotently) the override to
// postgresql.auto.conf inside the postgres data directory.
func writeMaxConnectionsOverride(poolerDir string, n int) error {
	autoConf := filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf")

	existing, err := os.ReadFile(autoConf)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", autoConf, err)
	}

	// If we already wrote the override, replace the line; otherwise append.
	override := fmt.Sprintf("%smax_connections = %d\n", maxConnectionsMarker, n)
	out := stripPriorOverride(string(existing)) + override

	if err := os.WriteFile(autoConf, []byte(out), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", autoConf, err)
	}
	return nil
}

// stripPriorOverride removes the previous max_connections override block
// (marker + immediate next line) so we can rewrite cleanly. Anything else
// in postgresql.auto.conf is preserved.
func stripPriorOverride(in string) string {
	idx := -1
	if i := indexOf(in, maxConnectionsMarker); i >= 0 {
		idx = i
	}
	if idx < 0 {
		return in
	}
	prefix := in[:idx]
	rest := in[idx+len(maxConnectionsMarker):]
	// drop the next line (the actual setting)
	if nl := indexOf(rest, "\n"); nl >= 0 {
		rest = rest[nl+1:]
	} else {
		rest = ""
	}
	return prefix + rest
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
