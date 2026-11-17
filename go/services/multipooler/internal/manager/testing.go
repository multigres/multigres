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

package manager

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// NewManagerForTesting builds a manager of the given leadership mode and tablegroup over an
// in-memory topology, for tests of the service layers that act on the mode alone and need no
// postgres. An unmanaged manager gets an external backend, as the constructor requires one.
// The manager is shut down when the test ends.
//
// It takes a testing.TB to mark it test-only. It lives here, not in a _test.go file, because
// the service packages' tests are in other packages.
func NewManagerForTesting(t testing.TB, mode clustermetadatapb.LeadershipMode, tablegroup string) *MultipoolerManager {
	t.Helper()
	// Detached: the shutdown runs from t.Cleanup, where t.Context() is already cancelled.
	ctx := ctxutil.Detach(t.Context())
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	cfg := &Config{TopoClient: ts, ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry())}
	if mode == clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED {
		cfg.ExternalBackend = &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"}
	}
	pm, err := NewMultipoolerManager(slog.New(slog.NewTextHandler(os.Stderr, nil)), &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler"},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: tablegroup, Shard: "0-inf"},
		LeadershipMode: mode,
	}, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { pm.ShutdownForTest(ctx) })
	return pm
}
