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

package consensus

import (
	"context"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// testBootstrapPolicy returns a minimal valid durability policy for use in tests.
func testBootstrapPolicy() *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
}

// noopSyncStandbyManager is a consensus.SyncStandbyManager test double that does nothing.
type noopSyncStandbyManager struct{}

func (noopSyncStandbyManager) SetPolicy(_ context.Context, _ commonconsensus.PolicyWithCohort) error {
	return nil
}

func (noopSyncStandbyManager) Clear(_ context.Context) error {
	return nil
}

func (noopSyncStandbyManager) NeedsApply(_ context.Context, _ commonconsensus.PolicyWithCohort) (bool, error) {
	return false, nil
}

// testBootstrapID returns a pooler ID suitable for the initial row's
// coordinator_id in tests that call CreateRuleTables.
func testBootstrapID() *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "bootstrap-pooler",
	}
}

// failingSyncStandbyManager is a consensus.SyncStandbyManager test double whose SetPolicy
// returns the configured error for each call in sequence. Used to exercise
// UpdateRule's GUC failure paths (pre-promote / pre-write / post-write) without
// a real postgres sync configuration. Pass nil at index i to make the i-th call
// succeed; a shorter list means later calls succeed.
type failingSyncStandbyManager struct {
	setPolicyErrs  []error
	setPolicyCalls int
}

func (f *failingSyncStandbyManager) SetPolicy(_ context.Context, _ commonconsensus.PolicyWithCohort) error {
	i := f.setPolicyCalls
	f.setPolicyCalls++
	if i < len(f.setPolicyErrs) {
		return f.setPolicyErrs[i]
	}
	return nil
}

func (f *failingSyncStandbyManager) Clear(_ context.Context) error { return nil }

func (f *failingSyncStandbyManager) NeedsApply(_ context.Context, _ commonconsensus.PolicyWithCohort) (bool, error) {
	return false, nil
}

// expectReloadConfig sets up the mock query expectations for one successful call
// to MultiPoolerManager.reloadPostgresConfig: read pg_conf_load_time (pre), run
// pg_reload_conf, then read pg_conf_load_time (post) returning a different value
// so the wait loop exits on the first poll.
func expectReloadConfig(m *mock.QueryService) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:01+00"}}))
}

// expectReloadConfigFailure sets up the mock query expectations for a call to
// MultiPoolerManager.reloadPostgresConfig where pg_reload_conf itself fails.
// The wait loop is never entered.
func expectReloadConfigFailure(m *mock.QueryService, reloadErr error) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnceWithError("SELECT pg_reload_conf", reloadErr)
}
