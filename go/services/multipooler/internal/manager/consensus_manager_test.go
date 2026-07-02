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

package manager

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// forTestOption configures NewMultiPoolerManagerForTesting.
type forTestOption func(*forTestConfig)

type forTestConfig struct {
	qsc   poolerserver.PoolerController
	rules consensus.RuleStorer
}

// withMockController injects a query-pooler controller (a mock query service)
// instead of the one the constructor would build. The consensus rule store is
// built over its InternalQueryService(), so this also points the rule store at
// the mock.
func withMockController(qsc poolerserver.PoolerController) forTestOption {
	return func(c *forTestConfig) { c.qsc = qsc }
}

// withFakeRules injects a fake rule store: the manager is built through the real
// constructor but its ConsensusManager uses this RuleStorer instead of a real
// store over postgres.
func withFakeRules(rules consensus.RuleStorer) forTestOption {
	return func(c *forTestConfig) { c.rules = rules }
}

// NewMultiPoolerManagerForTesting builds a MultiPoolerManager through the real
// constructor path (topology record, health streamer, lifecycle, etc.) while
// letting a test inject a mock query-pooler controller and/or a fake rule store.
// It replaces the old build-then-swap pattern (a manual pm.qsc assignment plus
// setTestRuleStore/setTestPromises).
func NewMultiPoolerManagerForTesting(t *testing.T, logger *slog.Logger, mp *clustermetadatapb.MultiPooler, config *Config, opts ...forTestOption) (*MultiPoolerManager, error) {
	t.Helper()
	var c forTestConfig
	for _, o := range opts {
		o(&c)
	}
	ov := overrides{qsc: c.qsc}
	if c.rules != nil {
		// Build the ConsensusManager with the fake rule store. Promises is rooted
		// at the pooler dir; no broadcaster (these tests don't assert broadcasts).
		promises := consensus.NewConsensusPromises(mp.GetPoolerDir(), mp.GetId())
		ov.consensusMgr = consensus.NewManagerForTesting(t, mp.GetId(), promises, c.rules, nil)
	}
	return newMultiPoolerManager(logger, mp, config, 5*time.Minute, ov)
}

// testManagerConfig collects the consensus-related inputs a test wants to inject
// into a MultiPoolerManager. Defaults are filled in by newTestManager, so a test
// only sets what it cares about via the with* options.
type testManagerConfig struct {
	serviceID            *clustermetadatapb.ID
	record               *poolerRecord
	promises             *consensus.ConsensusPromises
	rules                consensus.RuleStorer
	replicationPrimary   *clustermetadatapb.ReplicationPrimary
	cohortEligibility    clustermetadatapb.CohortEligibilitySignal
	resignedLeaderAtTerm int64
}

type testManagerOption func(*testManagerConfig)

func withServiceID(id *clustermetadatapb.ID) testManagerOption {
	return func(c *testManagerConfig) { c.serviceID = id }
}

func withRecord(record *poolerRecord) testManagerOption {
	return func(c *testManagerConfig) { c.record = record }
}

// withPromises injects an already-constructed ConsensusPromises (e.g. one that a
// test has seeded via consensustest.SeedTerm or RecordTermPrimary).
func withPromises(promises *consensus.ConsensusPromises) testManagerOption {
	return func(c *testManagerConfig) { c.promises = promises }
}

func withRuleStore(rules consensus.RuleStorer) testManagerOption {
	return func(c *testManagerConfig) { c.rules = rules }
}

// withReplicationPrimary records a ReplicationPrimary on the manager at
// construction (via RecordTermPrimary), seeding the recorded primary/leader the
// consensus decision paths read.
func withReplicationPrimary(rp *clustermetadatapb.ReplicationPrimary) testManagerOption {
	return func(c *testManagerConfig) { c.replicationPrimary = rp }
}

func withResignedLeaderAtTerm(term int64) testManagerOption {
	return func(c *testManagerConfig) { c.resignedLeaderAtTerm = term }
}

// resolveTestManagerConfig applies the options and fills in defaults: a fake
// rule store and an empty in-memory ConsensusPromises (rooted at a temp dir).
// Shared by the test constructors so they install a non-nil ConsensusManager.
func resolveTestManagerConfig(t *testing.T, opts ...testManagerOption) *testManagerConfig {
	t.Helper()
	cfg := &testManagerConfig{
		cohortEligibility: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.promises == nil {
		cfg.promises = consensus.NewConsensusPromises(t.TempDir(), cfg.serviceID)
	}
	if cfg.rules == nil {
		cfg.rules = &fakeRuleStore{}
	}
	return cfg
}

// consensusManager builds the ConsensusManager. A nil broadcaster means health
// broadcasts are skipped in tests. The recorded primary, resignation, and
// eligibility are seeded separately under the action lock by seedLockedState,
// since those setters assert the action lock.
func (cfg *testManagerConfig) consensusManager(t *testing.T) *consensus.ConsensusManager {
	return consensus.NewManagerForTesting(t, cfg.serviceID, cfg.promises, cfg.rules, nil)
}

// seedLockedState applies the replication-primary / resignation / eligibility
// overrides through the action-lock-asserting setters, briefly acquiring the
// manager's action lock. No-op when all are at their defaults.
func (cfg *testManagerConfig) seedLockedState(t *testing.T, pm *MultiPoolerManager) {
	t.Helper()
	eligibleDefault := clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE
	if cfg.replicationPrimary == nil && cfg.resignedLeaderAtTerm == 0 && cfg.cohortEligibility == eligibleDefault {
		return
	}
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	if cfg.replicationPrimary != nil {
		require.NoError(t, pm.consensusMgr.RecordTermPrimary(lockCtx, cfg.replicationPrimary))
	}
	if cfg.resignedLeaderAtTerm != 0 {
		require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, cfg.resignedLeaderAtTerm))
	}
	if cfg.cohortEligibility != eligibleDefault {
		require.NoError(t, pm.consensusMgr.SetCohortEligibility(lockCtx, cfg.cohortEligibility))
	}
}

// newTestManager builds a MultiPoolerManager for unit tests of the consensus
// decision paths (remedial-action selection, stale-standby detection, etc.)
// without going through the full NewMultiPoolerManager bootstrap. It always
// installs a non-nil ConsensusManager: a fake rule store and an empty in-memory
// ConsensusPromises by default, each overridable via with* options.
func newTestManager(t *testing.T, opts ...testManagerOption) *MultiPoolerManager {
	t.Helper()
	cfg := resolveTestManagerConfig(t, opts...)
	if cfg.record == nil {
		// A non-nil record is required to wire the StateManager (below); default to
		// a REPLICA/DISABLED record for tests that don't supply their own.
		cfg.record = newRecordFromProto(&clustermetadatapb.MultiPooler{
			Id:            cfg.serviceID,
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		})
	}
	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
		serviceID:    cfg.serviceID,
		record:       cfg.record,
		consensusMgr: cfg.consensusManager(t),
	}
	// The remedial-action decision path (determineRoleAction) consults
	// StateManager.hasDrift, so a non-nil StateManager wired to the same record and
	// live consensus snapshot the manager uses is required.
	pm.stateManager = NewStateManager(pm.logger, pm.record, pm.consensusMgr.CachedConsensusStatus)
	cfg.seedLockedState(t, pm)
	// Prime the StateManager's last-fanned-out state to reflect what components
	// last saw: the seeded record's own label and serving status, with the physical
	// recovery flag assumed aligned with that label (PRIMARY <-> primary). This is
	// the faithful port of the old determineRemedialAction lastAppliedPrimary input
	// (which the tables passed as state.isPrimary): a freshly-built manager is NOT
	// drifted relative to its seed, so drift is registered only when the observed
	// postgresState / consensus diverges from the seeded label.
	pm.stateManager.pgMode = pgmode.InRecovery
	if pm.record.Type() == clustermetadatapb.PoolerType_PRIMARY {
		pm.stateManager.pgMode = pgmode.Primary
	}
	primaryBaseline := pm.stateManager.pgMode.OutOfRecovery()
	baseline := servingstate.State{
		Routing:       servingstate.RoutingState{Role: servingstate.RoutingRoleReplica},
		ServingStatus: pm.record.ServingStatus(),
	}
	if primaryBaseline {
		baseline.Routing.Role = servingstate.RoutingRolePrimary
	}
	pm.stateManager.lastFannedOut = &baseline
	return pm
}
