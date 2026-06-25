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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

// testManagerConfig collects the consensus-related inputs a test wants to inject
// into a MultiPoolerManager. Defaults are filled in by newTestManager, so a test
// only sets what it cares about via the with* options.
type testManagerConfig struct {
	serviceID            *clustermetadatapb.ID
	record               *poolerRecord
	promises             *consensus.ConsensusPromises
	rules                consensus.RuleStorer
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

func withCohortEligibility(signal clustermetadatapb.CohortEligibilitySignal) testManagerOption {
	return func(c *testManagerConfig) { c.cohortEligibility = signal }
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

func (cfg *testManagerConfig) consensusManager() *consensus.ConsensusManager {
	return consensus.NewConsensusManager(cfg.promises, cfg.rules)
}

// newTestManager builds a MultiPoolerManager for unit tests of the consensus
// decision paths (remedial-action selection, stale-standby detection, etc.)
// without going through the full NewMultiPoolerManager bootstrap. It always
// installs a non-nil ConsensusManager: a fake rule store and an empty in-memory
// ConsensusPromises by default, each overridable via with* options.
func newTestManager(t *testing.T, opts ...testManagerOption) *MultiPoolerManager {
	t.Helper()
	cfg := resolveTestManagerConfig(t, opts...)
	return &MultiPoolerManager{
		logger:               slog.Default(),
		actionLock:           actionlock.NewActionLock(),
		serviceID:            cfg.serviceID,
		record:               cfg.record,
		cohortEligibility:    cfg.cohortEligibility,
		resignedLeaderAtTerm: cfg.resignedLeaderAtTerm,
		consensusMgr:         cfg.consensusManager(),
	}
}

// setTestRuleStore swaps the rule store of an already-constructed manager,
// preserving its promises. Used by tests that build a manager via the real
// NewMultiPoolerManager and then point the rule store at a mock query service.
func setTestRuleStore(pm *MultiPoolerManager, rules consensus.RuleStorer) {
	pm.consensusMgr = consensus.NewConsensusManager(pm.consensusMgr.Promises(), rules)
}

// setTestPromises swaps the durable-promise store of an already-constructed
// manager, preserving its rule store.
func setTestPromises(pm *MultiPoolerManager, promises *consensus.ConsensusPromises) {
	pm.consensusMgr = consensus.NewConsensusManager(promises, pm.consensusMgr.Rules())
}
