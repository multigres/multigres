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

// ConsensusManager owns the pooler's consensus state. It composes the two
// lower-level pieces in this package:
//
//   - promises: the durable term revocation (ConsensusPromises), persisted to
//     disk via pessimistic save-then-update.
//   - rules: the rule store (RuleStorer), which observes and writes the
//     current_rule row replicated through postgres WAL.
//
// The fields are unexported and reached through Promises()/Rules(); callers
// build a manager with NewConsensusManager. Tests construct it the same way
// (via the manager package's test builder), so neither the fields nor a setter
// need to be exported.
//
// Later steps fold the consensus state currently scattered across the manager
// (the recorded replication primary and leader-observed-at, leader resignation,
// cohort eligibility, suspected divergence, and the rewind backoff) into this
// type so consensus state lives in one place.
type ConsensusManager struct {
	promises *ConsensusPromises
	rules    RuleStorer
}

// NewConsensusManager builds a ConsensusManager over an already-constructed
// durable-promise store and rule store.
func NewConsensusManager(promises *ConsensusPromises, rules RuleStorer) *ConsensusManager {
	return &ConsensusManager{promises: promises, rules: rules}
}

// Promises returns the durable term-revocation store.
func (cm *ConsensusManager) Promises() *ConsensusPromises { return cm.promises }

// Rules returns the rule store.
func (cm *ConsensusManager) Rules() RuleStorer { return cm.rules }
