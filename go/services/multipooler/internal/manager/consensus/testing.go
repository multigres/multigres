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
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// NewManagerForTesting builds a ConsensusManager over already-constructed
// components, for tests that inject fakes (a fake RuleStorer, an in-memory
// ConsensusPromises). Production code uses NewConsensusManager, which builds the
// real components from its dependencies. id is the pooler identity stamped into
// the ConsensusStatus this manager builds.
//
// It takes a testing.TB to mark it test-only — it must live in this package (not
// a _test.go file) because the manager package's tests, in a different package,
// construct a ConsensusManager with a fake RuleStorer, and only this package can
// set the unexported fields.
func NewManagerForTesting(t testing.TB, id *clustermetadatapb.ID, promises *ConsensusPromises, rules RuleStorer, broadcaster Broadcaster) *ConsensusManager {
	t.Helper()
	return newConsensusManager(id, promises, rules, broadcaster)
}
