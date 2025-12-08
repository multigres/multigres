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

package analysis

import (
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
)

// Analyzer analyzes ReplicationAnalysis and detects problems.
type Analyzer interface {
	// Name returns the unique name of this analyzer.
	Name() types.CheckName

	// Analyze examines the ReplicationAnalysis and returns any detected problems.
	// Returns an error if the analyzer cannot perform its analysis (e.g., missing dependencies).
	Analyze(analysis *store.ReplicationAnalysis) ([]types.Problem, error)
}

// defaultAnalyzers holds the global list of analyzers.
// Can be overridden for testing via SetTestAnalyzers.
var defaultAnalyzers []Analyzer

// DefaultAnalyzers returns the current set of analyzers to run.
// The factory is injected into each analyzer for creating recovery actions.
func DefaultAnalyzers(factory *RecoveryActionFactory) []Analyzer {
	if defaultAnalyzers == nil {
		return []Analyzer{
			&ShardNeedsBootstrapAnalyzer{factory: factory},
			&PrimaryIsDeadAnalyzer{factory: factory},
			&ReplicaNotReplicatingAnalyzer{factory: factory},
		}
	}
	return defaultAnalyzers
}

// SetTestAnalyzers overrides the default analyzers for testing.
// This should only be called from tests.
func SetTestAnalyzers(analyzers []Analyzer) {
	defaultAnalyzers = analyzers
}

// ResetAnalyzers resets the analyzers to the default (empty) state.
// This should be called in test cleanup.
func ResetAnalyzers() {
	defaultAnalyzers = nil
}
