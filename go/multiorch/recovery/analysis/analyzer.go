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
	"github.com/multigres/multigres/go/multiorch/store"
)

// Analyzer analyzes ReplicationAnalysis and detects problems.
type Analyzer interface {
	// Name returns the unique name of this analyzer.
	Name() CheckName

	// Analyze examines the ReplicationAnalysis and returns any detected problems.
	Analyze(analysis *store.ReplicationAnalysis) []Problem
}

// defaultAnalyzers holds the global list of analyzers.
// Can be overridden for testing via SetTestAnalyzers.
var defaultAnalyzers []Analyzer

// DefaultAnalyzers returns the current set of analyzers to run.
func DefaultAnalyzers() []Analyzer {
	if defaultAnalyzers == nil {
		return []Analyzer{
			&ShardNeedsBootstrapAnalyzer{},
			&ShardHasNoPrimaryAnalyzer{},
			&PrimaryIsDeadAnalyzer{},
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

// globalFactory holds the global RecoveryActionFactory instance.
// This is set during engine initialization and used by analyzers.
var globalFactory *RecoveryActionFactory

// SetRecoveryActionFactory sets the global recovery action factory.
// This should be called during engine initialization.
func SetRecoveryActionFactory(factory *RecoveryActionFactory) {
	globalFactory = factory
}

// GetRecoveryActionFactory returns the global recovery action factory.
// Analyzers use this to create recovery actions.
func GetRecoveryActionFactory() *RecoveryActionFactory {
	return globalFactory
}
