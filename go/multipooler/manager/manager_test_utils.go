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
)

// newTestManager creates a minimally initialized MultiPoolerManager for unit tests.
// It initializes essential fields (logger, actionLock, recoveryActionState, consensusState)
// without heavy infrastructure (gRPC clients, topology, connection pools).
// Use functional options to customize fields for specific test needs.
func newTestManager(t *testing.T, opts ...func(*MultiPoolerManager)) *MultiPoolerManager {
	t.Helper()

	// Create a minimal service ID
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "test-cell",
		Name:      "test-manager",
	}

	// Create a temporary directory for state files
	tmpDir := t.TempDir()

	// Create manager with essential fields initialized
	pm := &MultiPoolerManager{
		logger:              slog.Default(),
		actionLock:          NewActionLock(),
		serviceID:           serviceID,
		recoveryActionState: NewRecoveryActionState(tmpDir, serviceID),
		consensusState:      NewConsensusState(tmpDir, serviceID),
		multipooler: &clustermetadatapb.MultiPooler{
			PoolerDir: tmpDir,
			Id:        serviceID,
		},
	}

	// Apply optional modifications
	for _, opt := range opts {
		opt(pm)
	}

	return pm
}
