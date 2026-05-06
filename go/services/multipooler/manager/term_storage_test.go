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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func newTestConsensusState(t *testing.T) (*ConsensusState, string) {
	t.Helper()
	poolerDir := t.TempDir()
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	return NewConsensusState(poolerDir, id), poolerDir
}

func TestDeleteTermFile_FileExists(t *testing.T) {
	cs, poolerDir := newTestConsensusState(t)

	// Write a term file with a non-zero term
	term := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 42}
	cs.mu.Lock()
	cs.revocation = term
	require.NoError(t, cs.setRevocation(term))
	cs.mu.Unlock()

	termPath := filepath.Join(poolerDir, constants.ConsensusTermFile)
	_, err := os.Stat(termPath)
	require.NoError(t, err, "term file should exist before deletion")

	require.NoError(t, cs.DeleteTermFile())

	// File must be gone
	_, err = os.Stat(termPath)
	assert.True(t, os.IsNotExist(err), "term file should be deleted")

	// In-memory term must be reset to 0
	n, err := cs.GetInconsistentCurrentTermNumber()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestDeleteTermFile_FileDoesNotExist(t *testing.T) {
	cs, poolerDir := newTestConsensusState(t)

	// Prime in-memory state to a non-zero term without writing to disk
	cs.mu.Lock()
	cs.revocation = &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7}
	cs.mu.Unlock()

	termPath := filepath.Join(poolerDir, constants.ConsensusTermFile)
	_, err := os.Stat(termPath)
	require.True(t, os.IsNotExist(err), "term file should not exist at start")

	// Must succeed (idempotent)
	require.NoError(t, cs.DeleteTermFile())

	// In-memory term must still be reset to 0
	n, err := cs.GetInconsistentCurrentTermNumber()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}
