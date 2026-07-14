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

// Package consensustest provides helpers for tests that need to manipulate
// on-disk consensus state directly.
package consensustest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// SeedTerm writes the consensus promises file into poolerDir so a
// ConsensusPromises rooted at that directory will load the given revocation.
// It writes the file directly, bypassing any in-memory cache, so tests can
// prime persisted state before exercising Load() or an operation that reads
// it.
func SeedTerm(t testing.TB, poolerDir string, revocation *clustermetadatapb.TermRevocation) {
	t.Helper()
	data, err := protojson.Marshal(&clustermetadatapb.ConsensusPromises{TermRevocation: revocation})
	require.NoError(t, err)
	path := filepath.Join(poolerDir, constants.ConsensusPromisesFile)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}
