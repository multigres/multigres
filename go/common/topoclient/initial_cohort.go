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

package topoclient

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/types"
)

func initialCohortPath(shardKey types.ShardKey) string {
	return path.Join(DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard, InitialCohortFile)
}

// ClaimInitialCohort atomically records the initial cohort pooler IDs for a
// shard using a compare-and-swap write to the global topology.
//
// The first caller creates the record and gets back its proposed IDs.
// Any subsequent caller (including after a crash and retry) reads the
// already-committed record and gets back those IDs instead of its own proposal.
//
// Callers must use the returned committedIDs, not their proposedIDs, to
// decide which poolers to include in the initial cohort.
func (ts *store) ClaimInitialCohort(ctx context.Context, shardKey types.ShardKey, proposedIDs []string) ([]string, error) {
	sorted := make([]string, len(proposedIDs))
	copy(sorted, proposedIDs)
	sort.Strings(sorted)

	contents := []byte(strings.Join(sorted, "\n"))
	filePath := initialCohortPath(shardKey)

	_, err := ts.globalTopo.Create(ctx, filePath, contents)
	if err == nil {
		return sorted, nil
	}

	if !errors.Is(err, &TopoError{Code: NodeExists}) {
		return nil, mterrors.Wrapf(err, "failed to claim initial cohort for shard %s", shardKey)
	}

	data, _, err := ts.globalTopo.Get(ctx, filePath)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to read committed initial cohort for shard %s", shardKey)
	}

	var committed []string
	for line := range strings.SplitSeq(string(data), "\n") {
		if line != "" {
			committed = append(committed, line)
		}
	}
	return committed, nil
}
