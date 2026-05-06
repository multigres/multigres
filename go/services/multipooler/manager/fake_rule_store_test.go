// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeRuleStore is a test double for ruleStorer that returns a preset position
// without hitting postgres. Both observePosition and updateRule return pos
// (or observeErr/updateErr when set). updateRule records all calls in updates.
//
// If posSequence is non-empty, observePosition returns positions from the
// sequence in order (consuming each entry), then falls back to pos once
// the sequence is exhausted. This is useful for simulating a position that
// changes between calls (e.g., Recruit's sanity check vs. post-stop check).
type fakeRuleStore struct {
	mu          sync.Mutex
	pos         *clustermetadatapb.PoolerPosition
	posSequence []*clustermetadatapb.PoolerPosition
	observeErr  error
	updateErr   error
	updates     []*ruleUpdateBuilder
}

func (f *fakeRuleStore) observePosition(_ context.Context) (*clustermetadatapb.PoolerPosition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.posSequence) > 0 {
		pos := f.posSequence[0]
		f.posSequence = f.posSequence[1:]
		return pos, f.observeErr
	}
	return f.pos, f.observeErr
}

func (f *fakeRuleStore) createRuleTables(_ context.Context) error {
	return nil
}

func (f *fakeRuleStore) cachedPosition() *clustermetadatapb.PoolerPosition {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos
}

func (f *fakeRuleStore) updateRule(_ context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, update)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return f.pos, nil
}

// assertPromoteRecorded asserts that exactly one updateRule call was made with
// eventType "promotion" and returns the update so callers can inspect its fields.
func (f *fakeRuleStore) assertPromoteRecorded(t *testing.T) *ruleUpdateBuilder {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.updates, 1, "expected exactly one updateRule call for promotion")
	assert.Equal(t, "promotion", f.updates[0].eventType)
	return f.updates[0]
}
