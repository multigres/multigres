// Copyright 2025 Supabase, Inc.
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

package actions

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestBootstrapExecuteEmptyCohort(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, logger)

	cohort := []*store.PoolerHealth{}
	err := action.Execute(ctx, "shard-01", "postgres", cohort)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cohort is empty")
}

func TestBootstrapParsePolicyANY_2(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, logger)

	rule, err := action.parsePolicy("ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes must acknowledge", rule.Description)
}

func TestBootstrapParsePolicyMULTI_CELL_ANY_2(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, logger)

	rule, err := action.parsePolicy("MULTI_CELL_ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes from different cells must acknowledge", rule.Description)
}

func TestBootstrapParsePolicyInvalid(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, logger)

	rule, err := action.parsePolicy("INVALID_POLICY")
	assert.Error(t, err)
	assert.Nil(t, rule)
	assert.Contains(t, err.Error(), "unsupported policy name")
}
