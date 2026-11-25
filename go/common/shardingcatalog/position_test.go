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

package shardingcatalog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestPositionCompare(t *testing.T) {
	at := func(term, version int64) Position {
		return Position{Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}, RoutingVersion: version}
	}
	assert.Equal(t, 0, at(2, 4).Compare(at(2, 4)))
	assert.Equal(t, -1, at(2, 3).Compare(at(2, 4)), "same rule: by routing version")
	assert.Equal(t, 1, at(2, 5).Compare(at(2, 4)))
	assert.Equal(t, -1, at(1, 9).Compare(at(2, 4)), "a lower rule is older, whatever its routing version")
	assert.Equal(t, 1, at(3, 1).Compare(at(2, 4)))
	assert.Equal(t, -1, Position{}.Compare(at(1, 0)), "the zero position is before any served one")
	assert.Equal(t, 0, Position{}.Compare(Position{}))
}
