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

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func rn(term, subterm int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
}

func TestCompareRuleNumbers(t *testing.T) {
	tests := []struct {
		name string
		a, b *clustermetadatapb.RuleNumber
		want int
	}{
		{"equal", rn(5, 3), rn(5, 3), 0},
		{"higher term wins", rn(6, 0), rn(5, 99), 1},
		{"lower term loses", rn(4, 99), rn(5, 0), -1},
		{"same term higher subterm", rn(5, 4), rn(5, 3), 1},
		{"same term lower subterm", rn(5, 2), rn(5, 3), -1},
		{"nil treated as zero", nil, rn(0, 0), 0},
		{"nil less than non-zero", nil, rn(1, 0), -1},
		{"both nil", nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareRuleNumbers(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}
