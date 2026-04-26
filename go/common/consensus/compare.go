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

// Package consensus provides utilities for working with consensus types.
package consensus

import (
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// CompareRuleNumbers compares two RuleNumbers lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// A nil RuleNumber is treated as zero (the smallest possible value).
func CompareRuleNumbers(a, b *clustermetadatapb.RuleNumber) int {
	aTerm := int64(0)
	aSubterm := int64(0)
	if a != nil {
		aTerm = a.GetCoordinatorTerm()
		aSubterm = a.GetLeaderSubterm()
	}

	bTerm := int64(0)
	bSubterm := int64(0)
	if b != nil {
		bTerm = b.GetCoordinatorTerm()
		bSubterm = b.GetLeaderSubterm()
	}

	if aTerm != bTerm {
		if aTerm < bTerm {
			return -1
		}
		return 1
	}
	if aSubterm != bSubterm {
		if aSubterm < bSubterm {
			return -1
		}
		return 1
	}
	return 0
}

// comparePosition returns negative, zero, or positive based on whether a is
// behind, equal to, or ahead of b. Rule number takes precedence; LSN breaks
// ties within the same rule. A missing or unparseable LSN is treated as less
// than any valid LSN.
func comparePosition(a, b *clustermetadatapb.PoolerPosition) int {
	if cmp := CompareRuleNumbers(a.GetRule().GetRuleNumber(), b.GetRule().GetRuleNumber()); cmp != 0 {
		return cmp
	}
	lsnA, errA := pgutil.ParseLSN(a.GetLsn())
	lsnB, errB := pgutil.ParseLSN(b.GetLsn())
	okA := errA == nil
	okB := errB == nil
	switch {
	case !okA && !okB:
		return 0
	case !okA:
		return -1
	case !okB:
		return 1
	case lsnA < lsnB:
		return -1
	case lsnA > lsnB:
		return 1
	default:
		return 0
	}
}
