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

package utils

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// NewTestClaim builds a CoordinatorClaim for e2e tests that drive a term-carrying
// RPC directly (without a real coordinator). Uses a fixed coordinator name and
// the current wall clock, which is sufficient for tests that only need the
// claim to be well-formed at a given term.
func NewTestClaim(term int64) *consensusdatapb.CoordinatorClaim {
	return &consensusdatapb.CoordinatorClaim{
		Term:                   term,
		CoordinatorId:          &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "test-coordinator"},
		CoordinatorInitiatedAt: timestamppb.Now(),
	}
}
