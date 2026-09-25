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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// QuorumCommitStale reports whether a quorum-commit timestamp is older than
// staleAfter. A nil or epoch (Seconds == 0) timestamp means no evidence yet,
// so it's never stale -- GetSeconds() is nil-safe, so this covers both with
// one check.
//
// Shared by the analyzer and AppointLeaderAction's live recheck, so both use
// the same definition of "stale".
func QuorumCommitStale(quorumCommitTs *timestamppb.Timestamp, now time.Time, staleAfter time.Duration) bool {
	return quorumCommitTs.GetSeconds() != 0 && now.Sub(quorumCommitTs.AsTime()) > staleAfter
}

// DefaultQuorumCommitStaleAfter is the staleness threshold used by both the
// analyzer's AvailabilityPolicy and AppointLeaderAction's live recheck.
const DefaultQuorumCommitStaleAfter = 20 * time.Second
