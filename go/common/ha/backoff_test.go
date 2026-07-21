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

package ha

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func orch(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: name}
}

func decision(term int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term}
}

// revAt builds a revocation whose recruit intent targets replaceDecision at the
// given attempt, initiated at the given time.
func revAt(initiated time.Time, replaceDecision *clustermetadatapb.RuleNumber, attempt int64) *clustermetadatapb.TermRevocation {
	return &clustermetadatapb.TermRevocation{
		CoordinatorInitiatedAt: timestamppb.New(initiated),
		RecruitIntent: &clustermetadatapb.RecruitIntent{
			ReplaceDecision: replaceDecision,
			Attempt:         attempt,
		},
	}
}

func TestBackoffSchedule_ExponentialGrowthAndCap(t *testing.T) {
	s := BackoffSchedule{Base: 2 * time.Second, Cap: 20 * time.Second} // no jitter
	initiated := time.Unix(1_000_000, 0)

	cases := []struct {
		attempt int64
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 20 * time.Second},  // 32s clamped to cap
		{50, 20 * time.Second}, // large attempt cannot overflow
	}
	for _, c := range cases {
		got := s.NextAttempt(revAt(initiated, decision(4), c.attempt), orch("a")).Sub(initiated)
		assert.Equal(t, c.want, got, "attempt %d", c.attempt)
	}
}

func TestBackoffSchedule_Deterministic(t *testing.T) {
	s := DefaultBackoffSchedule()
	initiated := time.Unix(1_000_000, 0)
	rev := revAt(initiated, decision(4), 3)

	first := s.NextAttempt(rev, orch("a"))
	for range 5 {
		assert.Equal(t, first, s.NextAttempt(rev, orch("a")))
	}
}

func TestBackoffSchedule_JitterWithinWindow(t *testing.T) {
	window := 5 * time.Second
	s := BackoffSchedule{Base: time.Second, Cap: time.Minute, JitterWindow: window}
	initiated := time.Unix(1_000_000, 0)

	base := s.backoff(2)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		delay := s.NextAttempt(revAt(initiated, decision(4), 2), orch(name)).Sub(initiated)
		jitter := delay - base
		assert.GreaterOrEqual(t, jitter, time.Duration(0), "orch %s", name)
		assert.Less(t, jitter, window, "orch %s", name)
	}
}

func TestBackoffSchedule_DistinctOrchsGetDistinctSlots(t *testing.T) {
	s := BackoffSchedule{Base: time.Second, Cap: time.Minute, JitterWindow: 10 * time.Second}
	initiated := time.Unix(1_000_000, 0)
	rev := revAt(initiated, decision(4), 2)

	a := s.NextAttempt(rev, orch("orch-a"))
	b := s.NextAttempt(rev, orch("orch-b"))
	assert.NotEqual(t, a, b, "different orchestrators should generally get different retry slots")
}

func TestBackoffSchedule_AttemptReshufflesJitter(t *testing.T) {
	s := BackoffSchedule{Base: time.Second, Cap: time.Minute, JitterWindow: 10 * time.Second}
	j2 := s.jitter(orch("a"), decision(4), 2)
	j3 := s.jitter(orch("a"), decision(4), 3)
	assert.NotEqual(t, j2, j3)
}

func TestBackoffSchedule_ReplaceDecisionReshufflesJitter(t *testing.T) {
	// A different decision being replaced (a different failover episode)
	// reshuffles the jitter even at the same attempt, so the same orch is not
	// first every episode.
	s := BackoffSchedule{Base: time.Second, Cap: time.Minute, JitterWindow: 10 * time.Second}
	j4 := s.jitter(orch("a"), decision(4), 1)
	j5 := s.jitter(orch("a"), decision(5), 1)
	assert.NotEqual(t, j4, j5)
}

func TestBackoffSchedule_ZeroJitterWindow(t *testing.T) {
	s := BackoffSchedule{Base: 3 * time.Second, Cap: time.Minute} // JitterWindow zero
	assert.Equal(t, time.Duration(0), s.jitter(orch("a"), decision(4), 5))
}

func TestBackoffSchedule_MissingAttemptTreatedAsOne(t *testing.T) {
	s := BackoffSchedule{Base: 2 * time.Second, Cap: time.Minute}
	initiated := time.Unix(1_000_000, 0)

	got := s.NextAttempt(revAt(initiated, decision(4), 0), orch("a")).Sub(initiated)
	assert.Equal(t, 2*time.Second, got)
}
