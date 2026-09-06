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

package analysis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestRecruitable(t *testing.T) {
	now := time.Now()
	freshness := 30 * time.Second
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"}

	base := func() *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: id},
			LastSeen:    timestamppb.New(now),
			Status:      &multipoolermanagerdatapb.Status{IsInitialized: true},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Lsn: "0/1"},
			},
		}
	}

	t.Run("fresh, initialized, eligible, positioned, not blocked", func(t *testing.T) {
		p := store.NewPooler(base(), nil)
		assert.True(t, recruitable(p, now, freshness))
	})

	t.Run("stale observation", func(t *testing.T) {
		h := base()
		h.LastSeen = timestamppb.New(now.Add(-time.Hour))
		p := store.NewPooler(h, nil)
		assert.False(t, recruitable(p, now, freshness))
	})

	t.Run("not initialized", func(t *testing.T) {
		h := base()
		h.Status.IsInitialized = false
		p := store.NewPooler(h, nil)
		assert.False(t, recruitable(p, now, freshness))
	})

	t.Run("self-declared cohort-ineligible", func(t *testing.T) {
		h := base()
		h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		p := store.NewPooler(h, nil)
		assert.False(t, recruitable(p, now, freshness))
	})

	t.Run("no cached position", func(t *testing.T) {
		// Fresh and initialized, but its consensus status hasn't completed a
		// position read yet (e.g. mid-flap) — the scenario this check exists
		// to catch: a Recruit attempt against this pooler would be refused by
		// ValidateRevocation ("unknown WAL position"), so it must not count as
		// recruitable even though it looks otherwise healthy.
		h := base()
		h.ConsensusStatus.CurrentPosition = nil
		p := store.NewPooler(h, nil)
		assert.False(t, recruitable(p, now, freshness))
	})

	t.Run("outstanding RecruitBlockedUntil", func(t *testing.T) {
		h := base()
		h.ConsensusStatus.RecruitBlockedUntil = &clustermetadatapb.LsnPosition{Lsn: "0/5"}
		p := store.NewPooler(h, nil)
		assert.False(t, recruitable(p, now, freshness))
	})
}
