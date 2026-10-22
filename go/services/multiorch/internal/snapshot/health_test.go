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

package snapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestLeaderWritesProgressing(t *testing.T) {
	now := time.Now()
	freshness := 30 * time.Second
	decided := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5, LeaderSubterm: 2},
	}}
	undecided := &clustermetadatapb.RulePosition{
		Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5, LeaderSubterm: 2}},
		Proposal: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6, LeaderSubterm: 0}},
	}

	leader := func(lastSeen time.Time, status multipoolermanagerdatapb.PostgresStatus) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{},
			LastSeen:    timestamppb.New(lastSeen),
			Status:      &multipoolermanagerdatapb.Status{PostgresStatus: status},
		}
	}

	t.Run("nil leader is never progressing", func(t *testing.T) {
		require.False(t, LeaderWritesProgressing(nil, decided, now, freshness))
	})

	t.Run("fresh primary with a decided rule is progressing", func(t *testing.T) {
		l := leader(now, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY)
		require.True(t, LeaderWritesProgressing(l, decided, now, freshness))
	})

	t.Run("stale observation is not progressing", func(t *testing.T) {
		l := leader(now.Add(-time.Minute), multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY)
		require.False(t, LeaderWritesProgressing(l, decided, now, freshness))
	})

	t.Run("never observed is not progressing", func(t *testing.T) {
		l := &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{},
			Status:      &multipoolermanagerdatapb.Status{PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY},
		}
		require.False(t, LeaderWritesProgressing(l, decided, now, freshness))
	})

	t.Run("standby is not progressing even if fresh", func(t *testing.T) {
		l := leader(now, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY)
		require.False(t, LeaderWritesProgressing(l, decided, now, freshness))
	})

	t.Run("undecided highest position is not progressing", func(t *testing.T) {
		l := leader(now, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY)
		require.False(t, LeaderWritesProgressing(l, undecided, now, freshness))
	})
}
