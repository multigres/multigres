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

package shardsetup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// PoolerStatusResult holds the fetched status for one pooler instance.
// Status is nil and Err is non-nil if the fetch failed.
type PoolerStatusResult struct {
	Name   string
	Status *multipoolermanagerdatapb.Status
	Err    error
}

// fetchPoolerStatuses fetches the status of each pooler in order, returning a slice
// that preserves the input ordering (no map iteration randomness).
func fetchPoolerStatuses(t *testing.T, poolers []*MultipoolerInstance) []PoolerStatusResult {
	results := make([]PoolerStatusResult, 0, len(poolers))
	for _, inst := range poolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			results = append(results, PoolerStatusResult{Name: inst.Name, Err: fmt.Errorf("connect: %w", err)})
			continue
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		cancel()
		client.Close()
		if err != nil {
			results = append(results, PoolerStatusResult{Name: inst.Name, Err: fmt.Errorf("status RPC: %w", err)})
			continue
		}
		results = append(results, PoolerStatusResult{Name: inst.Name, Status: resp.Status})
	}
	return results
}

// EventuallyPoolersCondition is like require.Eventually but fetches all pooler statuses
// on each tick and passes them as a batch to condition. Useful when the condition spans
// multiple poolers (e.g. find which one became primary) or needs to return a value.
//
// On each failed tick, all pooler diagnostics are logged to aid flake investigation.
// Returns the typed value produced by condition once it returns met=true.
func EventuallyPoolersCondition[T any](
	t *testing.T,
	poolers []*MultipoolerInstance,
	timeout, tick time.Duration,
	condition func(statuses []PoolerStatusResult) (value T, met bool, reason string),
	msgAndArgs ...any,
) T {
	t.Helper()
	var result T
	require.Eventually(t, func() bool {
		statuses := fetchPoolerStatuses(t, poolers)
		val, met, reason := condition(statuses)
		if !met {
			for _, r := range statuses {
				if r.Err != nil {
					t.Logf("%s: fetch error: %v", r.Name, r.Err)
				} else {
					t.Logf("%s: %s", r.Name, FormatPoolerDiagnostics(r.Status))
				}
			}
			if reason != "" {
				t.Logf("condition not met: %s", reason)
			}
		} else {
			result = val
		}
		return met
	}, timeout, tick, msgAndArgs...)
	return result
}

// EventuallyPoolerCondition is like require.Eventually but automatically fetches status
// for each pooler on every tick and passes it to condition. When condition returns false
// for any pooler, it logs the reason alongside FormatPoolerDiagnostics to aid flake
// investigation. Returns only when all poolers satisfy the condition.
//
// The condition function returns (met bool, reason string). When met=false, reason is
// logged alongside full pooler diagnostics before the next tick.
func EventuallyPoolerCondition(
	t *testing.T,
	poolers []*MultipoolerInstance,
	timeout, tick time.Duration,
	condition func(name string, s *multipoolermanagerdatapb.Status) (bool, string),
	msgAndArgs ...any,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		statuses := fetchPoolerStatuses(t, poolers)
		allMet := true
		for _, r := range statuses {
			if r.Err != nil {
				t.Logf("%s: fetch error: %v", r.Name, r.Err)
				allMet = false
				continue
			}
			met, reason := condition(r.Name, r.Status)
			if !met {
				t.Logf("%s: %s %s", r.Name, reason, FormatPoolerDiagnostics(r.Status))
				allMet = false
			}
		}
		return allMet
	}, timeout, tick, msgAndArgs...)
}

// FormatPoolerDiagnostics returns a compact diagnostic string for a pooler status,
// useful for appending to "not yet ready" log messages to aid flake investigation.
func FormatPoolerDiagnostics(s *multipoolermanagerdatapb.Status) string {
	if s == nil {
		return "[status=nil]"
	}
	term := int64(0)
	if s.ConsensusTerm != nil {
		term = s.ConsensusTerm.TermNumber
	}
	result := fmt.Sprintf("[postgres_running=%v, initialized=%v, pooler_type=%v, term=%d",
		s.PostgresRunning, s.IsInitialized, s.PoolerType, term)
	if s.PostgresAction != multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		dur := time.Duration(0)
		if s.PostgresActionDuration != nil {
			dur = s.PostgresActionDuration.AsDuration().Round(time.Second)
		}
		result += fmt.Sprintf(", action=%v (%v)", s.PostgresAction, dur)
	}
	if s.ReplicationStatus != nil {
		walStatus := s.ReplicationStatus.WalReceiverStatus
		if walStatus == "" {
			walStatus = "none"
		}
		result += ", wal_receiver=" + walStatus
	}
	if s.PrimaryStatus != nil {
		var syncStandbys []string
		if s.PrimaryStatus.SyncReplicationConfig != nil {
			for _, id := range s.PrimaryStatus.SyncReplicationConfig.StandbyIds {
				syncStandbys = append(syncStandbys, id.Name)
			}
		}
		var connectedFollowers []string
		for _, id := range s.PrimaryStatus.ConnectedFollowers {
			connectedFollowers = append(connectedFollowers, id.Name)
		}
		result += fmt.Sprintf(", sync_standbys=%v, connected_followers=%v", syncStandbys, connectedFollowers)
	}
	result += "]"
	return result
}
