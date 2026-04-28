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

package shardsetup

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// checkPoolerCondition evaluates condition for each pooler once. Returns nil if all
// poolers satisfy the condition, or a slice of failure lines (one per failing pooler)
// with the reason and FormatPoolerDiagnostics output.
func checkPoolerCondition(t *testing.T, poolers []*MultipoolerInstance, condition func(r PoolerStatusResult) (bool, string)) []string {
	t.Helper()
	statuses := fetchPoolerStatuses(t, poolers)
	var failures []string
	for _, r := range statuses {
		if r.Err != nil {
			failures = append(failures, fmt.Sprintf("%s: fetch error: %v", r.Name, r.Err))
			continue
		}
		met, reason := condition(r)
		if !met {
			failures = append(failures, fmt.Sprintf("%s: %s %s", r.Name, reason, FormatPoolerDiagnostics(r.Status, r.ConsensusStatus)))
		}
	}
	return failures
}

// PoolerStatusResult holds the fetched status for one pooler instance.
// Status is nil and Err is non-nil if the fetch failed.
type PoolerStatusResult struct {
	Name            string
	Status          *multipoolermanagerdatapb.Status
	ConsensusStatus *clustermetadatapb.ConsensusStatus
	Err             error
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
		results = append(results, PoolerStatusResult{Name: inst.Name, Status: resp.Status, ConsensusStatus: resp.ConsensusStatus})
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
					t.Logf("%s: %s", r.Name, FormatPoolerDiagnostics(r.Status, r.ConsensusStatus))
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
	condition func(r PoolerStatusResult) (bool, string),
	msgAndArgs ...any,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		failures := checkPoolerCondition(t, poolers, condition)
		for _, f := range failures {
			t.Log(f)
		}
		return len(failures) == 0
	}, timeout, tick, msgAndArgs...)
}

// RequirePoolerCondition fetches status for each pooler once and immediately fails the
// test if any pooler does not satisfy condition. Diagnostics for all failing poolers are
// included in the failure message. Use this after RequireRecovery or similar operations
// that guarantee the condition is already met — no polling needed.
func RequirePoolerCondition(
	t *testing.T,
	poolers []*MultipoolerInstance,
	condition func(r PoolerStatusResult) (bool, string),
	msgAndArgs ...any,
) {
	t.Helper()
	failures := checkPoolerCondition(t, poolers, condition)
	if len(failures) > 0 {
		require.Fail(t, strings.Join(failures, "\n"), msgAndArgs...)
	}
}

// FormatPoolerDiagnostics returns a compact diagnostic string for a pooler status,
// useful for appending to "not yet ready" log messages to aid flake investigation.
func FormatPoolerDiagnostics(s *multipoolermanagerdatapb.Status, cs *clustermetadatapb.ConsensusStatus) string {
	if s == nil {
		return "[status=nil]"
	}
	termNumber := cs.GetTermRevocation().GetRevokedBelowTerm()
	result := fmt.Sprintf("[postgres_ready=%v, initialized=%v, pooler_type=%v, term=%d",
		s.PostgresReady, s.IsInitialized, s.PoolerType, termNumber)
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
