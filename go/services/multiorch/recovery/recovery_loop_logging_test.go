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

package recovery

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// recordingHandler keeps every record at or above its level, so a test can assert
// on what the recovery loop logs at the level production runs with.
type recordingHandler struct {
	level slog.Level

	mu       sync.Mutex
	messages []string
}

func (h *recordingHandler) Enabled(_ context.Context, level slog.Level) bool { return level >= h.level }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// drain returns what was logged since the last drain.
func (h *recordingHandler) drain() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.messages
	h.messages = nil
	return out
}

// persistentProblem describes a problem that an analyzer reports every cycle
// while the test keeps it switched on.
type persistentProblem struct {
	code   types.ProblemCode
	scope  types.ProblemScope
	action types.RecoveryAction
}

// TestRecoveryLoop_StaysQuietWhileAProblemPersists is a log-volume guard: a cycle
// that finds the same problems as the last one, and has nothing to do about them,
// must not log at the default level. State changes are logged once, when they
// happen.
func TestRecoveryLoop_StaysQuietWhileAProblemPersists(t *testing.T) {
	alertOnly := &mockRecoveryAction{
		metadata: types.RecoveryMetadata{Name: actions.AlertOnlyActionName, Timeout: 5 * time.Second},
	}
	needsHealthyLeader := &mockRecoveryAction{name: "NeedsHealthyLeader", timeout: 5 * time.Second, requiresHealthyLeader: true}

	for _, tc := range []struct {
		name     string
		problems []persistentProblem
	}{
		{
			name:     "alert-only problem",
			problems: []persistentProblem{{"ShardAtRisk", types.ScopeShard, alertOnly}},
		},
		{
			// The follower's recovery is held back for as long as the shard has a
			// problem with its leader.
			name: "follower problem held back by an unhealthy leader",
			problems: []persistentProblem{
				{"ShardStuck", types.ScopeShard, alertOnly},
				{"ReplicaNotReplicating", types.ScopePooler, needsHealthyLeader},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
			require.NoError(t, ts.CreateDatabase(ctx, "db1", &clustermetadatapb.Database{
				Name:                      "db1",
				BootstrapDurabilityPolicy: topoclient.AtLeastN(2),
			}))
			logs := &recordingHandler{level: slog.LevelInfo}
			cfg := config.NewTestConfig(config.WithCell("cell1"))
			fakeClient := &rpcclient.FakeClient{}
			engine := NewEngine(ts, slog.New(logs), cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

			poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-pooler"}
			shardKey := &clustermetadatapb.ShardRef{Database: "db1", TableGroup: "tg1", Shard: "0"}

			var detected atomic.Bool
			detected.Store(true)
			var analyzers []analysis.Analyzer
			for _, spec := range tc.problems {
				analyzers = append(analyzers, &customAnalyzer{
					name:           "Persistent" + string(spec.code),
					recoveryAction: spec.action,
					analyzeFn: func(*multiorchdatapb.PoolerHealthState) *types.Problem {
						if !detected.Load() {
							return nil
						}
						return &types.Problem{
							Code:           spec.code,
							CheckName:      types.CheckName("Persistent" + string(spec.code)),
							PoolerID:       poolerID,
							ShardRef:       shardKey,
							Priority:       types.PriorityNormal,
							Scope:          spec.scope,
							RecoveryAction: spec.action,
							DetectedAt:     time.Now(),
							Description:    "a problem that stays",
						}
					},
				})
			}
			analysis.SetTestAnalyzers(analyzers)
			t.Cleanup(analysis.ResetAnalyzers)

			store.SeedCache(t, engine.poolerCache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{
					Id:       poolerID,
					ShardRef: shardKey,
					Type:     clustermetadatapb.PoolerType_REPLICA,
					Hostname: "replica-host",
				},
				LastSeen: timestamppb.Now(),
				Status:   &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_REPLICA},
			}, nil))

			engine.performRecoveryCycle(ctx)
			first := logs.drain()
			assert.Len(t, first, len(tc.problems), "each problem is reported once when it first appears: %v", first)
			assert.Subset(t, []string{"problem detected"}, first)

			for i := range 3 {
				engine.performRecoveryCycle(ctx)
				assert.Empty(t, logs.drain(), "cycle %d found nothing new and must not log", i+2)
			}

			detected.Store(false)
			engine.performRecoveryCycle(ctx)
			cleared := logs.drain()
			assert.Len(t, cleared, len(tc.problems), "each clear is reported once: %v", cleared)
			assert.Subset(t, []string{"problem cleared"}, cleared)

			engine.performRecoveryCycle(ctx)
			assert.Empty(t, logs.drain(), "and nothing more once they are gone")
		})
	}
}
