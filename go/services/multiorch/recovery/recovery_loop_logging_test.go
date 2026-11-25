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

	mu      sync.Mutex
	records []loggedRecord
}

// loggedRecord is one log record, with its attributes resolved, so tests can
// compare whole records.
type loggedRecord struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

func (h *recordingHandler) Enabled(_ context.Context, level slog.Level) bool { return level >= h.level }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Resolve().Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, loggedRecord{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// drainRecords returns the records logged since the last drain.
func (h *recordingHandler) drainRecords() []loggedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.records
	h.records = nil
	return out
}

// drain returns the messages logged since the last drain.
func (h *recordingHandler) drain() []string {
	var messages []string
	for _, r := range h.drainRecords() {
		messages = append(messages, r.msg)
	}
	return messages
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
	engine, logs, detected := newPersistentProblemEngine(t, []persistentProblem{
		{"ShardAtRisk", types.ScopeShard, alertOnly},
	})
	ctx := t.Context()

	engine.performRecoveryCycle(ctx)
	assert.Equal(t, []string{"problem detected"}, logs.drain(), "reported once when it first appears")

	for i := range 3 {
		engine.performRecoveryCycle(ctx)
		assert.Empty(t, logs.drain(), "cycle %d found nothing new and must not log", i+2)
	}

	detected.Store(false)
	engine.performRecoveryCycle(ctx)
	assert.Equal(t, []string{"problem cleared"}, logs.drain(), "reported once when it clears")

	engine.performRecoveryCycle(ctx)
	assert.Empty(t, logs.drain(), "and nothing more once it is gone")
}

// TestRecoveryLoop_LogsHeldBackFollowerFixEveryCycle documents a known exception to
// the quiet rule above: a follower fix held back by an unhealthy leader is logged
// every cycle, until planning reports which actions are waiting (see the TODO in
// processShardProblems).
func TestRecoveryLoop_LogsHeldBackFollowerFixEveryCycle(t *testing.T) {
	alertOnly := &mockRecoveryAction{
		metadata: types.RecoveryMetadata{Name: actions.AlertOnlyActionName, Timeout: 5 * time.Second},
	}
	needsHealthyLeader := &mockRecoveryAction{name: "NeedsHealthyLeader", timeout: 5 * time.Second, requiresHealthyLeader: true}
	const skipping = "skipping recovery - requires healthy leader but leader is unhealthy"
	engine, logs, detected := newPersistentProblemEngine(t, []persistentProblem{
		{"ShardStuck", types.ScopeShard, alertOnly},
		{"ReplicaNotReplicating", types.ScopePooler, needsHealthyLeader},
	})
	ctx := t.Context()

	engine.performRecoveryCycle(ctx)
	assert.Equal(t, []string{"problem detected", "problem detected", skipping}, logs.drain())

	for i := range 3 {
		engine.performRecoveryCycle(ctx)
		assert.Equal(t, []string{skipping}, logs.drain(), "cycle %d logs only the held-back fix", i+2)
	}

	detected.Store(false)
	engine.performRecoveryCycle(ctx)
	assert.Equal(t, []string{"problem cleared", "problem cleared"}, logs.drain())
}

// newPersistentProblemEngine returns an engine whose analyzers report problems on
// one replica every cycle until detected is set to false, and the handler that
// records what it logs at Info and above.
func newPersistentProblemEngine(t *testing.T, problems []persistentProblem) (engine *Engine, logs *recordingHandler, detected *atomic.Bool) {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	require.NoError(t, ts.CreateDatabase(ctx, "db1", &clustermetadatapb.Database{
		Name:                      "db1",
		BootstrapDurabilityPolicy: topoclient.AtLeastN(2),
	}))
	logs = &recordingHandler{level: slog.LevelInfo}
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	fakeClient := &rpcclient.FakeClient{}
	engine = NewEngine(ts, slog.New(logs), cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-pooler"}
	shardKey := &clustermetadatapb.ShardRef{Database: "db1", TableGroup: "tg1", Shard: "0"}

	detected = &atomic.Bool{}
	detected.Store(true)
	var analyzers []analysis.Analyzer
	for _, spec := range problems {
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
	return engine, logs, detected
}
