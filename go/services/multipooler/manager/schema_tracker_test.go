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

package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
)

// testLogger returns a silent logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// makeSchemaResult builds a sqltypes.Result matching schemaTrackingQuery output.
func makeSchemaResult(tables [][]any) *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{"schema_name", "table_name", "oid", "definition"},
		tables,
	)
}

// ----------------------------------------------------------------------------
// parseSchemaSnapshot
// ----------------------------------------------------------------------------

func TestParseSchemaSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		result      *sqltypes.Result
		wantLen     int
		wantErr     bool
		errContains string
	}{
		{
			name:    "empty result",
			result:  makeSchemaResult(nil),
			wantLen: 0,
		},
		{
			name: "single table",
			result: makeSchemaResult([][]any{
				{"public", "users", "16384", "id:23,name:25"},
			}),
			wantLen: 1,
		},
		{
			name: "multiple tables",
			result: makeSchemaResult([][]any{
				{"public", "users", "16384", "id:23,name:25"},
				{"public", "posts", "16385", "id:23,user_id:23,title:25"},
				{"app", "config", "16386", "key:25,value:25"},
			}),
			wantLen: 3,
		},
		{
			name: "invalid OID",
			result: makeSchemaResult([][]any{
				{"public", "users", "not_a_number", "id:23"},
			}),
			wantErr:     true,
			errContains: "parsing OID",
		},
		{
			name: "row with fewer than 4 columns is skipped",
			result: &sqltypes.Result{
				Rows: []*sqltypes.Row{
					{Values: []sqltypes.Value{[]byte("public"), []byte("users")}},
				},
			},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot, err := parseSchemaSnapshot(tt.result)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			assert.Len(t, snapshot, tt.wantLen)
		})
	}
}

func TestParseSchemaSnapshot_FieldValues(t *testing.T) {
	result := makeSchemaResult([][]any{
		{"myschema", "mytable", "12345", "col1:23,col2:25"},
	})
	snapshot, err := parseSchemaSnapshot(result)
	require.NoError(t, err)

	sig, ok := snapshot[12345]
	require.True(t, ok)
	assert.Equal(t, "myschema.mytable", sig.qualifiedName)
	assert.Equal(t, "col1:23,col2:25", sig.definition)
}

// ----------------------------------------------------------------------------
// poll
// ----------------------------------------------------------------------------

func TestPoll(t *testing.T) {
	ctx := context.Background()
	logger := testLogger()

	baselineResult := makeSchemaResult([][]any{
		{"public", "users", "16384", "id:23,name:25"},
		{"public", "posts", "16385", "id:23,title:25"},
	})

	t.Run("first poll establishes baseline without reporting change", func(t *testing.T) {
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnce("SELECT", baselineResult)

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		changed, snapshot, err := st.poll(ctx, nil)
		require.NoError(t, err)
		assert.False(t, changed)
		assert.Len(t, snapshot, 2)
	})

	t.Run("no change on identical snapshot", func(t *testing.T) {
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnce("SELECT", baselineResult)

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		// Build the baseline snapshot from the same data.
		baseline, _ := parseSchemaSnapshot(baselineResult)

		changed, _, err := st.poll(ctx, baseline)
		require.NoError(t, err)
		assert.False(t, changed)
	})

	t.Run("detects table creation", func(t *testing.T) {
		newResult := makeSchemaResult([][]any{
			{"public", "users", "16384", "id:23,name:25"},
			{"public", "posts", "16385", "id:23,title:25"},
			{"public", "comments", "16386", "id:23,body:25"},
		})
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnce("SELECT", newResult)

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		baseline, _ := parseSchemaSnapshot(baselineResult)
		changed, snapshot, err := st.poll(ctx, baseline)
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Len(t, snapshot, 3)
	})

	t.Run("detects table drop", func(t *testing.T) {
		droppedResult := makeSchemaResult([][]any{
			{"public", "users", "16384", "id:23,name:25"},
		})
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnce("SELECT", droppedResult)

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		baseline, _ := parseSchemaSnapshot(baselineResult)
		changed, snapshot, err := st.poll(ctx, baseline)
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Len(t, snapshot, 1)
	})

	t.Run("detects column alteration", func(t *testing.T) {
		alteredResult := makeSchemaResult([][]any{
			{"public", "users", "16384", "id:23,name:25,email:25"},
			{"public", "posts", "16385", "id:23,title:25"},
		})
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnce("SELECT", alteredResult)

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		baseline, _ := parseSchemaSnapshot(baselineResult)
		changed, _, err := st.poll(ctx, baseline)
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("query error preserves existing snapshot", func(t *testing.T) {
		mockQS := mock.NewQueryService()
		mockQS.AddQueryPatternOnceWithError("SELECT", errors.New("connection refused"))

		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return mockQS },
		}

		baseline, _ := parseSchemaSnapshot(baselineResult)
		changed, snapshot, err := st.poll(ctx, baseline)
		require.Error(t, err)
		assert.False(t, changed)
		// Snapshot returned is the old one so the caller can retry.
		assert.Equal(t, baseline, snapshot)
	})

	t.Run("nil query service returns without error", func(t *testing.T) {
		st := &schemaTracker{
			logger:          logger,
			getQueryService: func() executor.InternalQueryService { return nil },
		}

		changed, snapshot, err := st.poll(ctx, nil)
		require.NoError(t, err)
		assert.False(t, changed)
		assert.Nil(t, snapshot)
	})
}

// ----------------------------------------------------------------------------
// run — notification coalescing and version increments
// ----------------------------------------------------------------------------

func TestRun_TickerPollsAndIncrementsVersion(t *testing.T) {
	ctx := t.Context()

	mockQS := mock.NewQueryService()
	// First poll: baseline (no change).
	mockQS.AddQueryPatternOnce("SELECT", makeSchemaResult([][]any{
		{"public", "users", "16384", "id:23,name:25"},
	}))
	// Second poll: table added (change).
	mockQS.AddQueryPatternOnce("SELECT", makeSchemaResult([][]any{
		{"public", "users", "16384", "id:23,name:25"},
		{"public", "posts", "16385", "id:23,title:25"},
	}))

	var notified atomic.Int64
	st := newSchemaTracker(
		ctx,
		testLogger(),
		func() executor.InternalQueryService { return mockQS },
		nil, // pubsubListener not needed — we test the ticker path
		func(v int64) { notified.Store(v) },
		50*time.Millisecond, // short interval for test
	)

	st.wg.Add(1)
	runCtx, runCancel := context.WithCancel(ctx)
	go st.run(runCtx)

	// Wait for two ticks to fire.
	require.Eventually(t, func() bool {
		return notified.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond, "expected version to be incremented after schema change")

	runCancel()
	st.wg.Wait()
}

// ----------------------------------------------------------------------------
// start / stop lifecycle
// ----------------------------------------------------------------------------

func TestStartStop(t *testing.T) {
	ctx := context.Background()

	st := newSchemaTracker(
		ctx,
		testLogger(),
		func() executor.InternalQueryService { return nil },
		nil,
		func(v int64) {},
		time.Hour,
	)

	// start is idempotent.
	st.start()
	st.start()
	assert.NotNil(t, st.cancel)

	// stop is idempotent.
	st.stop()
	st.stop()
	assert.Nil(t, st.cancel)
}

// ----------------------------------------------------------------------------
// OnStateChange
// ----------------------------------------------------------------------------

func TestOnStateChange_StartsOnPrimaryServing(t *testing.T) {
	ctx := t.Context()

	// Use a stubPubSubListener that satisfies the schema tracker's needs
	// without requiring a real pool manager connection.
	stub := &stubPubSubListener{startedCh: make(chan struct{})}
	close(stub.startedCh) // pretend already running

	mockQS := mock.NewQueryService()
	// Provide a result for the initial poll.
	mockQS.AddQueryPattern("SELECT", makeSchemaResult(nil))

	st := &schemaTracker{
		logger:          testLogger().With("component", "schema_tracker"),
		getQueryService: func() executor.InternalQueryService { return mockQS },
		pubsubListener:  stub,
		notifyVersion:   func(v int64) {},
		pollInterval:    time.Hour,
		ctx:             ctx,
		notifCh:         make(chan *sqltypes.Notification, 16),
	}

	// Transition to PRIMARY+SERVING — should start the tracker.
	err := st.OnStateChange(ctx,
		clustermetadatapb.PoolerType_PRIMARY,
		clustermetadatapb.PoolerServingStatus_SERVING,
	)
	require.NoError(t, err)
	assert.NotNil(t, st.cancel, "tracker should be running after PRIMARY+SERVING")
	assert.True(t, stub.subscribed, "should have subscribed to schema change channel")

	// Transition away — should stop the tracker.
	err = st.OnStateChange(ctx,
		clustermetadatapb.PoolerType_REPLICA,
		clustermetadatapb.PoolerServingStatus_SERVING,
	)
	require.NoError(t, err)
	assert.Nil(t, st.cancel, "tracker should be stopped after REPLICA+SERVING")
}

// stubPubSubListener is a minimal stand-in for *pubsub.Listener that records
// Subscribe calls without requiring a real PostgreSQL connection.
type stubPubSubListener struct {
	startedCh  chan struct{}
	subscribed bool
}

func (s *stubPubSubListener) AwaitRunning(ctx context.Context) {
	select {
	case <-s.startedCh:
	case <-ctx.Done():
	}
}

func (s *stubPubSubListener) SubscribeCh(_ string, _ chan *sqltypes.Notification) {
	s.subscribed = true
}
