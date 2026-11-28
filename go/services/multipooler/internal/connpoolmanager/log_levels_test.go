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

package connpoolmanager

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// capturedLog is one record seen by recordingLogHandler.
type capturedLog struct {
	level slog.Level
	attrs map[string]any
}

// recordingLogHandler records every log record by message.
type recordingLogHandler struct {
	mu      sync.Mutex
	records map[string][]capturedLog
}

func newRecordingLogHandler() *recordingLogHandler {
	return &recordingLogHandler{records: make(map[string][]capturedLog)}
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records[r.Message] = append(h.records[r.Message], capturedLog{level: r.Level, attrs: attrs})
	return nil
}

func (h *recordingLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *recordingLogHandler) WithGroup(_ string) slog.Handler { return h }

func (h *recordingLogHandler) get(msg string) []capturedLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]capturedLog(nil), h.records[msg]...)
}

// newLoggedTestManager creates a Manager that logs to the given handler.
func newLoggedTestManager(t *testing.T, server *fakepgserver.Server, handler slog.Handler) *Manager {
	t.Helper()

	config := NewConfig(viperutil.NewRegistry())
	resolveTestPgPassword(t, config)

	manager := config.NewManager(slog.New(handler))
	manager.Open(context.Background(), &ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})
	return manager
}

func TestManager_CreateUserPoolLogsOnce(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	handler := newRecordingLogHandler()
	manager := newLoggedTestManager(t, server, handler)
	defer manager.Close()

	ctx := context.Background()
	for _, user := range []string{"user1", "user2"} {
		conn, err := manager.GetRegularConn(ctx, user, nil, nil)
		require.NoError(t, err)
		conn.Recycle()
	}

	created := handler.get("created user pool")
	require.Len(t, created, 2, "exactly one line per created pool")
	for _, rec := range created {
		assert.Equal(t, slog.LevelInfo, rec.level)
		assert.Contains(t, rec.attrs, "user")
		assert.Contains(t, rec.attrs, "total_users")
		assert.Contains(t, rec.attrs, "regular_capacity")
		assert.Contains(t, rec.attrs, "reserved_capacity")
	}
	assert.Empty(t, handler.get("user pool created"), "the pool constructor must not log a second line")
}

func TestManager_ClassCapacitySplitLogsAtDebug(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	handler := newRecordingLogHandler()
	manager := newLoggedTestManager(t, server, handler)
	defer manager.Close()

	ctx := context.Background()
	manager.splitClassCapacity(ctx, map[string]int64{"u": 1}, map[string]int64{"u": 1})
	manager.splitClassCapacity(ctx, map[string]int64{"u": 100}, map[string]int64{"u": 1})

	updates := handler.get("class capacity split updated")
	require.NotEmpty(t, updates, "a changed split must be logged")
	for _, rec := range updates {
		assert.Equal(t, slog.LevelDebug, rec.level)
	}
}

func TestManager_GarbageCollectLogsPerPoolOnly(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	handler := newRecordingLogHandler()
	manager := newLoggedTestManager(t, server, handler)
	defer manager.Close()

	ctx := context.Background()
	conn, err := manager.GetRegularConn(ctx, "inactive-user", nil, nil)
	require.NoError(t, err)
	conn.Recycle()

	pools := manager.userPoolsSnapshot.Load()
	(*pools)["inactive-user"].lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	manager.garbageCollectInactivePools(ctx)

	assert.Empty(t, handler.get("garbage collection complete"))
	removed := handler.get("garbage collected inactive user pool")
	require.Len(t, removed, 1)
	assert.Equal(t, slog.LevelInfo, removed[0].level)
	assert.Equal(t, "inactive-user", removed[0].attrs["user"])
	assert.EqualValues(t, 0, removed[0].attrs["remaining_pools"])
}
