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

package reserved

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// levelRecorder records the level of every record by message.
type levelRecorder struct {
	mu     sync.Mutex
	levels map[string][]slog.Level
}

func (h *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.levels[r.Message] = append(h.levels[r.Message], r.Level)
	return nil
}

func (h *levelRecorder) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *levelRecorder) WithGroup(_ string) slog.Handler { return h }

func TestPool_KillConnection_LogsAtDebug(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	rec := &levelRecorder{levels: make(map[string][]slog.Level)}
	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		Logger:            slog.New(rec),
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rc, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, pool.KillConnection(ctx, rc.ConnID()))

	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, []slog.Level{slog.LevelDebug}, rec.levels["connection killed"])
}
