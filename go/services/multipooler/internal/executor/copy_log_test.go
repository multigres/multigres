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

package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
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

func TestLogCopyFailure_Level(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want slog.Level
	}{
		{
			name: "postgres error response is a client error",
			err:  mterrors.NewPgError("ERROR", "23505", "duplicate key value violates unique constraint", ""),
			want: slog.LevelDebug,
		},
		{
			name: "wrapped postgres error response is a client error",
			err:  fmt.Errorf("copy: %w", mterrors.NewPgError("ERROR", "22P02", "invalid input syntax", "")),
			want: slog.LevelDebug,
		},
		{
			name: "fatal postgres error kills the connection",
			err:  mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", ""),
			want: slog.LevelError,
		},
		{
			name: "wrapped fatal postgres error kills the connection",
			err:  fmt.Errorf("copy: %w", mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", "")),
			want: slog.LevelError,
		},
		{
			name: "connection failure",
			err:  errors.New("failed to read message type: EOF"),
			want: slog.LevelError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &levelRecorder{levels: make(map[string][]slog.Level)}
			e := &Executor{logger: slog.New(rec)}

			e.logCopyFailure(context.Background(), tt.err)

			require.Equal(t, []slog.Level{tt.want}, rec.levels["COPY operation failed"])
			assert.Len(t, rec.levels, 1)
		})
	}
}
