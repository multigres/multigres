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

package eventlog

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler captures slog records for assertion.
type testHandler struct {
	records []slog.Record
}

func (h *testHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *testHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}
func (h *testHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *testHandler) WithGroup(string) slog.Handler      { return h }

// recordAttrs extracts all attributes from a slog.Record into a map.
func recordAttrs(r slog.Record) map[string]any {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	return attrs
}

func TestEmit(t *testing.T) {
	tests := []struct {
		name          string
		outcome       Outcome
		event         Event
		extra         []any
		wantLevel     slog.Level
		wantEventType string
		wantAttrs     map[string]any
	}{
		{
			name:          "event with fields",
			outcome:       Success,
			event:         PrimaryPromotion{NewPrimary: "pg-1"},
			wantLevel:     slog.LevelInfo,
			wantEventType: "primary.promotion",
			wantAttrs:     map[string]any{"outcome": "success", "new_primary": "pg-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &testHandler{}
			logger := slog.New(h)

			Emit(context.Background(), logger, tt.outcome, tt.event, tt.extra...)

			require.Len(t, h.records, 1)
			r := h.records[0]
			assert.Equal(t, "multigres.event", r.Message)
			assert.Equal(t, tt.wantLevel, r.Level)

			attrs := recordAttrs(r)
			assert.Equal(t, tt.wantEventType, attrs["event_type"])
			for k, v := range tt.wantAttrs {
				assert.Equal(t, v, attrs[k], "attr %s", k)
			}
		})
	}
}
