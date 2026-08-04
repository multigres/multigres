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

package servenv

// These tests pin the runtime log-shape guarantees: whatever call-site style is
// used, records built through buildHandler share one shape, and eventlog.Emit
// produces a meaningful message rather than a fixed sentinel.
//
// Attribute-key consistency (the canonical "error" key) is enforced statically
// by sloglint in CI, not at runtime, so it is not exercised here.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/eventlog"
)

func decodeRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec), "log line must be valid JSON")
	return rec
}

// eventlog.Emit used to hardcode the record message to the sentinel
// "multigres.event", so every event collapsed into one opaque value in the
// message column. It now uses the event's canonical type, matching how every
// other log line reads, while keeping event_type/outcome for structured filters.
func TestConsistency_EventMessageIsMeaningful(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "json", slog.LevelInfo))

	eventlog.Emit(context.Background(), logger, eventlog.Failed,
		eventlog.NodeJoin{NodeName: "p-1"}, "error", errors.New("rpc failed"))
	rec := decodeRecord(t, &buf)

	assert.NotEqual(t, "multigres.event", rec["msg"], "old sentinel must not reappear")
	assert.Equal(t, "node.join", rec["msg"], "message is the event's canonical type")
	assert.Equal(t, "node.join", rec["event_type"], "event_type retained for filtering")
	assert.Equal(t, "failed", rec["outcome"])
	assert.Equal(t, "ERROR", rec["level"], "Failed outcome logs at ERROR")
	assert.Equal(t, "rpc failed", rec["error"], "error travels under the canonical key")
}

// Whatever call-site style is used — plain, context-aware, or an event — every
// record shares the same top-level skeleton (time/level/msg), and error-carrying
// records place the error under the canonical "error" key.
func TestConsistency_UniformRecordShape(t *testing.T) {
	err := errors.New("kaboom")
	ctx := context.Background()

	cases := map[string]struct {
		logIt    func(l *slog.Logger)
		hasError bool
	}{
		"plain error":   {func(l *slog.Logger) { l.Error("plain error", "error", err) }, true},
		"context error": {func(l *slog.Logger) { l.ErrorContext(ctx, "context error", "error", err) }, true},
		"info":          {func(l *slog.Logger) { l.Info("informational", "count", 3) }, false},
		"event": {func(l *slog.Logger) {
			eventlog.Emit(ctx, l, eventlog.Success, eventlog.NodeJoin{NodeName: "p-2"})
		}, false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			tc.logIt(slog.New(buildHandler(&buf, "json", slog.LevelDebug)))
			rec := decodeRecord(t, &buf)

			for _, key := range []string{"time", "level", "msg"} {
				assert.Contains(t, rec, key, "every record carries a %q field", key)
			}
			if tc.hasError {
				assert.Equal(t, "kaboom", rec["error"], "error carried under the canonical key")
			}
		})
	}
}
