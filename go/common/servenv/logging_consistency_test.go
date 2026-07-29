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

// These tests pin the log-shape consistency guarantees this change introduced.
// Each one contrasts the "before" behavior (what the logger used to produce)
// with the "after" behavior (what buildHandler / eventlog.Emit produce now), so
// a regression that reintroduces the inconsistency fails here loudly.

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

// Before this change the logger built a stock JSON handler with no ReplaceAttr,
// so a caller writing "err" and a caller writing "error" produced two different
// field names for the same concept. buildHandler now folds them onto "error",
// so both call-site styles serialize identically.
func TestConsistency_ErrorKeyUnifiedAcrossCallSites(t *testing.T) {
	err := errors.New("boom")

	// BEFORE: the stock handler logging.go used to construct keeps "err" as-is.
	var before bytes.Buffer
	slog.New(slog.NewJSONHandler(&before, &slog.HandlerOptions{})).Error("x", "err", err) //nolint:sloglint // this test asserts the old, un-normalized handler kept "err"
	beforeRec := decodeRecord(t, &before)
	require.Equal(t, "boom", beforeRec["err"], "before: the inconsistent 'err' field survived")
	require.NotContains(t, beforeRec, "error", "before: nothing landed under the canonical key")

	// AFTER: buildHandler normalizes, so "err" and "error" converge.
	writeVia := func(key string) map[string]any {
		var buf bytes.Buffer
		slog.New(buildHandler(&buf, "json", slog.LevelInfo)).Error("x", key, err)
		return decodeRecord(t, &buf)
	}
	viaErr := writeVia("err")
	viaError := writeVia("error")

	assert.Equal(t, "boom", viaErr["error"], "after: 'err' is renamed to 'error'")
	assert.NotContains(t, viaErr, "err", "after: the 'err' field no longer appears")
	assert.Equal(t, viaError["error"], viaErr["error"],
		"after: writing 'err' and 'error' produce the identical error field")
}

// Before this change eventlog.Emit hardcoded the record message to the sentinel
// "multigres.event", so every event collapsed into one opaque value in the
// message column. It now uses the event's canonical type, matching how every
// other log line reads, while keeping event_type/outcome for structured filters.
func TestConsistency_EventMessageIsMeaningful(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "json", slog.LevelInfo))

	eventlog.Emit(context.Background(), logger, eventlog.Failed,
		eventlog.NodeJoin{NodeName: "p-1"}, "error", errors.New("rpc failed"))
	rec := decodeRecord(t, &buf)

	assert.NotEqual(t, "multigres.event", rec["msg"], "before-sentinel must not reappear")
	assert.Equal(t, "node.join", rec["msg"], "message is the event's canonical type")
	assert.Equal(t, "node.join", rec["event_type"], "event_type retained for filtering")
	assert.Equal(t, "failed", rec["outcome"])
	assert.Equal(t, "ERROR", rec["level"], "Failed outcome logs at ERROR")
	assert.Equal(t, "rpc failed", rec["error"], "error travels under the canonical key here too")
}

// Whatever call-site style is used — plain, context-aware, or an event — every
// record shares the same top-level skeleton (time/level/msg) and never carries
// the non-canonical "err" key.
func TestConsistency_UniformRecordShape(t *testing.T) {
	err := errors.New("kaboom")
	ctx := context.Background()

	cases := map[string]func(l *slog.Logger){
		"plain error":   func(l *slog.Logger) { l.Error("plain error", "error", err) },
		"context error": func(l *slog.Logger) { l.ErrorContext(ctx, "context error", "error", err) },
		"info with err key": func(l *slog.Logger) {
			l.Info("informational", "err", err) //nolint:sloglint // deliberately the non-canonical key, to prove it is normalized away
		},
		"event": func(l *slog.Logger) {
			eventlog.Emit(ctx, l, eventlog.Success, eventlog.NodeJoin{NodeName: "p-2"})
		},
	}

	for name, logIt := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logIt(slog.New(buildHandler(&buf, "json", slog.LevelDebug)))
			rec := decodeRecord(t, &buf)

			for _, key := range []string{"time", "level", "msg"} {
				assert.Contains(t, rec, key, "every record carries a %q field", key)
			}
			assert.NotContains(t, rec, "err", "no record exposes the non-canonical 'err' key")
		})
	}
}

// Guards the assumption underpinning normalizeAttr: slog already serializes an
// error value to its Error() string, so the normalizer only has to fix the key,
// never coerce the value.
func TestConsistency_ErrorValueSerializesAsString(t *testing.T) {
	var buf bytes.Buffer
	slog.New(buildHandler(&buf, "json", slog.LevelInfo)).Error("x", "error", errors.New("detail"))
	rec := decodeRecord(t, &buf)
	assert.Equal(t, "detail", rec["error"], "error value renders as its string, not an object")
}
