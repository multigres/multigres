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

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logOnce logs one record with ctx and returns its decoded JSON.
func logOnce(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(NewTelemetry().WrapSlogHandler(slog.NewJSONHandler(&buf, nil)))
	logger.InfoContext(ctx, "msg")
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

func TestLogAttrs_TaskStepAndClientAddress(t *testing.T) {
	ctx := WithTask(t.Context(), "postgres-monitor")
	ctx = WithStep(ctx, "restore-from-backup")
	ctx = WithClientAddress(ctx, &net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 54321})

	rec := logOnce(t, ctx)
	assert.Equal(t, "postgres-monitor", rec[LogKeyTask])
	assert.Equal(t, "restore-from-backup", rec[LogKeyStep])
	assert.Equal(t, "10.1.2.3", rec[LogKeyClientAddress])
}

func TestLogAttrs_ReplaceNotAppend(t *testing.T) {
	ctx := WithStep(WithTask(t.Context(), "a"), "s1")
	ctx = WithStep(ctx, "s2")

	var buf bytes.Buffer
	logger := slog.New(NewTelemetry().WrapSlogHandler(slog.NewJSONHandler(&buf, nil)))
	logger.InfoContext(ctx, "msg")
	// Count raw JSON keys: decoding into a map would hide a duplicate.
	assert.Equal(t, 1, bytes.Count(buf.Bytes(), []byte(`"`+LogKeyStep+`"`)), buf.String())
	assert.Contains(t, buf.String(), `"`+LogKeyStep+`":"s2"`)
}

func TestLogAttrs_NewTaskClearsStep(t *testing.T) {
	parent := WithStep(WithTask(t.Context(), "MultiPoolerManager/Promote"), "wait-for-catchup")
	child := WithTask(parent, "post-promotion-checkpoint")

	rec := logOnce(t, child)
	assert.Equal(t, "post-promotion-checkpoint", rec[LogKeyTask])
	assert.NotContains(t, rec, LogKeyStep)

	// The parent context is unchanged.
	assert.Equal(t, "wait-for-catchup", logOnce(t, parent)[LogKeyStep])
}

func TestLogAttrs_AbsentWithoutTagging(t *testing.T) {
	rec := logOnce(t, t.Context())
	assert.NotContains(t, rec, LogKeyTask)
	assert.NotContains(t, rec, LogKeyStep)
	assert.NotContains(t, rec, LogKeyClientAddress)
}
