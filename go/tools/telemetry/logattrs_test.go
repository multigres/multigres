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
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextWithLogAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewTelemetry().WrapSlogHandler(slog.NewJSONHandler(&buf, nil)))

	ctx := ContextWithLogAttrs(t.Context(), slog.String("a", "1"))
	ctx = ContextWithLogAttrs(ctx, slog.String("b", "2"))
	logger.InfoContext(ctx, "with attrs")
	logger.InfoContext(t.Context(), "without attrs")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)

	var with, without map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &with))
	require.NoError(t, json.Unmarshal(lines[1], &without))

	assert.Equal(t, "1", with["a"])
	assert.Equal(t, "2", with["b"])
	assert.NotContains(t, without, "a")
	assert.NotContains(t, without, "b")
}
