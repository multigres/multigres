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

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logAndDecode logs a single record through a JSON handler built by buildHandler
// and returns the decoded record.
func logAndDecode(t *testing.T, level slog.Level, msg string, args ...any) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "json", slog.LevelDebug))
	logger.Log(t.Context(), level, msg, args...)

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	return record
}

func TestNormalizeAttr_ErrKeyCollapsesToError(t *testing.T) {
	record := logAndDecode(t, slog.LevelError, "boom", "err", errors.New("kaboom"))

	assert.Equal(t, "kaboom", record["error"], "err key should be renamed to error")
	_, hasErr := record["err"]
	assert.False(t, hasErr, "err key should not survive normalization")
}

func TestNormalizeAttr_ErrorValueRendersAsString(t *testing.T) {
	// A raw error passed as a value must serialize to its Error() string, not an
	// empty object, regardless of whether the call site used "err" or "error".
	record := logAndDecode(t, slog.LevelError, "boom", "error", errors.New("kaboom"))
	assert.Equal(t, "kaboom", record["error"])
}

func TestNormalizeAttr_LeavesNestedGroupsUntouched(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "json", slog.LevelDebug))
	group := slog.Group("inner", slog.String("err", "nested")) //nolint:sloglint // the nested "err" key is deliberate: it verifies normalizeAttr leaves grouped keys untouched
	logger.Error("boom", group)

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))

	inner, ok := record["inner"].(map[string]any)
	require.True(t, ok, "expected inner group")
	assert.Equal(t, "nested", inner["err"], "nested err key should be left untouched")
}

func TestBuildHandler_UnknownFormatFallsBackToJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "yaml-ish-nonsense", slog.LevelInfo))
	logger.Info("hello")

	// Valid JSON => JSON handler was chosen.
	var record map[string]any
	assert.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	assert.Equal(t, "hello", record["msg"])
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":     slog.LevelDebug,
		"INFO":      slog.LevelInfo,
		"Warn":      slog.LevelWarn,
		"error":     slog.LevelError,
		"":          slog.LevelInfo,
		"gibberish": slog.LevelInfo,
	}
	for in, want := range cases {
		assert.Equalf(t, want, parseLevel(in), "parseLevel(%q)", in)
	}
}
