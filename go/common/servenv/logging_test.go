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

func TestBuildHandler_ErrorValueRendersAsString(t *testing.T) {
	// A raw error passed as a value serializes to its Error() string, not an
	// empty object. This is what lets the canonical "error" key carry the
	// message without any custom handling.
	var buf bytes.Buffer
	logger := slog.New(buildHandler(&buf, "json", slog.LevelDebug))
	logger.Error("boom", "error", errors.New("kaboom"))

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	assert.Equal(t, "kaboom", record["error"])
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
