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

package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureDefaultLog routes the default slog logger to a buffer for the test.
func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestCancelOnServer_LogsWhenCancelsStartFailingAndWhenTheyRecover(t *testing.T) {
	logs := captureDefaultLog(t)
	cancelFailures.Store(0)
	t.Cleanup(func() { cancelFailures.Store(0) })
	errUnreachable := errors.New("postmaster unreachable")
	fail := func(context.Context) error { return errUnreachable }
	succeed := func(context.Context) error { return nil }

	for range 3 {
		cancelOnServer(t.Context(), fail)
	}
	cancelOnServer(t.Context(), succeed)
	cancelOnServer(t.Context(), succeed)
	cancelOnServer(t.Context(), fail)

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.Len(t, lines, 3, logs.String())
	assert.Contains(t, lines[0], "level=WARN")
	assert.Contains(t, lines[0], "error=\"postmaster unreachable\"")
	assert.Contains(t, lines[1], "level=INFO")
	assert.Contains(t, lines[1], "failed_cancels=3")
	assert.Contains(t, lines[2], "level=WARN", "a new run of failures is logged again")
	assert.Equal(t, int64(1), cancelFailures.Load())
}

func TestCancelOnServer_BoundsTheSendEvenIfTheCallerHasNoDeadline(t *testing.T) {
	cancelFailures.Store(0)
	t.Cleanup(func() { cancelFailures.Store(0) })
	callerCtx, cancel := context.WithCancel(t.Context())
	cancel()

	var sendErr error
	cancelOnServer(callerCtx, func(ctx context.Context) error {
		_, hasDeadline := ctx.Deadline()
		assert.True(t, hasDeadline, "the send must be bounded")
		sendErr = ctx.Err()
		return nil
	})

	assert.NoError(t, sendErr, "the send must not inherit the caller's cancellation")
}
