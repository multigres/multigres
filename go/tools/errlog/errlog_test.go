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

package errlog

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestDedupe_Observe(t *testing.T) {
	ctx := context.Background()
	logger, buf := newLogger()
	var d Dedupe
	observe := func(key string, err error) {
		d.Observe(ctx, logger, key, slog.LevelWarn, "failed", "recovered", err, "k", key)
	}
	count := func(s string) int { return strings.Count(buf.String(), s) }

	observe("a", nil)
	assert.Zero(t, buf.Len(), "success with no prior failure is silent")

	for range 5 {
		observe("a", errors.New("boom"))
	}
	assert.Equal(t, 1, count("msg=failed"))
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "error=boom")
	assert.Contains(t, buf.String(), "k=a")

	observe("a", errors.New("different"))
	assert.Equal(t, 2, count("msg=failed"), "changed error text logs again")

	observe("b", errors.New("different"))
	assert.Equal(t, 3, count("msg=failed"), "keys are independent")

	observe("a", nil)
	assert.Equal(t, 1, count("msg=recovered"))
	assert.Contains(t, buf.String(), "level=INFO msg=recovered")
	observe("a", nil)
	assert.Equal(t, 1, count("msg=recovered"), "recovery logs once")

	observe("a", errors.New("boom"))
	assert.Equal(t, 4, count("msg=failed"), "a failure after recovery logs again")
}

func TestDedupe_RetainAndReset(t *testing.T) {
	ctx := context.Background()
	logger, buf := newLogger()
	var d Dedupe
	fail := func(key string) {
		d.Observe(ctx, logger, key, slog.LevelError, "failed", "recovered", errors.New("boom"))
	}

	fail("a")
	fail("b")
	d.Retain(func(key string) bool { return key == "a" })
	buf.Reset()
	fail("a")
	fail("b")
	assert.Equal(t, 1, strings.Count(buf.String(), "msg=failed"), "only the dropped key logs afresh")
	assert.NotContains(t, buf.String(), "recovered", "retain drops state without logging")
	assert.Contains(t, buf.String(), "level=ERROR")

	d.Reset()
	buf.Reset()
	fail("a")
	fail("b")
	assert.Equal(t, 2, strings.Count(buf.String(), "msg=failed"))
}
