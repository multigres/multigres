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

package shardsetup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithPgInitdbArgs verifies the SetupOption stores the verbatim initdb
// args string on SetupConfig. The args are passed through to pgctld's
// --pg-initdb-args flag at process spawn time.
func TestWithPgInitdbArgs(t *testing.T) {
	c := &SetupConfig{}
	WithPgInitdbArgs("--no-locale --encoding=UTF8")(c)
	assert.Equal(t, "--no-locale --encoding=UTF8", c.PgInitdbArgs)
}

// TestWithPgInitdbExtraConfFiles verifies the SetupOption appends each path
// to SetupConfig.PgInitdbExtraConfFiles. Variadic + repeated invocations
// must accumulate rather than overwrite so callers can stack snippets.
func TestWithPgInitdbExtraConfFiles(t *testing.T) {
	t.Run("single path", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf"}, c.PgInitdbExtraConfFiles)
	})

	t.Run("multiple paths in one call", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf", "/tmp/b.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf", "/tmp/b.conf"}, c.PgInitdbExtraConfFiles)
	})

	t.Run("repeated calls accumulate", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf")(c)
		WithPgInitdbExtraConfFiles("/tmp/b.conf", "/tmp/c.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf", "/tmp/b.conf", "/tmp/c.conf"}, c.PgInitdbExtraConfFiles)
	})
}

// TestProcessInstancePgInitdbArgs pins the pgctld arg-construction path
// that forwards PgInitdbArgs as --pg-initdb-args. Bypasses the actual
// process spawn (which needs a real pgctld binary on PATH) by reading the
// argv that startPgctld would assemble.
func TestProcessInstancePgInitdbArgs(t *testing.T) {
	t.Run("empty arg omits flag", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: ""}
		args := buildPgctldServerArgs(p)
		assert.NotContains(t, args, "--pg-initdb-args")
	})

	t.Run("non-empty arg appends flag and value", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: "--no-locale --encoding=UTF8"}
		args := buildPgctldServerArgs(p)
		// flag and value are appended as two argv slots so exec preserves
		// the value as a single argument even with embedded spaces.
		idx := -1
		for i, a := range args {
			if a == "--pg-initdb-args" {
				idx = i
				break
			}
		}
		if assert.GreaterOrEqual(t, idx, 0, "--pg-initdb-args flag should be present") {
			assert.Equal(t, "--no-locale --encoding=UTF8", args[idx+1])
		}
	})
}
