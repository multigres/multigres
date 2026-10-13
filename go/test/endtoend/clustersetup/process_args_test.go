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

package clustersetup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestProcessInstancePgInitdbArgs pins the pgctld arg-construction path
// that forwards PgInitdbArgs as --pg-initdb-args. Bypasses the actual
// process spawn (which needs a real pgctld binary on PATH) by reading the
// argv that startPgctld would assemble.
func TestProcessInstancePgInitdbArgs(t *testing.T) {
	t.Run("empty arg omits flag", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: ""}
		args := BuildPgctldServerArgs(p)
		assert.NotContains(t, args, "--pg-initdb-args")
	})

	t.Run("non-empty arg appends flag and value", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: "--no-locale --encoding=UTF8"}
		args := BuildPgctldServerArgs(p)
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
