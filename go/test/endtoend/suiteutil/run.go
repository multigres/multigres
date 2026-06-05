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

package suiteutil

import (
	"context"
	"testing"
	"time"
)

// Per-file run scaffolding shared by the differential query-serving suites:
// a bounded-context wrapper, the schema-resetter setup boilerplate, and the
// deadline-aware corpus iteration loop.

// RunWithTimeout invokes fn under a child context bounded by timeout, cancelling
// it when fn returns. The result type is generic so each suite can return its
// own per-run result struct.
func RunWithTimeout[T any](parent context.Context, timeout time.Duration, fn func(context.Context) T) T {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return fn(ctx)
}

// NewSchemaResetterWithCleanup opens a long-lived schema-reset connection for
// target with a bounded dial timeout, fails the test if it can't connect, and
// registers a t.Cleanup to close it. Suites call this once per target (the
// PostgreSQL baseline and, for the gateway, a connection pinned directly to the
// underlying primary PostgreSQL — see SchemaResetter for why a single pinned
// backend matters).
func NewSchemaResetterWithCleanup(t *testing.T, target Target) *SchemaResetter {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	r, err := NewSchemaResetter(ctx, target)
	if err != nil {
		t.Fatalf("open schema-resetter for %s: %v", target.Name, err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

// RunCorpusLoop iterates files in order, invoking runFile for each, while
// honoring the overall context deadline so a timeout still produces a coherent
// partial report. Before each file it checks ctx: once cancelled it logs how
// far it got, stops, and returns timedOut=true. logProgress is called after
// every 25th file and after the last, so each suite can log its own counters.
//
// Returns ran (the number of files runFile was invoked for) and whether the
// loop stopped early on the deadline.
func RunCorpusLoop(t *testing.T, ctx context.Context, files []string, runFile func(i int, file string), logProgress func(done, total int)) (ran int, timedOut bool) {
	t.Helper()
	for i, f := range files {
		select {
		case <-ctx.Done():
			t.Logf("overall deadline reached after %d/%d files; stopping run loop", i, len(files))
			return i, true
		default:
		}

		runFile(i, f)

		if (i+1)%25 == 0 || (i+1) == len(files) {
			logProgress(i+1, len(files))
		}
	}
	return len(files), false
}
