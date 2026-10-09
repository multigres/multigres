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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A test group that passes but then has a stray output line attributed to it,
// so its last event lacks Elapsed — the shape that crashes the reporter.
const strayPassStream = `{"Action":"run","Package":"pkg","Test":"TestA"}
{"Action":"pass","Package":"pkg","Test":"TestA","Elapsed":0.1}
{"Action":"output","Package":"pkg","Test":"TestA","Output":"stray async line\n"}
`

// An interrupted group: no terminal event at all.
const interruptedStream = `{"Action":"run","Package":"pkg","Test":"TestB"}
{"Action":"output","Package":"pkg","Test":"TestB","Output":"panic: boom\n"}
`

// lastEventHasElapsed mirrors the reporter's contract: every (Package, Test)
// group's last event must carry Elapsed.
func lastEventHasElapsed(t *testing.T, data string) bool {
	t.Helper()
	last := map[string]bool{}
	for line := range strings.SplitSeq(strings.TrimSpace(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e struct {
			Package string   `json:"Package"`
			Test    string   `json:"Test"`
			Elapsed *float64 `json:"Elapsed"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &e))
		if e.Test == "" {
			continue
		}
		last[e.Package+"/"+e.Test] = e.Elapsed != nil
	}
	for _, ok := range last {
		if !ok {
			return false
		}
	}
	return true
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "results.jsonl")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestNormalizeFileRewritesInPlace(t *testing.T) {
	p := writeTemp(t, strayPassStream)
	require.False(t, lastEventHasElapsed(t, strayPassStream), "fixture must start in the broken shape")

	require.NoError(t, normalizeFile(p))

	out, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.True(t, lastEventHasElapsed(t, string(out)), "every group's last event must carry Elapsed after normalization")
}

func TestNormalizeFileInterruptedWarns(t *testing.T) {
	p := writeTemp(t, interruptedStream)
	// Exercises the Interrupted > 0 branch (the ::warning:: path).
	require.NoError(t, normalizeFile(p))

	out, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.True(t, lastEventHasElapsed(t, string(out)))
	assert.Contains(t, string(out), `"Action":"fail"`, "an interrupted test must be surfaced as failed")
}

func TestNormalizeFileMissingIsSkipped(t *testing.T) {
	// A not-yet-created results file must be skipped, not error the step.
	err := normalizeFile(filepath.Join(t.TempDir(), "does-not-exist.jsonl"))
	assert.NoError(t, err)
}

func TestRunWithFileArgs(t *testing.T) {
	p1 := writeTemp(t, strayPassStream)
	p2 := writeTemp(t, strayPassStream)

	require.NoError(t, run([]string{p1, p2}))

	for _, p := range []string{p1, p2} {
		out, err := os.ReadFile(p)
		require.NoError(t, err)
		assert.True(t, lastEventHasElapsed(t, string(out)))
	}
}

func TestRunStdinToStdout(t *testing.T) {
	inPath := writeTemp(t, strayPassStream)
	in, err := os.Open(inPath)
	require.NoError(t, err)
	defer in.Close()

	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	out, err := os.Create(outPath)
	require.NoError(t, err)
	defer out.Close()

	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = in, out
	defer func() { os.Stdin, os.Stdout = oldIn, oldOut }()

	require.NoError(t, run(nil))
	require.NoError(t, out.Close())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.True(t, lastEventHasElapsed(t, string(data)))
}
