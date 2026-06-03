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
	"errors"
	"os/exec"
	"strings"
)

// Small helpers shared by suite runners/reports for classifying subprocess
// errors and trimming captured output.

// IsExitError reports whether err is a non-zero process exit (as opposed to a
// failure to exec at all, e.g. a missing binary). Runners use this to tell "the
// tool ran and the test failed" apart from "the tool could not run".
func IsExitError(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}

// TruncateOutput bounds captured output to limit bytes, appending a marker when
// it had to cut. Keeps a single pathological file from bloating results.json.
// The limit differs per suite (trace verbosity varies), so it is an argument.
func TruncateOutput(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "\n... (output truncated)"
}

// FirstLine returns the first non-empty line of s, trimmed. Used to surface a
// one-line cause (e.g. the first line of a failed run's output) in summaries.
func FirstLine(s string) string {
	s = strings.TrimSpace(s)
	first, _, _ := strings.Cut(s, "\n")
	return strings.TrimSpace(first)
}
