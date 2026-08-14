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

// Command normalize repairs a `go test -json` JSONL file so the CI test
// reporter (dorny/test-reporter, golang-json) can parse it even when a test
// group's final event lacks an Elapsed field. See the gotestjson package doc
// for the why and the exact guarantees.
//
// Usage:
//
//	normalize FILE [FILE...]   # rewrite each file in place
//	normalize                  # read stdin, write stdout
//
// It is deliberately tolerant: a missing input file is reported and skipped
// (exit 0) rather than failing the job, matching the timing-summary tool, so a
// not-yet-created results file never turns a green run red. Repaired groups
// that never reached a terminal (an interrupted/killed test) are surfaced as a
// GitHub Actions warning but do not, on their own, fail this step — the test
// run's own exit code is what gates the job.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/tools/gotestjson"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1) //nolint:forbidigo // os.Exit is fine in main()
	}
}

func run(args []string) error {
	if len(args) == 0 {
		stats, err := gotestjson.Normalize(os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "normalized stdin: %s\n", stats)
		return nil
	}
	for _, path := range args {
		if err := normalizeFile(path); err != nil {
			return err
		}
	}
	return nil
}

// normalizeFile rewrites path in place. It writes the normalized output to a
// temporary file in the same directory and atomically renames it over the
// original, so a crash mid-write can never leave a half-written results file.
func normalizeFile(path string) error {
	// #nosec G703 -- path is a CI-controlled argument (this tool's own workflow
	// invocations), not external/untrusted input.
	in, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "No %s found — skipping normalization\n", path)
			return nil
		}
		return err
	}
	defer in.Close()

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".norm-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	stats, err := gotestjson.Normalize(in, tmp)
	if err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := in.Close(); err != nil {
		return err
	}
	// #nosec G703 -- path is a CI-controlled argument (this tool's own workflow
	// invocations), not external/untrusted input.
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "normalized %s: %s\n", path, stats)
	if stats.Interrupted > 0 {
		// A test that never reached a terminal event means the stream was
		// interrupted (timeout, panic, OOM). Make it visible; the failing test
		// run itself is what fails the job.
		fmt.Printf("::warning::%s: %d test(s) had no terminal event and were marked failed (interrupted stream)\n",
			path, stats.Interrupted)
	}
	return nil
}
