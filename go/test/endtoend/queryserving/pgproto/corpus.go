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

package pgproto

import (
	"fmt"
	"os"
	"path/filepath"
)

// The pgproto corpus lives in-tree under testdata/ as hand-written .pgproto
// data files. Unlike sqllogictest there is no large upstream corpus to mirror:
// each file is a deliberately-chosen sequence of raw frontend protocol messages
// (see the package README for the data-file grammar). The corpus grows by
// committing new .pgproto files, not by bumping a pinned upstream SHA.
const (
	// DefaultCorpusDir is the in-tree corpus, relative to the package dir
	// (Go runs tests with cwd set to the package directory). Override with
	// PGPROTO_CORPUS_DIR to point at an external set of data files.
	DefaultCorpusDir = "testdata"

	// DefaultCorpusGlob matches every data file in the corpus. Override with
	// PGPROTO_CORPUS_GLOB to scope which files run during iteration.
	DefaultCorpusGlob = "**/*.pgproto"

	// PgprotoRepoURL is the upstream tool repository.
	PgprotoRepoURL = "https://github.com/tatsuo-ishii/pgproto"

	// PgprotoCommit pins the pgproto tool revision built by `make tools`. It
	// mirrors PGPROTO_VER in the Makefile and is recorded in results.json so a
	// recorded baseline is tied to the exact tool that produced it. Keep these
	// two in sync when bumping the tool.
	PgprotoCommit = "fa08c9c96df9ca514cd19aa7f587e27c7ac63160"
)

// resolveCorpusDir returns the absolute directory containing the corpus to run.
// Defaults to the in-tree testdata/ directory; PGPROTO_CORPUS_DIR overrides it.
func resolveCorpusDir() (string, error) {
	dir := os.Getenv("PGPROTO_CORPUS_DIR")
	if dir == "" {
		dir = DefaultCorpusDir
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve corpus dir %q: %w", dir, err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		return "", fmt.Errorf("corpus dir %q is not a directory", abs)
	}
	return abs, nil
}
