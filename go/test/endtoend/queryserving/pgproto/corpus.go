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
	"regexp"
	"sort"
	"strings"
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

// listCorpusFiles returns the data files in corpusDir matching
// PGPROTO_CORPUS_GLOB (defaulting to DefaultCorpusGlob). Paths are absolute and
// sorted so per-file ordering is deterministic across runs.
//
// The glob uses doublestar semantics: "**" matches across path components, "*"
// matches within a single component, "?" matches a single non-/ char.
func listCorpusFiles(corpusDir string) ([]string, error) {
	glob := os.Getenv("PGPROTO_CORPUS_GLOB")
	if glob == "" {
		glob = DefaultCorpusGlob
	}

	re, err := globToRegexp(glob)
	if err != nil {
		return nil, fmt.Errorf("compile glob %q: %w", glob, err)
	}

	var files []string
	walkErr := filepath.WalkDir(corpusDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(corpusDir, path)
		if relErr != nil {
			return relErr
		}
		if re.MatchString(filepath.ToSlash(rel)) {
			files = append(files, path)
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walk %s: %w", corpusDir, walkErr)
	}
	sort.Strings(files)
	return files, nil
}

// globToRegexp translates a shell glob into an anchored regexp. `**` matches
// across path separators, `*` matches within one segment, `?` matches a single
// non-`/` character. Regex metacharacters are escaped via regexp.QuoteMeta so
// regex syntax in the pattern stays literal.
//
// `a/**/b` also matches `a/b` (zero intermediate segments): a `/` immediately
// after `**` is consumed along with it.
func globToRegexp(pat string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.Grow(len(pat) + 4)
	b.WriteString(`\A`)
	for i := 0; i < len(pat); i++ {
		c := pat[i]
		switch c {
		case '*':
			if i+1 < len(pat) && pat[i+1] == '*' {
				b.WriteString(`.*`)
				i++
				if i+1 < len(pat) && pat[i+1] == '/' {
					i++
				}
			} else {
				b.WriteString(`[^/]*`)
			}
		case '?':
			b.WriteString(`[^/]`)
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString(`\z`)
	return regexp.Compile(b.String())
}
