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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Corpus discovery shared by the differential query-serving suites (pgproto,
// sqllogictest): walk a corpus directory and return the files matching a glob.
// The glob env-var name and default differ per suite, so they are arguments.

// GlobToRegexp translates a shell glob into an anchored regexp. `**` matches
// across path separators, `*` matches within one segment, `?` matches a single
// non-`/` character. Regex metacharacters are escaped via regexp.QuoteMeta so
// regex syntax in the pattern stays literal.
//
// `a/**/b` also matches `a/b` (zero intermediate segments): a `/` immediately
// after `**` is consumed along with it.
func GlobToRegexp(pat string) (*regexp.Regexp, error) {
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

// ListCorpusFiles returns the files under corpusDir matching the glob from the
// named environment variable (falling back to defaultGlob when unset). Paths
// are absolute and sorted so per-file ordering is deterministic across runs.
//
// The glob uses doublestar semantics (see GlobToRegexp): "**" matches across
// path components, "*" matches within a single component, "?" matches a single
// non-/ char. Matching is against the corpus-relative, slash-separated path.
func ListCorpusFiles(corpusDir, globEnvVar, defaultGlob string) ([]string, error) {
	glob := os.Getenv(globEnvVar)
	if glob == "" {
		glob = defaultGlob
	}

	re, err := GlobToRegexp(glob)
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
