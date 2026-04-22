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

package sqllogictest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/tools/executil"
)

// Upstream sqllogictest corpus. The original is hosted in the SQLite fossil
// repository at https://www.sqlite.org/sqllogictest/ and carries an explicit
// multi-license (GPL / BSD / MIT / CC0 — "use whichever applies best") per
// its COPYRIGHT.md. gregrahn/sqllogictest is an actively-maintained git
// mirror of that fossil repo; pinning against it lets us shallow-clone via
// plain git with SHA-verified reproducibility.
//
// We consume this corpus under the MIT license per the upstream offer.
const (
	// CorpusRepoURL points at the git mirror we fetch from.
	CorpusRepoURL = "https://github.com/gregrahn/sqllogictest"

	// CorpusCommit pins the corpus at a specific revision so pass-rate
	// tracking over time is meaningful (the corpus contents don't drift
	// out from under the recorded baseline).
	CorpusCommit = "c67f97bf3ca7e590d12e073408bcacaf2ff0f3a0"

	// DefaultCorpusGlob matches every .test file in the upstream corpus.
	// Callers override via SLT_CORPUS_GLOB when they want to run a targeted
	// subset (useful for iteration).
	DefaultCorpusGlob = "test/**/*.test"

	defaultCacheRoot = "/tmp/multigres_slt_cache"
)

// resolveCorpusDir returns the directory containing the corpus to run.
//
// Default behaviour shallow-clones the pinned upstream at CorpusCommit into
// $SLT_CACHE_DIR/source/sqllogictest (reusing the cache across runs).
// Override with SLT_CORPUS_DIR to point at any local directory — useful
// for iterating against a subset or against an internal fork.
func resolveCorpusDir(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()

	if override := os.Getenv("SLT_CORPUS_DIR"); override != "" {
		abs, err := filepath.Abs(override)
		if err != nil {
			return "", fmt.Errorf("resolve SLT_CORPUS_DIR=%q: %w", override, err)
		}
		if info, err := os.Stat(abs); err != nil || !info.IsDir() {
			return "", fmt.Errorf("SLT_CORPUS_DIR=%q is not a directory", override)
		}
		t.Logf("Using SLT_CORPUS_DIR override: %s", abs)
		return abs, nil
	}
	return ensureUpstreamCorpus(t, ctx)
}

// ensureUpstreamCorpus clones the pinned corpus if the cache is missing or
// points at a different SHA. Returns the absolute directory.
func ensureUpstreamCorpus(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()

	cacheRoot := os.Getenv("SLT_CACHE_DIR")
	if cacheRoot == "" {
		cacheRoot = defaultCacheRoot
	}
	dir := filepath.Join(cacheRoot, "source", "sqllogictest")

	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		head, err := executil.Command(ctx, "git", "-C", dir, "rev-parse", "HEAD").Output()
		if err == nil && strings.TrimSpace(string(head)) == CorpusCommit {
			t.Logf("Using cached sqllogictest corpus at %s (sha=%s)", dir, CorpusCommit)
			return dir, nil
		}
		t.Logf("Cached sqllogictest corpus at %s does not match pinned commit; re-cloning", dir)
		if err := os.RemoveAll(dir); err != nil {
			return "", fmt.Errorf("remove stale corpus: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", fmt.Errorf("mkdir corpus parent: %w", err)
	}

	t.Logf("Cloning sqllogictest corpus from %s (~30s)...", CorpusRepoURL)
	cloneCmd := executil.Command(ctx, "git", "clone",
		"--filter=blob:none",
		CorpusRepoURL,
		dir,
	)
	var stderr bytes.Buffer
	cloneCmd.Stderr = &stderr
	if err := cloneCmd.Run(); err != nil {
		return "", fmt.Errorf("clone corpus: %w (stderr: %s)", err, stderr.String())
	}

	checkoutCmd := executil.Command(ctx, "git", "-C", dir, "checkout", CorpusCommit)
	checkoutCmd.Stderr = &stderr
	if err := checkoutCmd.Run(); err != nil {
		return "", fmt.Errorf("checkout pinned corpus commit: %w (stderr: %s)", err, stderr.String())
	}

	t.Logf("sqllogictest corpus ready at %s (sha=%s)", dir, CorpusCommit)
	return dir, nil
}

// listCorpusFiles returns the .test / .slt files in the corpus directory
// matching SLT_CORPUS_GLOB (defaulting to DefaultCorpusGlob). Paths are
// absolute and sorted so per-file ordering is deterministic across runs.
//
// The glob uses doublestar semantics: "**" matches across path components.
func listCorpusFiles(corpusDir string) ([]string, error) {
	glob := os.Getenv("SLT_CORPUS_GLOB")
	if glob == "" {
		glob = DefaultCorpusGlob
	}

	files, err := doublestarGlob(corpusDir, glob)
	if err != nil {
		return nil, fmt.Errorf("glob %q in %s: %w", glob, corpusDir, err)
	}
	sort.Strings(files)
	return files, nil
}

// doublestarGlob handles "**" as match-across-components. The upstream
// corpus only nests a few levels, so filepath.WalkDir is both simple and
// fast enough.
func doublestarGlob(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		ok, matchErr := matchDoublestar(pattern, rel)
		if matchErr != nil {
			return matchErr
		}
		if ok {
			matches = append(matches, path)
		}
		return nil
	})
	return matches, err
}

// matchDoublestar implements "**" as match-any-number-of-path-segments.
// Falls back to filepath.Match for single-star segments.
func matchDoublestar(pattern, name string) (bool, error) {
	patSegs := strings.Split(pattern, "/")
	nameSegs := strings.Split(name, "/")
	return matchSegs(patSegs, nameSegs)
}

func matchSegs(pat, name []string) (bool, error) {
	for len(pat) > 0 {
		p := pat[0]
		if p == "**" {
			rest := pat[1:]
			if len(rest) == 0 {
				return true, nil
			}
			for i := 0; i <= len(name); i++ {
				ok, err := matchSegs(rest, name[i:])
				if err != nil {
					return false, err
				}
				if ok {
					return true, nil
				}
			}
			return false, nil
		}
		if len(name) == 0 {
			return false, nil
		}
		ok, err := filepath.Match(p, name[0])
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		pat = pat[1:]
		name = name[1:]
	}
	return len(name) == 0, nil
}
