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

// Package postgresttests runs PostgREST's own upstream hspec test suite with
// PostgREST pointed at the multigateway, to find behavioural divergences on the
// proxied path. PostgREST is a plain libpq client, so it is repointed at the
// gateway with a connection-string change; the suite (test:spec) connects purely
// via libpq env, so this harness provides the database (a multigres cluster),
// loads PostgREST's fixtures itself, and runs the compiled suite from a pinned
// Docker image. See README.md for the design.
package postgresttests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/tools/executil"
)

const (
	// postgrestRepo is the upstream PostgREST source we clone to build the
	// test-suite image and load fixtures from.
	postgrestRepo = "https://github.com/PostgREST/postgrest"

	// postgrestTag pins the PostgREST version. The suite, its fixtures, and the
	// image are all built from this tag so pass-rate tracking is meaningful.
	// Bump deliberately (re-baseline the divergence report when you do).
	postgrestTag = "v14.16"

	// specImage is the local tag of the compiled test-suite image. Includes the
	// PostgREST tag so a version bump builds a fresh image rather than reusing a
	// stale one.
	specImage = "postgrest-spec:" + postgrestTag

	defaultCacheRoot = "/tmp/multigres_postgrest_cache"
)

// resolvePostgrestSource returns a directory containing a PostgREST checkout at
// postgrestTag. Default behaviour shallow-clones the pinned tag into the cache
// (reused across runs). Override with POSTGREST_SRC_DIR to point at a local
// checkout — useful for iterating on fixtures or the Dockerfile.
func resolvePostgrestSource(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()

	if override := os.Getenv("POSTGREST_SRC_DIR"); override != "" {
		abs, err := filepath.Abs(override)
		if err != nil {
			return "", fmt.Errorf("resolve POSTGREST_SRC_DIR=%q: %w", override, err)
		}
		if info, err := os.Stat(abs); err != nil || !info.IsDir() {
			return "", fmt.Errorf("POSTGREST_SRC_DIR=%q is not a directory", override)
		}
		t.Logf("Using POSTGREST_SRC_DIR override: %s", abs)
		return abs, nil
	}
	return ensurePostgrestSource(t, ctx)
}

// ensurePostgrestSource clones the pinned tag if the cache is missing or points
// at a different ref. Returns the absolute checkout directory.
func ensurePostgrestSource(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()

	cacheRoot := os.Getenv("POSTGREST_CACHE_DIR")
	if cacheRoot == "" {
		cacheRoot = defaultCacheRoot
	}
	dir := filepath.Join(cacheRoot, "source", "postgrest-"+postgrestTag)

	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		// Confirm the checkout is at the pinned tag; the tag ref resolves to a
		// commit, so compare against the tag's commit.
		want, werr := executil.Command(ctx, "git", "-C", dir, "rev-list", "-n", "1", postgrestTag).Output()
		head, herr := executil.Command(ctx, "git", "-C", dir, "rev-parse", "HEAD").Output()
		if werr == nil && herr == nil && strings.TrimSpace(string(want)) == strings.TrimSpace(string(head)) {
			t.Logf("Using cached PostgREST source at %s (%s)", dir, postgrestTag)
			return dir, nil
		}
		t.Logf("Cached PostgREST source at %s does not match %s; re-cloning", dir, postgrestTag)
		if err := os.RemoveAll(dir); err != nil {
			return "", fmt.Errorf("remove stale source: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", fmt.Errorf("mkdir source parent: %w", err)
	}

	t.Logf("Cloning PostgREST %s from %s ...", postgrestTag, postgrestRepo)
	var cloneStderr bytes.Buffer
	cloneCmd := executil.Command(ctx, "git", "clone",
		"--depth", "1",
		"--branch", postgrestTag,
		postgrestRepo,
		dir,
	)
	cloneCmd.Stderr = &cloneStderr
	if err := cloneCmd.Run(); err != nil {
		return "", fmt.Errorf("clone PostgREST: %w (stderr: %s)", err, cloneStderr.String())
	}

	t.Logf("PostgREST source ready at %s (%s)", dir, postgrestTag)
	return dir, nil
}

// ensureSpecImage builds the compiled test-suite image from srcDir if it is not
// already present locally. Building compiles GHC deps + PostgREST + test:spec
// (~8 min cold, cached thereafter), so an existing image is reused. Set
// POSTGREST_REBUILD_IMAGE=1 to force a rebuild.
func ensureSpecImage(t *testing.T, ctx context.Context, srcDir string) error {
	t.Helper()

	if os.Getenv("POSTGREST_REBUILD_IMAGE") != "1" {
		if imageExists(ctx, specImage) {
			t.Logf("Using existing spec image %s", specImage)
			return nil
		}
	}

	dockerfile := dockerfilePath()
	if _, err := os.Stat(dockerfile); err != nil {
		return fmt.Errorf("spec Dockerfile not found at %s: %w", dockerfile, err)
	}

	t.Logf("Building spec image %s from %s (first build compiles PostgREST; ~8 min)...", specImage, srcDir)
	var buildOut bytes.Buffer
	build := executil.Command(ctx, "docker", "build",
		"-f", dockerfile,
		"-t", specImage,
		srcDir,
	)
	build.Stdout = &buildOut
	build.Stderr = &buildOut
	if err := build.Run(); err != nil {
		return fmt.Errorf("docker build spec image: %w\n%s", err, tail(buildOut.String(), 40))
	}
	t.Logf("Spec image %s built", specImage)
	return nil
}

// imageExists reports whether a local docker image with the given tag exists.
func imageExists(ctx context.Context, image string) bool {
	out, err := executil.Command(ctx, "docker", "image", "inspect", image).Output()
	return err == nil && len(out) > 0
}

// dockerfilePath returns the absolute path to the vendored spec Dockerfile,
// resolved via runtime.Caller so it is correct regardless of the working dir.
func dockerfilePath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "Dockerfile.spec")
}

// tail returns the last n lines of s (for trimming long build logs in errors).
func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
