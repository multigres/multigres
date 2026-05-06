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

// Package pgbuilder provides reusable helpers for checking out, building, and
// installing a pinned PostgreSQL source tree from git for end-to-end tests.
//
// Both the PostgreSQL regression/isolation harness (pgregresstest) and the
// sqllogictest differential harness (queryserving/sqllogictest) consume this
// package so they run against the same PostgreSQL version.
package pgbuilder

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

const (
	// PostgresGitRepo is the official PostgreSQL git repository.
	PostgresGitRepo = "https://github.com/postgres/postgres"

	// PostgresVersion is the git tag to checkout.
	PostgresVersion = "REL_17_6"

	// PostgresCacheDir is the default cache directory for PostgreSQL source and builds.
	PostgresCacheDir = "/tmp/multigres_pg_cache"
)

// Builder manages a PostgreSQL source checkout, an isolated build, and a
// per-test install prefix. It does not run any server processes itself;
// callers either invoke the built binaries directly or compose Builder with
// higher-level helpers (see Standalone in this package).
type Builder struct {
	// SourceDir is the shared source checkout, reused across runs.
	SourceDir string
	// BuildDir is the per-invocation build directory.
	BuildDir string
	// InstallDir is the per-invocation install prefix (contains bin/, lib/, share/).
	InstallDir string
	// OutputDir is a persistent per-invocation directory for caller-written artifacts
	// (reports, diffs, etc.). pgbuilder itself does not write here.
	OutputDir string
}

// New returns a Builder with unique per-invocation build and install directories
// rooted at $MULTIGRES_PG_CACHE_DIR (or /tmp/multigres_pg_cache when unset).
// Multiple concurrent callers get distinct build/install trees but share the
// source checkout.
func New(t *testing.T) *Builder {
	t.Helper()

	cacheDir := os.Getenv("MULTIGRES_PG_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = PostgresCacheDir
	}

	timestamp := time.Now().Format("20060102-150405.000000")
	buildRoot := filepath.Join(cacheDir, "builds", timestamp)

	return &Builder{
		SourceDir:  filepath.Join(cacheDir, "source", "postgres"),
		BuildDir:   filepath.Join(buildRoot, "build"),
		InstallDir: filepath.Join(buildRoot, "install"),
		OutputDir:  filepath.Join(cacheDir, "results", timestamp),
	}
}

// BinDir is the directory containing the built PostgreSQL binaries (postgres,
// initdb, psql, pg_ctl, ...).
func (b *Builder) BinDir() string {
	return filepath.Join(b.InstallDir, "bin")
}

// CheckBuildDependencies verifies that required C toolchain is available on
// the host. Callers that depend on building PostgreSQL from source should
// invoke this early and skip the test on a clear error message.
func CheckBuildDependencies(t *testing.T) error {
	t.Helper()

	required := []string{"make", "gcc"}
	var missing []string
	for _, tool := range required {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing build dependencies: %v. Install with: apt-get install build-essential", missing)
	}
	return nil
}

// EnsureSource ensures the pinned PostgreSQL source tree is available,
// cloning it if missing or wrong version. The source is shared across
// concurrent builders.
func (b *Builder) EnsureSource(t *testing.T, ctx context.Context) error {
	t.Helper()

	if _, err := os.Stat(b.SourceDir); err == nil {
		t.Logf("Found cached PostgreSQL source at %s, verifying version...", b.SourceDir)

		cmd := executil.Command(ctx, "git", "-C", b.SourceDir, "describe", "--tags", "--exact-match")
		output, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(output)) == PostgresVersion {
			t.Logf("Using cached PostgreSQL source (version %s)", PostgresVersion)
			return nil
		}

		t.Logf("Cached source version mismatch or invalid, re-cloning...")
		if err := os.RemoveAll(b.SourceDir); err != nil {
			return fmt.Errorf("failed to remove old cache: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(b.SourceDir), 0o755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	t.Logf("Cloning PostgreSQL %s from %s...", PostgresVersion, PostgresGitRepo)
	cmd := executil.Command(ctx, "git", "clone",
		"--depth=1",
		"--branch", PostgresVersion,
		PostgresGitRepo,
		b.SourceDir)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to clone PostgreSQL: %w (stderr: %s)", err, stderr.String())
	}

	t.Logf("Successfully cloned PostgreSQL %s", PostgresVersion)
	return nil
}

// Build runs ./configure + make + make install into b.InstallDir.
// ICU is disabled so the build does not require icu4c headers; that matches
// what the existing pgregresstest suite already does.
func (b *Builder) Build(t *testing.T, ctx context.Context) error {
	t.Helper()

	if err := os.MkdirAll(b.BuildDir, 0o755); err != nil {
		return fmt.Errorf("failed to create build directory: %w", err)
	}

	t.Logf("Configuring PostgreSQL with ./configure...")
	configureCmd := executil.Command(ctx, filepath.Join(b.SourceDir, "configure"),
		"--prefix="+b.InstallDir,
		"--enable-cassert=no",
		"--enable-tap-tests=no",
		"--without-icu",
	)
	configureCmd.Dir = b.BuildDir
	configureCmd.Stdout = os.Stdout
	configureCmd.Stderr = os.Stderr
	if err := configureCmd.Run(); err != nil {
		return fmt.Errorf("configure failed: %w", err)
	}

	t.Logf("Building PostgreSQL with make...")
	makeCmd := executil.Command(ctx, "make", "-j", "4")
	makeCmd.Dir = b.BuildDir
	makeCmd.Stdout = os.Stdout
	makeCmd.Stderr = os.Stderr
	if err := makeCmd.Run(); err != nil {
		return fmt.Errorf("make failed: %w", err)
	}

	t.Logf("Installing PostgreSQL to %s...", b.InstallDir)
	installCmd := executil.Command(ctx, "make", "install")
	installCmd.Dir = b.BuildDir
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	if err := installCmd.Run(); err != nil {
		return fmt.Errorf("make install failed: %w", err)
	}

	t.Logf("PostgreSQL build completed successfully")
	return nil
}

// Cleanup removes per-invocation build and install artifacts but leaves the
// shared source checkout in place so subsequent runs skip the clone.
func (b *Builder) Cleanup() {
	if b.BuildDir != "" {
		buildRoot := filepath.Dir(b.BuildDir)
		_ = os.RemoveAll(buildRoot)
	}
}
