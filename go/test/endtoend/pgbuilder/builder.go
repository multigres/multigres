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
	"errors"
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
	// ExternalDir is the per-invocation root under which external extensions
	// (KindExternal, e.g. pgvector) are cloned and built as PGXS modules. It is
	// per-run (under the same build root as BuildDir) so concurrent callers
	// don't share a checkout, and is removed by Cleanup along with the build.
	ExternalDir string
	// OutputDir is a persistent per-invocation directory for caller-written artifacts
	// (reports, diffs, etc.). pgbuilder itself does not write here.
	OutputDir string
	// ConfigureArgs are extra flags appended to ./configure. Callers set this
	// before Build to enable optional features that some contrib extensions
	// require (e.g. --with-uuid for uuid-ossp). Empty by default so existing
	// callers (regression/isolation, sqllogictest) build unchanged.
	ConfigureArgs []string
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
		SourceDir:   filepath.Join(cacheDir, "source", "postgres"),
		BuildDir:    filepath.Join(buildRoot, "build"),
		InstallDir:  filepath.Join(buildRoot, "install"),
		ExternalDir: filepath.Join(buildRoot, "external"),
		OutputDir:   filepath.Join(cacheDir, "results", timestamp),
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
	configureArgs := []string{
		"--prefix=" + b.InstallDir,
		"--enable-cassert=no",
		"--enable-tap-tests=no",
		"--without-icu",
	}
	configureArgs = append(configureArgs, b.ConfigureArgs...)
	configureCmd := executil.Command(ctx, filepath.Join(b.SourceDir, "configure"), configureArgs...)
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

// InstallContrib builds and installs all configured contrib modules into
// b.InstallDir. Must be called after Build().
//
// The top-level `make install` run by Build installs only the core server;
// contrib extensions (citext, hstore, cube, ...) ship their own
// .so/.control/.sql and must be installed separately before CREATE EXTENSION
// can load them. contrib/Makefile already skips modules whose optional
// dependencies (libxml, openssl, ...) were not enabled at configure time, so
// this installs only what the --without-icu configure produced.
func (b *Builder) InstallContrib(t *testing.T, ctx context.Context) error {
	t.Helper()

	contribDir := filepath.Join(b.BuildDir, "contrib")
	t.Logf("Building and installing contrib modules from %s...", contribDir)

	cmd := executil.Command(ctx, "make", "-C", contribDir, "-j", "4", "install")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("make -C contrib install failed: %w", err)
	}

	t.Logf("contrib modules installed to %s", b.InstallDir)
	return nil
}

// InstallContribModules builds and installs specific contrib modules by
// directory name (e.g. "citext"), rather than all of contrib. The external
// suite uses it to satisfy an extension's contrib dependencies — pg_graphql's
// tests CREATE EXTENSION citext — so an external-only run works without building
// all of contrib. Idempotent: re-running `make install` for a module is a no-op,
// so it is safe even when a full InstallContrib already ran.
func (b *Builder) InstallContribModules(t *testing.T, ctx context.Context, names []string) error {
	t.Helper()

	for _, name := range names {
		dir := filepath.Join(b.BuildDir, "contrib", name)
		t.Logf("Building and installing contrib module %s from %s...", name, dir)
		cmd := executil.Command(ctx, "make", "-C", dir, "-j", "4", "install")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("make -C contrib/%s install failed: %w", name, err)
		}
	}
	return nil
}

// ExtensionBuildSpec describes how to clone and build one external extension.
// It is pgbuilder's view of pgregresstest's ExternalExtension, defined here
// (rather than imported) because pgbuilder must not depend on pgregresstest.
type ExtensionBuildSpec struct {
	Name string
	Repo string
	Tag  string
	// BuildSubdir is the directory within the checkout holding the build entry
	// point (the PGXS Makefile, or the pgrx crate root), relative to the clone
	// root. Empty means the repo root (pgvector, pg_cron); set when it lives in a
	// subdirectory (pgmq: "pgmq-extension").
	BuildSubdir string
	// BuildSystem selects the toolchain: "" or "pgxs" builds with make against
	// PGXS; "pgrx" builds a Rust/pgrx crate with cargo-pgrx.
	BuildSystem string
	// PgrxVersion pins the cargo-pgrx CLI version for BuildSystem=="pgrx"; it must
	// match the crate's pinned pgrx dependency. Ignored for pgxs.
	PgrxVersion string
}

// PgMajorVersion returns the PostgreSQL major version as a string ("17"),
// derived from PostgresVersion (e.g. "REL_17_6"). pgrx uses it to form the
// per-version feature/init flag ("pg17").
func PgMajorVersion() string {
	v := strings.TrimPrefix(PostgresVersion, "REL_")
	major, _, _ := strings.Cut(v, "_")
	return major
}

// InstallExternalExtension clones an external extension's repo at a pinned tag
// into b.ExternalDir/<name>, builds it against this builder's from-source
// PostgreSQL, and installs it into b.InstallDir. It returns the checkout
// directory so the caller can drive the extension's shipped pg_regress suite
// (sql/ + expected/) from there. Must be called after Build().
//
// The build is pointed at the per-run install tree (via PG_CONFIG for PGXS, or
// --pg-config for pgrx) so the extension's .so links against the exact
// PostgreSQL the cluster runs — the same ABI-consistency guarantee Build
// provides for regress.so. The checkout is per-run, so a shallow clone happens
// once per invocation; pinning the tag keeps the suite reproducible.
func (b *Builder) InstallExternalExtension(t *testing.T, ctx context.Context, spec ExtensionBuildSpec) (string, error) {
	t.Helper()

	if err := os.MkdirAll(b.ExternalDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create external dir: %w", err)
	}
	cloneDir := filepath.Join(b.ExternalDir, spec.Name)

	t.Logf("Cloning external extension %s (%s) from %s...", spec.Name, spec.Tag, spec.Repo)
	clone := executil.Command(ctx, "git", "clone",
		"--depth=1",
		"--branch", spec.Tag,
		spec.Repo,
		cloneDir)
	var stderr bytes.Buffer
	clone.Stderr = &stderr
	if err := clone.Run(); err != nil {
		return "", fmt.Errorf("failed to clone %s: %w (stderr: %s)", spec.Name, err, stderr.String())
	}

	pgConfig := filepath.Join(b.BinDir(), "pg_config")
	// The build entry point may live in a subdirectory of the checkout (pgmq).
	buildDir := filepath.Join(cloneDir, spec.BuildSubdir)

	switch spec.BuildSystem {
	case "", "pgxs":
		if err := b.installPGXSExtension(t, ctx, spec.Name, buildDir, pgConfig); err != nil {
			return "", err
		}
	case "pgrx":
		if err := b.installPgrxExtension(t, ctx, spec, buildDir, pgConfig); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("external extension %s: unknown build system %q", spec.Name, spec.BuildSystem)
	}

	t.Logf("External extension %s installed", spec.Name)
	return cloneDir, nil
}

// installPGXSExtension builds and installs a standard PGXS (make) extension into
// the install tree pgConfig points at.
func (b *Builder) installPGXSExtension(t *testing.T, ctx context.Context, name, buildDir, pgConfig string) error {
	t.Logf("Building external extension %s with PGXS (PG_CONFIG=%s)...", name, pgConfig)
	makeCmd := executil.Command(ctx, "make", "-C", buildDir, "-j", "4", "PG_CONFIG="+pgConfig)
	makeCmd.Stdout = os.Stdout
	makeCmd.Stderr = os.Stderr
	if err := makeCmd.Run(); err != nil {
		return fmt.Errorf("make %s failed: %w", name, err)
	}

	t.Logf("Installing external extension %s into %s...", name, b.InstallDir)
	installCmd := executil.Command(ctx, "make", "-C", buildDir, "PG_CONFIG="+pgConfig, "install")
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	if err := installCmd.Run(); err != nil {
		return fmt.Errorf("make %s install failed: %w", name, err)
	}
	return nil
}

// installPgrxExtension builds and installs a Rust/pgrx extension with cargo-pgrx
// against this builder's from-source PostgreSQL. Unlike PGXS there is no make:
// cargo-pgrx compiles the crate and copies the .so/.control/.sql into the tree
// pgConfig points at (it also substitutes control-file placeholders like
// @CARGO_VERSION@, so the files must be produced by cargo-pgrx, not copied).
func (b *Builder) installPgrxExtension(t *testing.T, ctx context.Context, spec ExtensionBuildSpec, buildDir, pgConfig string) error {
	// pgrx selects the target server with a per-major-version feature/flag.
	feature := "pg" + PgMajorVersion()

	if err := b.ensureCargoPgrx(t, ctx, spec.PgrxVersion); err != nil {
		return err
	}

	// Register our from-source PostgreSQL with pgrx. Passing the pg_config path
	// (rather than "download") makes init adopt the existing install instead of
	// fetching and building its own, so it is fast and uses the exact server ABI.
	t.Logf("Registering PostgreSQL with pgrx (cargo pgrx init --%s %s)...", feature, pgConfig)
	initCmd := executil.Command(ctx, "cargo", "pgrx", "init", "--"+feature, pgConfig)
	initCmd.Stdout = os.Stdout
	initCmd.Stderr = os.Stderr
	if err := initCmd.Run(); err != nil {
		return fmt.Errorf("cargo pgrx init %s failed: %w", spec.Name, err)
	}

	// Build and install. --no-default-features --features <pgNN> overrides the
	// crate's default (often the latest major) so it builds against our server;
	// --pg-config installs into that server's pkglibdir/sharedir.
	t.Logf("Building external extension %s with cargo-pgrx (--features %s, --pg-config %s)...", spec.Name, feature, pgConfig)
	installCmd := executil.Command(ctx, "cargo", "pgrx", "install",
		"--release",
		"--no-default-features",
		"--features", feature,
		"--pg-config", pgConfig).SetDir(buildDir)
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	if err := installCmd.Run(); err != nil {
		return fmt.Errorf("cargo pgrx install %s failed: %w", spec.Name, err)
	}
	return nil
}

// ensureCargoPgrx installs the cargo-pgrx CLI at the pinned version unless it is
// already present, so the build matches the crate's pgrx dependency exactly.
// Installing it compiles the CLI (minutes), so the already-installed fast path
// matters for repeated local runs.
func (b *Builder) ensureCargoPgrx(t *testing.T, ctx context.Context, version string) error {
	if version == "" {
		return errors.New("pgrx build requires a pinned PgrxVersion")
	}
	if out, err := executil.Command(ctx, "cargo", "pgrx", "--version").CombinedOutput(); err == nil &&
		strings.Contains(string(out), "cargo-pgrx "+version) {
		t.Logf("cargo-pgrx %s already installed", version)
		return nil
	}
	t.Logf("Installing cargo-pgrx %s (compiles the CLI, may take several minutes)...", version)
	cmd := executil.Command(ctx, "cargo", "install", "cargo-pgrx", "--version", version, "--locked")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cargo install cargo-pgrx %s failed: %w", version, err)
	}
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
