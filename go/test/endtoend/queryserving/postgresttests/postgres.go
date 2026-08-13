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

package postgresttests

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/tools/executil"
)

// postgisTag pins the PostGIS version built when the system PostgreSQL doesn't
// already provide it (macOS local dev). On Linux CI postgis comes from the
// distro package (postgresql-17-postgis-3), so this build is skipped.
const postgisTag = "3.6.3"

// requiredExtensions are the extensions PostgREST's fixtures load. All except
// postgis ship with the server/contrib packages; postgis is handled separately.
var requiredExtensions = []string{"pgcrypto", "ltree", "isn", "file_fdw"}

// ensurePostgres locates a system PostgreSQL 17 install, makes sure PostGIS is
// available in it, and prepends its bin directory to PATH so pgctld (via
// shardsetup) and host-side psql use it. It returns the install prefix so the
// direct-baseline arm can start a standalone off the same binaries.
//
// Unlike the pgregress suite, this does NOT build PostgreSQL from source — the
// PostgREST suite only needs a working PG 17 with the fixture extensions, which
// the packaged server + contrib already provide (matching how the repo's other
// integration tests provision PostgreSQL via setup-test-environment.sh). Only
// PostGIS may be missing locally; it is built into the system PG on demand.
func ensurePostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	binDir := resolvePgBinDir(t)
	prefix := filepath.Dir(binDir)

	// Confirm it's PostgreSQL 17 (multigres requires major 17).
	if ver, err := exec.Command(filepath.Join(binDir, "pg_config"), "--version").Output(); err != nil {
		t.Skipf("pg_config not usable at %s: %v", binDir, err)
	} else if !strings.Contains(string(ver), "PostgreSQL 17") {
		t.Skipf("need PostgreSQL 17, found %q at %s", strings.TrimSpace(string(ver)), binDir)
	}

	// Prepend to PATH first so the postgis build and the cluster use this PG.
	if err := os.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		t.Fatalf("set PATH: %v", err)
	}

	verifyExtensionsAvailable(t, binDir)
	ensurePostGIS(t, ctx, binDir, prefix)

	t.Logf("Using system PostgreSQL 17 at %s", prefix)
	return prefix
}

// resolvePgBinDir returns the bin directory of the PostgreSQL 17 to use.
// Order: POSTGREST_PG_BINDIR override, then platform defaults (homebrew
// postgresql@17 on macOS, PGDG /usr/lib/postgresql/17/bin on Linux), then a
// pg_config already on PATH.
func resolvePgBinDir(t *testing.T) string {
	t.Helper()

	if d := os.Getenv("POSTGREST_PG_BINDIR"); d != "" {
		if _, err := os.Stat(filepath.Join(d, "initdb")); err != nil {
			t.Skipf("POSTGREST_PG_BINDIR=%s has no initdb: %v", d, err)
		}
		return d
	}
	if runtime.GOOS == "darwin" {
		if p := brewPrefix("postgresql@17"); p != "" {
			return filepath.Join(p, "bin")
		}
	} else {
		if d := "/usr/lib/postgresql/17/bin"; fileExists(filepath.Join(d, "initdb")) {
			return d
		}
	}
	if pgc, err := exec.LookPath("pg_config"); err == nil {
		if out, err := exec.Command(pgc, "--bindir").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	t.Skip("PostgreSQL 17 not found: set POSTGREST_PG_BINDIR, or install postgresql@17 (brew) / postgresql-17 (apt)")
	return ""
}

// verifyExtensionsAvailable skips the test with an actionable message if a
// required non-postgis extension's control file is missing from the PG install.
func verifyExtensionsAvailable(t *testing.T, binDir string) {
	t.Helper()
	shareDir := pgShareDir(t, binDir)
	for _, ext := range requiredExtensions {
		if !fileExists(filepath.Join(shareDir, "extension", ext+".control")) {
			t.Skipf("extension %q missing from %s/extension — install postgresql-contrib-17 (apt) or postgresql@17 (brew)", ext, shareDir)
		}
	}
}

// ensurePostGIS makes sure postgis is installed in the system PG. If its control
// file is already present (Linux distro package, or a prior local build), nothing
// happens. Otherwise it builds postgis from source against this PG's pg_config —
// the fixtures require it and PG17 loads extensions only from the server's own
// extension dir, so it must live there.
func ensurePostGIS(t *testing.T, ctx context.Context, binDir, prefix string) {
	t.Helper()
	shareDir := pgShareDir(t, binDir)
	if fileExists(filepath.Join(shareDir, "extension", "postgis.control")) {
		return
	}

	t.Logf("PostGIS not found in %s/extension; building it into the system PostgreSQL (one-time)...", shareDir)
	if err := pgbuilder.CheckBuildDependencies(t); err != nil {
		t.Skipf("cannot build PostGIS (missing build tools): %v", err)
	}
	applyDarwinBuildEnv()

	// Point a Builder at the system install so InstallExternalExtension's
	// `make install` (driven by this PG's pg_config) lands postgis in the
	// server's own lib/share dirs. Clean any partial clone from a prior aborted
	// run so the shallow clone doesn't fail on a non-empty directory.
	externalDir := filepath.Join(cacheRoot(), "external")
	_ = os.RemoveAll(filepath.Join(externalDir, "postgis"))
	b := &pgbuilder.Builder{
		InstallDir:  prefix,
		OutputDir:   t.TempDir(),
		ExternalDir: externalDir,
	}
	if _, err := b.InstallExternalExtension(t, ctx, postgisSpec()); err != nil {
		t.Fatalf("install PostGIS into system PostgreSQL: %v", err)
	}
}

// postgisSpec pins the PostGIS source build (full build — disabling components
// triggers a parallel-make race in 3.6's upgrade-SQL generation). On macOS the
// keg-only geos-config/proj are pointed at explicitly; Linux finds them by default.
func postgisSpec() pgbuilder.ExtensionBuildSpec {
	var args []string
	if runtime.GOOS == "darwin" {
		if gc := lookPathOrEmpty("geos-config"); gc != "" {
			args = append(args, "--with-geosconfig="+gc)
		}
		if proj := brewPrefix("proj"); proj != "" {
			args = append(args, "--with-projdir="+proj)
		}
	}
	args = append(args, strings.Fields(os.Getenv("POSTGREST_POSTGIS_CONFIGURE_ARGS"))...)
	return pgbuilder.ExtensionBuildSpec{
		Name:          "postgis",
		Repo:          "https://github.com/postgis/postgis",
		Tag:           postgisTag,
		BuildSystem:   "postgis",
		ConfigureArgs: args,
	}
}

// applyDarwinBuildEnv extends PKG_CONFIG_PATH so the PostGIS configure step finds
// homebrew's keg-only json-c/proj/protobuf-c/gdal .pc files. No-op off macOS.
func applyDarwinBuildEnv() {
	if runtime.GOOS != "darwin" {
		return
	}
	var paths []string
	for _, keg := range []string{"json-c", "proj", "geos", "protobuf-c", "gdal"} {
		if p := brewPrefix(keg); p != "" {
			paths = append(paths, filepath.Join(p, "lib", "pkgconfig"))
		}
	}
	if existing := os.Getenv("PKG_CONFIG_PATH"); existing != "" {
		paths = append(paths, existing)
	}
	if len(paths) > 0 {
		_ = os.Setenv("PKG_CONFIG_PATH", strings.Join(paths, ":"))
	}

	// Homebrew's PostgreSQL is built with NLS, so its server c.h includes
	// <libintl.h>; building an extension against it needs gettext's (keg-only)
	// headers and lib on the compile/link path.
	if gt := brewPrefix("gettext"); gt != "" {
		appendEnv("CPPFLAGS", "-I"+filepath.Join(gt, "include"))
		appendEnv("LDFLAGS", "-L"+filepath.Join(gt, "lib"))
	}
}

// appendEnv appends val to a space-separated environment variable.
func appendEnv(key, val string) {
	if existing := os.Getenv(key); existing != "" {
		val = existing + " " + val
	}
	_ = os.Setenv(key, val)
}

// pgShareDir returns `pg_config --sharedir` for the PG at binDir.
func pgShareDir(t *testing.T, binDir string) string {
	t.Helper()
	out, err := executil.Command(context.Background(), filepath.Join(binDir, "pg_config"), "--sharedir").Output()
	if err != nil {
		t.Fatalf("pg_config --sharedir: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// cacheRoot is the shared on-disk cache dir for this suite's downloads/builds.
func cacheRoot() string {
	if d := os.Getenv("POSTGREST_CACHE_DIR"); d != "" {
		return d
	}
	return defaultCacheRoot
}
