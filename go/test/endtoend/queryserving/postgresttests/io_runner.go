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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// ioImage is the local tag of the compiled io-suite image. Includes the
// PostgREST tag so a version bump builds a fresh image rather than reusing a
// stale one.
const ioImage = "postgrest-io:" + postgrestTag

// ensureIOImage builds the io-suite image from srcDir if it is not already
// present locally. The image only downloads a prebuilt postgrest binary and
// pip-installs the pytest deps (no Haskell build), so it's cheap (~1 min cold)
// and reused thereafter. Set POSTGREST_IO_REBUILD_IMAGE=1 to force a rebuild.
func ensureIOImage(t *testing.T, ctx context.Context, srcDir string) error {
	t.Helper()

	if os.Getenv("POSTGREST_IO_REBUILD_IMAGE") != "1" && imageExists(ctx, ioImage) {
		t.Logf("Using existing io image %s", ioImage)
		return nil
	}

	dockerfile := ioDockerfilePath()
	if _, err := os.Stat(dockerfile); err != nil {
		return fmt.Errorf("io Dockerfile not found at %s: %w", dockerfile, err)
	}

	t.Logf("Building io image %s from %s ...", ioImage, srcDir)
	var buildOut bytes.Buffer
	build := executil.Command(ctx, "docker", "build",
		"-f", dockerfile,
		// The image pins FROM --platform=linux/amd64 (static x86-64 postgrest
		// binary); build for that platform (emulated on Apple Silicon).
		"--platform", "linux/amd64",
		"-t", ioImage,
		srcDir,
	)
	build.Stdout = &buildOut
	build.Stderr = &buildOut
	if err := build.Run(); err != nil {
		return fmt.Errorf("docker build io image: %w\n%s", err, tail(buildOut.String(), 40))
	}
	t.Logf("io image %s built", ioImage)
	return nil
}

// runIO runs the selected pytest node ids (from ioTestArgs) against target,
// with each spawned postgrest repointed at target over TCP+password via the
// image's libpq-env shim. It returns the parsed per-test result. The container
// uses host networking and connects on 127.0.0.1 (like the hspec runner), so it
// reaches both the gateway and the standalone baseline, which bind 127.0.0.1.
func runIO(t *testing.T, ctx context.Context, target specTarget) (*specResult, error) {
	t.Helper()

	dbHost, netArgs := ioContainerNetwork()

	args := []string{"run", "--rm", "--platform", "linux/amd64"}
	args = append(args, netArgs...)
	args = append(args,
		// Connection identity the suite forwards into each postgrest subprocess
		// (test/io/conftest.py::baseenv reads exactly these three).
		"-e", "PGHOST="+dbHost,
		"-e", "PGUSER="+authenticatorRole,
		"-e", "PGDATABASE=postgres",
		// Connection extras the suite does NOT forward; the image's postgrest
		// shim sources these (see Dockerfile.io). All io login roles share
		// authenticatorPassword (set in loadIOFixtures), so one password serves
		// every per-test PGUSER override.
		"-e", "PGPORT="+strconv.Itoa(target.Port),
		"-e", "PGPASSWORD="+authenticatorPassword,
		"-e", "PGSSLMODE=disable",
		ioImage,
	)
	// pytest flags: verbose per-test lines (parsed by parsePytestOutput), short
	// tracebacks, no cache writes into the read-only source tree.
	args = append(args, ioTestArgs()...)
	args = append(args,
		"-v", "--color=no", "--tb=short", "-ra",
		"-p", "no:cacheprovider",
	)

	t.Logf("Running io suite against %s (%s:%d)...", target.Name, dbHost, target.Port)
	var outBuf bytes.Buffer
	cmd := executil.Command(ctx, "docker", args...)
	cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &outBuf)
	cmd.SetWaitDelay(10 * time.Second)

	runErr := cmd.Run()

	res := parsePytestOutput(outBuf.String())
	res.Target = target.Name
	// A non-zero exit is expected when tests fail; only treat it as a harness
	// error when we collected zero results (pytest crashed at collection — e.g.
	// a connection/auth failure through the gateway, or an import error).
	if res.Total == 0 {
		return res, fmt.Errorf("io suite produced no results against %s: %w", target.Name, runErr)
	}
	return res, nil
}

// ioContainerNetwork returns the DB host the containerized suite should connect
// to and the docker network flags to make it reachable.
//
//   - Linux (CI): the container shares the host net namespace (--network=host)
//     and reaches host-bound services on 127.0.0.1. This is the path the CI
//     job uses.
//   - macOS (Docker Desktop): --network=host does NOT expose the host's
//     loopback to the container (the container's "host" is the Docker VM), so
//     127.0.0.1 is unreachable. The multigateway PG listener binds 0.0.0.0
//     (init.go), so we instead use bridge networking and connect to
//     host.docker.internal, which Docker Desktop routes to the host. NOTE: the
//     direct-PostgreSQL baseline binds loopback only (pgbuilder) and is NOT
//     reachable this way — on macOS run the default gateway-only mode, not
//     POSTGREST_FULL_BASELINE.
//
// Override the host explicitly with POSTGREST_IO_DB_HOST.
func ioContainerNetwork() (dbHost string, netArgs []string) {
	dbHost = os.Getenv("POSTGREST_IO_DB_HOST")
	if dbHost == "" {
		if runtime.GOOS == "darwin" {
			dbHost = "host.docker.internal"
		} else {
			dbHost = "127.0.0.1"
		}
	}
	if dbHost == "host.docker.internal" {
		return dbHost, []string{"--add-host=host.docker.internal:host-gateway"}
	}
	return dbHost, []string{"--network=host"}
}

// ioDockerfilePath returns the absolute path to the vendored io Dockerfile.
func ioDockerfilePath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "Dockerfile.io")
}
