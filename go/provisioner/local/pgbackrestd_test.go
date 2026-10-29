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

package local

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func sidecarTestConfig(t *testing.T, loc *clustermetadatapb.BackupLocation) PgBackRestdConfig {
	t.Helper()
	pooler := filepath.Join(t.TempDir(), "pooler_x")
	return PgBackRestdConfig{
		PoolerDir:  pooler,
		Paths:      NewPgBackRestdPaths(pooler),
		PgPort:     5433,
		PgUser:     "postgres",
		Location:   loc,
		ServerPort: 8432,
		TLSCertDir: "/certs",
	}
}

func TestPgBackRestdPathsAreOutsideThePoolerDir(t *testing.T) {
	pooler := "/work/data/pooler_abc"
	p := NewPgBackRestdPaths(pooler)

	// pgbackrestd rejects a data volume that holds its state or sockets.
	for name, path := range map[string]string{
		"state": p.StateDir, "wal socket": p.WALSocket, "control socket": p.ControlSocket, "password": p.PasswordFile,
	} {
		rel, err := filepath.Rel(pooler, path)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(rel, ".."), "%s %s must not be inside %s", name, path, pooler)
	}
	assert.Equal(t, "/work/data/pgbackrestd-pooler_abc/run-wal/wal.sock", p.WALSocket)
	assert.Equal(t, "/work/data/pgbackrestd-pooler_abc/run-control/control.sock", p.ControlSocket)
}

func TestPgBackRestdPrepareRejectsOverlongSocketPath(t *testing.T) {
	long := filepath.Join(t.TempDir(), strings.Repeat("d", 120))
	err := NewPgBackRestdPaths(filepath.Join(long, "pooler")).Prepare("pw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "byte limit")
}

func TestPgBackRestdArgsFilesystem(t *testing.T) {
	c := sidecarTestConfig(t, &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_Filesystem{
		Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/repo"},
	}})
	c.CipherKeyFile = "/keys.json"

	args, err := c.BuildArgs()
	require.NoError(t, err)
	joined := " " + strings.Join(args, " ") + " "
	for _, want := range []string{
		" serve ",
		" --wal-socket " + c.Paths.WALSocket + " ",
		" --control-socket " + c.Paths.ControlSocket + " ",
		" --state-dir " + c.Paths.StateDir + " ",
		" --data-volume " + c.PoolerDir + " ",
		" --pg-port 5433 ",
		" --pg-user postgres ",
		" --pg-password-file " + c.Paths.PasswordFile + " ",
		" --pgdata " + filepath.Join(c.PoolerDir, "pg_data") + " ",
		" --pg-socket-dir " + filepath.Join(c.PoolerDir, "pg_sockets") + " ",
		" --restore-pidfile " + filepath.Join(c.PoolerDir, "restore_command.pid") + " ",
		" --location-type filesystem --fs-repo-path /repo ",
		" --server-port 8432 ",
		" --tls-cert-dir /certs ",
		" --cipher-key-file /keys.json ",
		" --sandbox-anchor=dir ",
	} {
		assert.Contains(t, joined, want)
	}
}

func TestPgBackRestdArgsCarrySandboxMode(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_Filesystem{
		Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/repo"},
	}}
	for mode, want := range map[string]string{
		"":                        "--sandbox=required",
		constants.SandboxRequired: "--sandbox=required",
		constants.SandboxAuto:     "--sandbox=auto",
		constants.SandboxOff:      "--sandbox=off",
	} {
		c := sidecarTestConfig(t, loc)
		c.Sandbox = mode
		args, err := c.BuildArgs()
		require.NoError(t, err)
		assert.Equal(t, 1, countOf(args, want), "mode %q: %v", mode, args)
	}
}

func countOf(args []string, want string) int {
	n := 0
	for _, a := range args {
		if a == want {
			n++
		}
	}
	return n
}

func TestSandboxModeFromEnvironment(t *testing.T) {
	t.Setenv(constants.PgBackRestSandboxEnvVar, "")
	mode, err := SandboxMode()
	require.NoError(t, err)
	assert.Equal(t, constants.SandboxRequired, mode)

	for _, want := range []string{constants.SandboxRequired, constants.SandboxAuto, constants.SandboxOff} {
		t.Setenv(constants.PgBackRestSandboxEnvVar, want)
		mode, err := SandboxMode()
		require.NoError(t, err)
		assert.Equal(t, want, mode)
	}

	t.Setenv(constants.PgBackRestSandboxEnvVar, "sometimes")
	_, err = SandboxMode()
	require.ErrorContains(t, err, constants.PgBackRestSandboxEnvVar)
}

func TestRequireSidecarHost(t *testing.T) {
	require.NoError(t, requireSidecarHost("linux", constants.SandboxRequired))
	for _, goos := range []string{"darwin", "windows", "freebsd"} {
		err := requireSidecarHost(goos, constants.SandboxRequired)
		require.Error(t, err, goos)
		assert.Equal(t, constants.SidecarHostRequirement, err.Error())
		assert.Contains(t, err.Error(), "--sandbox=auto")
		assert.Contains(t, err.Error(), constants.PgBackRestSandboxEnvVar)
		for _, mode := range []string{constants.SandboxAuto, constants.SandboxOff} {
			assert.NoError(t, requireSidecarHost(goos, mode), "%s %s", goos, mode)
		}
	}
}

func TestPgBackRestdArgsS3(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{
		S3: &clustermetadatapb.S3Backup{Bucket: "b", Region: "r", Endpoint: "https://e", KeyPrefix: "k"},
	}}

	t.Run("endpoint credentials", func(t *testing.T) {
		c := sidecarTestConfig(t, loc)
		c.S3CredEndpoint = "https://e/_session?ttl=10m"
		c.S3CAFile = "/ca.pem"
		args, err := c.BuildArgs()
		require.NoError(t, err)
		joined := " " + strings.Join(args, " ") + " "
		assert.Contains(t, joined, " --location-type s3 --s3-bucket b --s3-region r --s3-endpoint https://e --s3-key-prefix k ")
		assert.Contains(t, joined, " --s3-cred-source endpoint --s3-cred-endpoint https://e/_session?ttl=10m ")
		assert.Contains(t, joined, " --s3-cred-ca-file /ca.pem ")
	})

	t.Run("static keys", func(t *testing.T) {
		c := sidecarTestConfig(t, loc)
		c.S3StaticKeyDir = "/keys"
		args, err := c.BuildArgs()
		require.NoError(t, err)
		assert.Contains(t, " "+strings.Join(args, " ")+" ", " --s3-cred-source static --s3-static-key-dir /keys ")
	})
}

func TestPgBackRestdArgsRequireALocation(t *testing.T) {
	_, err := sidecarTestConfig(t, nil).BuildArgs()
	require.Error(t, err)
}

func TestMinigresGetsTheSidecarFlags(t *testing.T) {
	poolerDir := t.TempDir()
	args := minigresArgs(minigresLaunch{
		httpPort: 1, grpcPort: 2, pgPort: 3, etcdAddress: "e:1", topoGlobalRoot: "/r",
		cell: "zone1", database: "d", tableGroup: "default", shard: "0-inf",
		serviceID: "mg-1", pgctldAddr: "localhost:9", logLevel: "info", logFile: "/l", poolerDir: poolerDir,
	})
	paths := NewPgBackRestdPaths(poolerDir)
	joined := " " + strings.Join(args, " ") + " "
	for _, want := range []string{
		" --pgbackrest-control-socket " + paths.ControlSocket + " ",
		" --pgbackrest-wal-socket " + paths.WALSocket + " ",
		" --data-volume-root " + poolerDir + " ",
		" --data-volume-anchor dir ",
		" --pgctld-addr localhost:9 ",
	} {
		assert.Contains(t, joined, want)
	}
	// The multipooler half of a standalone cell gets the same sidecar flags.
	assert.Equal(t, sidecarPoolerArgs(poolerDir), args[len(args)-len(sidecarPoolerArgs(poolerDir)):])
}
