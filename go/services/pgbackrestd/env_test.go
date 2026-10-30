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

package pgbackrestd

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	dataVolume := t.TempDir()
	return Config{
		WALSocket:          "/run/pgbackrest-wal/wal.sock",
		ControlSocket:      "/run/pgbackrest-control/control.sock",
		RestorePIDFile:     "/var/lib/pooler/restore.pid",
		StateDir:           t.TempDir(),
		DataVolume:         dataVolume,
		PGData:             dataVolume + "/pg_data",
		PGSocketDir:        dataVolume + "/pg_sockets",
		TLSCertDir:         "/certs/pgbackrest",
		LocationType:       LocationS3,
		S3Bucket:           "b",
		S3Region:           "r",
		S3CredSource:       CredStatic,
		S3StaticKeyDir:     "/secrets/static",
		CipherKeyFile:      "/secrets/cipher",
		ServerPort:         8432,
		SandboxAnchor:      AnchorDirFlag,
		MaxFetches:         2,
		ArchiveGetQueueMax: "256MiB",
	}
}

// Generations 3 and 7 sit on repositories 1 and 2, so a mix-up of generation
// and repository number shows.
var testSecrets = Secrets{
	RepoSnapshot: RepoSnapshot{
		Repos:      map[uint32]uint32{1: 3, 2: 7},
		CipherPass: map[uint32]string{3: "cipher-three", 7: "cipher-seven"},
		StaticKeys: map[uint32]StaticKey{
			1: {ID: "AKIASTATIC", Secret: "static-secret-value"},
			2: {ID: "AKIASTATIC", Secret: "static-secret-value"},
		},
	},
	CredsToken: "tok-123",
}

func TestEnvFor_S3CommandHoldsTokenAndSecrets(t *testing.T) {
	cfg := testConfig(t)
	env := envFor(KindS3Command, cfg, testSecrets)
	assert.Contains(t, env, "PGBACKRESTD_CREDS_TOKEN=tok-123")
	assert.Contains(t, env, "PGBACKREST_REPO1_CIPHER_PASS=cipher-three")
	assert.Contains(t, env, "PGBACKREST_REPO2_CIPHER_PASS=cipher-seven")
	for _, e := range env {
		assert.NotContains(t, e, "REPO3_", "generation numbers must not be used as repository numbers")
		assert.NotContains(t, e, "REPO7_")
	}
	assert.Contains(t, env, "PGBACKREST_REPO1_S3_KEY=AKIASTATIC")
	assert.Contains(t, env, "PGBACKREST_REPO2_S3_KEY_SECRET=static-secret-value")
	assert.Contains(t, env, "PGPASSFILE="+cfg.StateDir+"/pgpass")
	assert.Contains(t, env, "HOME="+cfg.StateDir)

	// An unencrypted generation emits no passphrase, and a repository with
	// no static key gets no S3 key.
	snap := testSecrets
	snap.CipherPass = map[uint32]string{3: "", 7: "cipher-seven"}
	snap.StaticKeys = map[uint32]StaticKey{2: {ID: "K2", Secret: "S2"}}
	env = envFor(KindS3Command, cfg, snap)
	assert.NotContains(t, strings.Join(env, "\n"), "REPO1_CIPHER_PASS")
	assert.Contains(t, env, "PGBACKREST_REPO2_S3_KEY=K2")
	assert.NotContains(t, strings.Join(env, "\n"), "REPO1_S3_KEY")
}

func TestEnvFor_ServerHasNoToken(t *testing.T) {
	for _, e := range envFor(KindServer, testConfig(t), testSecrets) {
		assert.NotContains(t, e, "PGBACKRESTD_CREDS_TOKEN")
		assert.NotContains(t, e, "tok-123")
	}
}

func TestEnvFor_ServerHasNoRepoSecret(t *testing.T) {
	cfg := testConfig(t)
	env := envFor(KindServer, cfg, testSecrets)
	for _, e := range env {
		assert.NotContains(t, e, "CIPHER_PASS")
		assert.NotContains(t, e, "S3_KEY")
		assert.NotContains(t, e, "cipher-three")
		assert.NotContains(t, e, "static-secret-value")
	}
	assert.Contains(t, env, "PGBACKREST_CONFIG="+cfg.StateDir+"/conf/pgbackrest-server.conf")
}

func TestEnvFor_NoAWSFromParent(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/x")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token")
	t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/var/run/secrets/pod")
	for _, kind := range []Kind{KindS3Command, KindServer} {
		for _, e := range envFor(kind, testConfig(t), testSecrets) {
			assert.False(t, strings.HasPrefix(e, "AWS_"), "leaked %q", e)
			assert.NotContains(t, e, "arn:aws")
		}
	}
}

func TestEnvFor_SeparatorsOnly(t *testing.T) {
	t.Setenv("SOME_PARENT_VAR", "x")
	for _, kind := range []Kind{KindS3Command, KindServer} {
		for _, e := range envFor(kind, testConfig(t), testSecrets) {
			assert.NotContains(t, e, "SOME_PARENT_VAR")
		}
	}
}

func TestEnvFor_Sorted(t *testing.T) {
	for _, kind := range []Kind{KindS3Command, KindServer} {
		env := envFor(kind, testConfig(t), testSecrets)
		require.NotEmpty(t, env)
		assert.True(t, slices.IsSorted(env), "%v", env)
	}
}

// Only commands that read or write the repository hold the token, the cipher
// passphrases and the static S3 keys; stop, start and version hold none. Every
// command in the allowlist goes through the real classification.
func TestEnvFor_PerCommandSecrets(t *testing.T) {
	cfg := testConfig(t)
	local := map[string]bool{cmdStop: true, cmdStart: true, cmdVersion: true}
	for _, command := range s3Commands {
		env := envFor(envKind(KindS3Command, command), cfg, testSecrets)
		joined := strings.Join(env, "\n")
		secret := []string{"PGBACKRESTD_CREDS_TOKEN", "tok-123", "CIPHER_PASS", "cipher-three", "cipher-seven", "S3_KEY", "AKIASTATIC", "static-secret-value"}
		for _, s := range secret {
			if local[command] {
				assert.NotContains(t, joined, s, "%s must not hold %s", command, s)
			} else {
				assert.Contains(t, joined, s, "%s reads the repository and needs %s", command, s)
			}
		}
		// Every command, secret or not, keeps the base environment.
		assert.Contains(t, env, "PGPASSFILE="+cfg.StateDir+"/pgpass", command)
		assert.Contains(t, env, "HOME="+cfg.StateDir, command)
		assert.True(t, slices.IsSorted(env), command)
	}
	assert.Equal(t, KindServer, envKind(KindServer, cmdServer), "the server keeps its own class")
}

// The environment a stop child really gets, through the runner.
func TestRunChild_LocalCommandsGetNoSecrets(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "env")
	script := "#!/bin/sh\nenv >> " + out + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := newTestServer(t, nil)
	s.setRepoSnapshot(testSecrets.RepoSnapshot)

	for _, command := range []string{cmdStop, cmdStart, cmdVersion} {
		require.NoError(t, os.RemoveAll(out))
		code, err := s.runChild(t.Context(), KindS3Command, command, nil, io.Discard, io.Discard)
		require.NoError(t, err)
		require.Equal(t, 0, code)
		raw, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "PGBACKRESTD_CREDS_TOKEN", command)
		assert.NotContains(t, string(raw), "CIPHER_PASS", command)
		assert.NotContains(t, string(raw), "S3_KEY", command)
	}
	require.NoError(t, os.RemoveAll(out))
	code, err := s.runChild(t.Context(), KindS3Command, cmdInfo, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 0, code)
	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "PGBACKRESTD_CREDS_TOKEN=", "info reads the repository")
	assert.Contains(t, string(raw), "PGBACKREST_REPO1_CIPHER_PASS=cipher-three")
}
