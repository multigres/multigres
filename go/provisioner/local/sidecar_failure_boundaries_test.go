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
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSidecarCorruptOwnerBlocksAdoptionAndStop(t *testing.T) {
	for _, raw := range []string{"not json", `{}`, `{"pid":1,"created":1,"argv":["only-binary"]}`} {
		t.Run(raw, func(t *testing.T) {
			paths := ownerTestPaths(t)
			require.NoError(t, os.WriteFile(paths.ownerFile(), []byte(raw), 0o600))
			owner, err := paths.loadOwner()
			require.Error(t, err)
			require.Nil(t, owner)
			_, err = paths.validateAdoption(t.Context(), "/unused", []string{"serve"})
			require.Error(t, err)
			require.Error(t, paths.stopOwned(t.Context()))
			require.Error(t, paths.rejectLegacyOwnership(0))
			after, err := os.ReadFile(paths.ownerFile())
			require.NoError(t, err)
			require.Equal(t, raw, string(after), "unverified ownership must remain available for diagnosis")
		})
	}
}

func TestSidecarOwnerStorageFailuresAreNotAbsence(t *testing.T) {
	paths := ownerTestPaths(t)
	// A directory at the record path is a real I/O failure, not proof that no
	// sidecar owns the installation. Neither stop nor launch may ignore it.
	require.NoError(t, os.Mkdir(paths.ownerFile(), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(paths.ownerFile(), "keep"), []byte("evidence"), 0o600))
	_, err := paths.loadOwner()
	require.Error(t, err)
	require.Error(t, paths.removeOwner())
	require.Error(t, paths.saveOwner(&sidecarOwner{PID: 1, Created: 1, Argv: []string{"binary", "serve"}}))
	require.Error(t, paths.stopOwned(t.Context()))
	_, err = paths.ensureSidecar(t.Context(), "/unused", []string{"serve"})
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(paths.ownerFile(), "keep"))
	require.NoError(t, err)
}

func TestSidecarStopRejectsUnrecordedListeners(t *testing.T) {
	for _, socket := range []string{"control", "wal"} {
		t.Run(socket, func(t *testing.T) {
			paths := ownerTestPaths(t)
			path := paths.ControlSocket
			if socket == "wal" {
				path = paths.WALSocket
			}
			ln, err := net.Listen("unix", path)
			require.NoError(t, err)
			defer ln.Close()
			require.ErrorContains(t, paths.stopOwned(t.Context()), "unrecorded sidecar is still listening")
			_, err = os.Stat(path)
			require.NoError(t, err, "refusal must not unlink a potentially live socket")
			owner, err := paths.loadOwner()
			require.NoError(t, err)
			require.Nil(t, owner, "a listener alone is not sufficient ownership evidence")
		})
	}
}

func TestSidecarRetiredOwnerRejectsListener(t *testing.T) {
	paths := ownerTestPaths(t)
	state, err := cleanupComparisonPath(paths.StateDir, true)
	require.NoError(t, err)
	owner := &sidecarOwner{PID: 1, Created: 1, Argv: []string{"binary", "serve"}, InstanceID: "fixture-instance", RetiredStateDir: state}
	require.NoError(t, paths.saveOwner(owner))
	ln, err := net.Listen("unix", paths.ControlSocket)
	require.NoError(t, err)
	defer ln.Close()
	require.ErrorContains(t, paths.stopOwned(t.Context()), "listening despite retired ownership")
	_, err = paths.validateAdoption(t.Context(), "/unused", []string{"serve"})
	require.ErrorContains(t, err, "listening despite retired ownership")
	saved, err := paths.loadOwner()
	require.NoError(t, err)
	require.Equal(t, owner, saved)
}

func TestSidecarReadinessRequiresBothSockets(t *testing.T) {
	paths := ownerTestPaths(t)
	ln, err := net.Listen("unix", paths.WALSocket)
	require.NoError(t, err)
	defer ln.Close()
	require.ErrorContains(t, WaitForPgBackRestdSockets(t.Context(), paths, nil, time.Millisecond), "not accepting")
	exited := make(chan struct{})
	close(exited)
	require.ErrorContains(t, WaitForPgBackRestdSockets(t.Context(), paths, exited, time.Second), "exited before its sockets came up")
}

func TestSidecarCredentialPublicationFailureRetainsOldBundle(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture-new-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture-new-secret")
	t.Setenv("AWS_SESSION_TOKEN", "fixture-new-session")
	paths := ownerTestPaths(t)
	dir := paths.s3KeyDir()
	require.NoError(t, os.Mkdir(dir, 0o700))
	// Refuse replacing a nonempty directory; do not remove it to force a write.
	bundle := filepath.Join(dir, "credentials.json")
	require.NoError(t, os.Mkdir(bundle, 0o700))
	evidence := filepath.Join(bundle, "keep")
	require.NoError(t, os.WriteFile(evidence, []byte("old bundle evidence"), 0o600))
	_, err := paths.writeS3KeysFromEnv()
	require.Error(t, err)
	after, err := os.ReadFile(evidence)
	require.NoError(t, err)
	require.Equal(t, "old bundle evidence", string(after))
}

func TestSidecarCredentialLegacyCleanupFailureKeepsPublishedBundle(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	paths := ownerTestPaths(t)
	dir := paths.s3KeyDir()
	require.NoError(t, os.Mkdir(dir, 0o700))
	legacy := filepath.Join(dir, "AWS_SESSION_TOKEN")
	require.NoError(t, os.Mkdir(legacy, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "keep"), []byte("legacy"), 0o600))
	_, err := paths.writeS3KeysFromEnv()
	require.Error(t, err, "legacy cleanup errors must not be silently ignored")
	raw, err := os.ReadFile(filepath.Join(dir, "credentials.json"))
	require.NoError(t, err)
	var bundle map[string]string
	require.NoError(t, json.Unmarshal(raw, &bundle))
	require.Equal(t, map[string]string{"access_key_id": "fixture-key", "secret_access_key": "fixture-secret", "session_token": ""}, bundle)
	info, err := os.Stat(filepath.Join(dir, "credentials.json"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	_, err = os.Stat(filepath.Join(legacy, "keep"))
	require.NoError(t, err, "file cleanup must not become recursive deletion")
}
