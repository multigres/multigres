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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/fileutil"
)

func TestLocalCredentialBundleConcurrentSnapshots(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	a := StaticKey{ID: "A", Secret: "A", SessionToken: "A"}
	b := StaticKey{ID: "B", Secret: "B"}
	write := func(key StaticKey) error {
		raw, err := json.Marshal(key) //nolint:gosec // G117: dummy A/B credentials intentionally exercise the private credential bundle format.
		if err != nil {
			return err
		}
		return fileutil.AtomicWriteFile(path, raw, 0o600)
	}
	require.NoError(t, write(a))
	// Deliberately conflicting legacy files must not be combined with a bundle.
	for _, name := range []string{staticKeyIDFile, staticKeySecretFile, staticKeyTokenFile} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("legacy"), 0o600))
	}
	s := &Server{cfg: Config{S3CredSource: CredStatic, S3StaticKeyDir: dir}}
	done := make(chan error, 1)
	go func() {
		for i := range 200 {
			key := a
			if i%2 == 1 {
				key = b
			}
			if err := write(key); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for range 500 {
		snap, err := s.buildSnapshot(map[uint32]uint32{1: 0, 2: 0})
		require.NoError(t, err)
		key := snap.StaticKeys[1]
		require.True(t, key == a || key == b, "must read an entire credential generation")
		require.Equal(t, key, snap.StaticKeys[2])
	}
	require.NoError(t, <-done)
}

func TestInvalidLocalBundleNeverFallsBackToSecretFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{staticKeyIDFile, staticKeySecretFile} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("legacy"), 0o600))
	}
	for _, content := range []string{`{broken`, `{"access_key_id":"new"}`} {
		require.NoError(t, fileutil.AtomicWriteFile(filepath.Join(dir, "credentials.json"), []byte(content), 0o600))
		_, err := loadStaticKey(dir)
		require.Error(t, err)
	}
}
