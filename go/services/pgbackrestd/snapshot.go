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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/common/backup"
)

// Names of the files in --s3-static-key-dir, as the operator mounts the
// credentials Secret.
const (
	staticKeyIDFile     = "AWS_ACCESS_KEY_ID"
	staticKeySecretFile = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // file name, not a credential
)

// StaticKey is an S3 access key pair for the static credential source.
type StaticKey struct {
	ID, Secret string
}

// RepoSnapshot is the repository view an environment is built from. It is
// immutable once published: Configure builds a new one and swaps it in, so a
// child being started never sees a half-updated mix of two Configures.
type RepoSnapshot struct {
	// Repos maps a repository number to its cipher generation.
	Repos map[uint32]uint32
	// CipherPass maps a cipher generation to its passphrase. An empty
	// passphrase declares the generation unencrypted.
	CipherPass map[uint32]string
	// StaticKeys maps a repository number to its S3 key; only the static
	// credential source fills it.
	StaticKeys map[uint32]StaticKey
}

// Secrets is what envFor needs: the current snapshot plus the process-wide
// token.
type Secrets struct {
	RepoSnapshot
	// CredsToken authenticates S3-command children to the credential socket.
	CredsToken string
}

func (r RepoSnapshot) clone() RepoSnapshot {
	return RepoSnapshot{
		Repos:      maps.Clone(r.Repos),
		CipherPass: maps.Clone(r.CipherPass),
		StaticKeys: maps.Clone(r.StaticKeys),
	}
}

// snapshot returns the published repository snapshot. The result must not be
// modified.
func (s *Server) snapshot() *RepoSnapshot { return s.snap.Load() }

// setRepoSnapshot publishes snap, replacing the previous one atomically. The
// caller's maps are copied, so later changes to them cannot reach children.
func (s *Server) setRepoSnapshot(snap RepoSnapshot) {
	c := snap.clone()
	s.snap.Store(&c)
}

// secretsNow is the Secrets for a child about to start.
func (s *Server) secretsNow() Secrets {
	return Secrets{RepoSnapshot: *s.snapshot(), CredsToken: s.token}
}

// buildSnapshot reads the cipher key file and the static key directory afresh
// and returns the snapshot for repos (repository number to generation). A
// repository whose generation is absent from the key file is an error:
// treating it as unencrypted would silently downgrade it.
func (s *Server) buildSnapshot(repos map[uint32]uint32) (RepoSnapshot, error) {
	snap := RepoSnapshot{
		Repos:      maps.Clone(repos),
		CipherPass: map[uint32]string{},
		StaticKeys: map[uint32]StaticKey{},
	}
	if s.cfg.CipherKeyFile != "" {
		keys, err := backup.LoadCipherKeys(s.cfg.CipherKeyFile)
		if err != nil {
			return RepoSnapshot{}, err
		}
		for gen, pass := range keys {
			snap.CipherPass[uint32(gen)] = pass
		}
		for n, gen := range repos {
			if _, ok := snap.CipherPass[gen]; !ok {
				return RepoSnapshot{}, fmt.Errorf("repository %d uses cipher generation %d, absent from the cipher key file", n, gen)
			}
		}
	}
	if s.cfg.S3CredSource == CredStatic {
		key, err := loadStaticKey(s.cfg.S3StaticKeyDir)
		if err != nil {
			return RepoSnapshot{}, err
		}
		for n := range repos {
			snap.StaticKeys[n] = key
		}
	}
	return snap, nil
}

func loadStaticKey(dir string) (StaticKey, error) {
	id, err := readStaticKey(dir, staticKeyIDFile)
	if err != nil {
		return StaticKey{}, err
	}
	secret, err := readStaticKey(dir, staticKeySecretFile)
	if err != nil {
		return StaticKey{}, err
	}
	return StaticKey{ID: id, Secret: secret}, nil
}

// readStaticKey reads one single-value file from the static key directory.
func readStaticKey(dir, name string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", fmt.Errorf("read static key file: %w", err)
	}
	v := strings.TrimSpace(string(raw))
	if v == "" {
		return "", fmt.Errorf("static key file %q is empty", filepath.Join(dir, name))
	}
	return v, nil
}

func newCredsToken() (string, error) {
	var tok [32]byte
	if _, err := rand.Read(tok[:]); err != nil {
		return "", fmt.Errorf("generate creds token: %w", err)
	}
	return hex.EncodeToString(tok[:]), nil
}
