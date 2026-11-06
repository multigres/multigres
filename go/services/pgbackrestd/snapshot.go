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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/backup"
)

// Names of the files in --s3-static-key-dir, as the operator mounts the
// credentials Secret.
const (
	staticKeyTokenFile  = "AWS_SESSION_TOKEN" //nolint:gosec // file name, not a credential
	staticKeyIDFile     = "AWS_ACCESS_KEY_ID"
	staticKeySecretFile = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // file name, not a credential
)

// StaticKey is one complete S3 credential generation. SessionToken is optional.
type StaticKey struct {
	ID           string `json:"access_key_id"`
	Secret       string `json:"secret_access_key"`
	SessionToken string `json:"session_token"`
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
		keys, err := loadCipherKeys(s.cfg.CipherKeyFile)
		if err != nil {
			return RepoSnapshot{}, err
		}
		maps.Copy(snap.CipherPass, keys)
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

// loadCipherKeys enforces the sidecar's uint32 generation boundary before
// publishing keys to either a snapshot or Status. Reject the whole file rather
// than allowing an oversized generation to alias a different repository's key.
func loadCipherKeys(path string) (map[uint32]string, error) {
	keys, err := backup.LoadCipherKeys(path)
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]string, len(keys))
	for gen, pass := range keys {
		// Parse at the wire width so the subsequent conversion is bounded on
		// every architecture, without relying on the host int width.
		generation, err := strconv.ParseUint(strconv.Itoa(gen), 10, 32)
		if err != nil || generation == 0 {
			return nil, fmt.Errorf("backup cipher key file %q: generation %d is outside the sidecar uint32 range", path, gen)
		}
		result[uint32(generation)] = pass
	}
	return result, nil
}

func loadStaticKey(dir string) (StaticKey, error) {
	// The local adapter publishes all three values in one atomic generation.
	// Kubernetes Secret directory mounts retain their existing file contract.
	raw, err := os.ReadFile(filepath.Join(dir, "credentials.json"))
	if err == nil {
		var key StaticKey
		if json.Unmarshal(raw, &key) != nil {
			return StaticKey{}, errors.New("invalid static credential bundle")
		}
		if key.ID == "" || key.Secret == "" {
			return StaticKey{}, errors.New("static credential bundle requires access key and secret")
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return StaticKey{}, fmt.Errorf("read static credential bundle: %w", err)
	}

	id, err := readStaticKey(dir, staticKeyIDFile)
	if err != nil {
		return StaticKey{}, err
	}
	secret, err := readStaticKey(dir, staticKeySecretFile)
	if err != nil {
		return StaticKey{}, err
	}
	token, err := readStaticKey(dir, staticKeyTokenFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return StaticKey{}, err
	}
	return StaticKey{ID: id, Secret: secret, SessionToken: token}, nil
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
