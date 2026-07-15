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

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// InitialRepoGeneration is the generation of the first backup repository every
// cluster gets by convention.
const InitialRepoGeneration = 1

// cipherKeyFingerprintLen is the length in hex characters of a cipher key
// fingerprint (128 bits of the SHA-256).
const cipherKeyFingerprintLen = 32

// CipherKeys maps a backup repository generation to its cipher passphrase.
// An empty-string passphrase declares that generation unencrypted — distinct
// from the generation being absent from the file (index with the two-value
// form to tell them apart).
type CipherKeys map[int]string

// LoadCipherKeys reads a backup cipher key file: a JSON document mapping
// repository generation to cipher passphrase, e.g. {"1": "<passphrase>"}.
// The file is typically a mounted Kubernetes Secret.
//
// Any read or parse failure is an error — a configured key file that cannot
// be loaded must never be treated as "no keys", because that would silently
// downgrade an encrypted repository to unencrypted.
func LoadCipherKeys(path string) (CipherKeys, error) {
	// #nosec G703 -- path is an operator-configured key-file location (flag/env/Secret mount), not external request input.
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read backup cipher key file %q: %w", path, err)
	}
	var raw map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse backup cipher key file %q: %w", path, err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("backup cipher key file %q declares no generations", path)
	}
	keys := make(CipherKeys, len(raw))
	for genStr, pass := range raw {
		// Canonical decimal only: reject "01", "+1", " 1" so distinct JSON
		// keys can never collapse onto the same generation.
		gen, err := strconv.Atoi(genStr)
		if err != nil || gen < 1 || strconv.Itoa(gen) != genStr {
			return nil, fmt.Errorf("backup cipher key file %q: invalid generation %q (must be a positive integer)", path, genStr)
		}
		// The passphrase is rendered verbatim onto a pgbackrest.conf INI
		// line; control characters would corrupt the conf or inject settings.
		for _, r := range pass {
			if r < 0x20 || r == 0x7f {
				return nil, fmt.Errorf("backup cipher key file %q: passphrase for generation %d contains control characters", path, gen)
			}
		}
		keys[gen] = pass
	}
	return keys, nil
}

// CipherKeyFingerprint returns the fingerprint of a cipher passphrase: a
// SHA-256 prefix, safe to store and log in place of the passphrase itself.
// Fingerprints are only inert against offline guessing when passphrases are
// high-entropy (control-plane-minted), which is the operating invariant.
func CipherKeyFingerprint(pass string) string {
	sum := sha256.Sum256([]byte(pass))
	return hex.EncodeToString(sum[:])[:cipherKeyFingerprintLen]
}
