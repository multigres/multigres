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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadCipherKeys(t *testing.T) {
	writeKeyFile := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "keys.json")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	tests := []struct {
		name    string
		content string
		want    CipherKeys
		wantErr string
	}{
		{
			name:    "single generation",
			content: `{"1": "pass-one"}`,
			want:    CipherKeys{1: "pass-one"},
		},
		{
			name:    "multiple generations",
			content: `{"1": "pass-one", "2": "pass-two"}`,
			want:    CipherKeys{1: "pass-one", 2: "pass-two"},
		},
		{
			name:    "empty passphrase declares an unencrypted generation",
			content: `{"1": "", "2": "pass-two"}`,
			want:    CipherKeys{1: "", 2: "pass-two"},
		},
		{
			name:    "empty document is an error",
			content: `{}`,
			wantErr: "declares no generations",
		},
		{
			name:    "malformed JSON is an error",
			content: `{"1": "pass-one"`,
			wantErr: "parse backup cipher key file",
		},
		{
			name:    "non-integer generation is an error",
			content: `{"one": "pass-one"}`,
			wantErr: "invalid generation",
		},
		{
			name:    "non-positive generation is an error",
			content: `{"0": "pass-one"}`,
			wantErr: "invalid generation",
		},
		{
			name:    "non-canonical generation is an error, never a silent collision",
			content: `{"01": "pass-one"}`,
			wantErr: "invalid generation",
		},
		{
			name:    "passphrase with control characters is an error",
			content: `{"1": "good\nrepo1-path=/tmp/evil"}`,
			wantErr: "control characters",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := LoadCipherKeys(writeKeyFile(t, tt.content))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, keys)
		})
	}

	t.Run("missing file is an error, never treated as no keys", func(t *testing.T) {
		_, err := LoadCipherKeys(filepath.Join(t.TempDir(), "does-not-exist.json"))
		require.ErrorContains(t, err, "read backup cipher key file")
	})
}

func TestCipherKeyFingerprint(t *testing.T) {
	fp := CipherKeyFingerprint("some-passphrase")
	assert.Len(t, fp, cipherKeyFingerprintLen)
	assert.Equal(t, fp, CipherKeyFingerprint("some-passphrase"), "fingerprint is deterministic")
	assert.NotEqual(t, fp, CipherKeyFingerprint("other-passphrase"))
	assert.NotContains(t, fp, "some-passphrase", "fingerprint never contains the passphrase")
}
