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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitialPgBackRestRepo(t *testing.T) {
	tests := []struct {
		name string
		keys CipherKeys
		want PgBackRestRepo
	}{
		{
			name: "no keys is an unencrypted repo",
			keys: nil,
			want: PgBackRestRepo{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true, Version: 1},
		},
		{
			name: "declared-unencrypted generation is an unencrypted repo",
			keys: CipherKeys{1: ""},
			want: PgBackRestRepo{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true, Version: 1},
		},
		{
			name: "mounted key is an encrypted repo with its fingerprint",
			keys: CipherKeys{1: "some-passphrase"},
			want: PgBackRestRepo{
				Generation:     1,
				RepoNumber:     1,
				Encrypted:      true,
				KeyFingerprint: CipherKeyFingerprint("some-passphrase"),
				State:          PgBackRestRepoStateActive,
				Authoritative:  true,
				Version:        1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, InitialPgBackRestRepo(tt.keys))
		})
	}
}
