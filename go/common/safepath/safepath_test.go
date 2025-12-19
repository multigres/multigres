// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package safepath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsSafePathChar(t *testing.T) {
	tests := []struct {
		name string
		r    rune
		want bool
	}{
		{"lowercase letter", 'a', true},
		{"uppercase letter", 'Z', true},
		{"digit", '5', true},
		{"underscore", '_', true},
		{"hyphen", '-', true},
		{"dot", '.', true},
		{"forward slash", '/', false},
		{"backward slash", '\\', false},
		{"colon", ':', false},
		{"space", ' ', false},
		{"null byte", '\x00', false},
		{"control char", '\x1F', false},
		{"unicode", '日', false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSafePathChar(tt.r)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEncodePathComponent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple alphanumeric", "mydb", "mydb"},
		{"with underscore", "my_db", "my_db"},
		{"with hyphen", "my-db", "my-db"},
		{"with single dot", "my.db", "my.db"},
		{"double dot", "my..db", "my%2E%2Edb"},
		{"just double dot", "..", "%2E%2E"},
		{"forward slash", "db/backup", "db%2Fbackup"},
		{"backward slash", "db\\backup", "db%5Cbackup"},
		{"colon", "db:backup", "db%3Abackup"},
		{"space", "my db", "my%20db"},
		{"null byte", "db\x00name", "db%00name"},
		{"control char", "db\x1Fname", "db%1Fname"},
		{"mixed safe and unsafe", "db-123.backup:v2", "db-123.backup%3Av2"},
		{"unicode japanese", "データベース", "%E3%83%87%E3%83%BC%E3%82%BF%E3%83%99%E3%83%BC%E3%82%B9"},
		{"unicode emoji", "db🔥test", "db%F0%9F%94%A5test"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodePathComponent(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		name            string
		basePath        string
		components      []string
		wantPath        string
		wantErr         bool
		wantErrContains string
	}{
		{
			name:       "simple valid path",
			basePath:   "/backups",
			components: []string{"mydb", "tg1", "shard0"},
			wantPath:   "/backups/mydb/tg1/shard0",
			wantErr:    false,
		},
		{
			name:       "with dots in components",
			basePath:   "/backups",
			components: []string{"my.db", "tg.1", "shard.0"},
			wantPath:   "/backups/my.db/tg.1/shard.0",
			wantErr:    false,
		},
		{
			name:       "double dot encoded",
			basePath:   "/backups",
			components: []string{"..", "tg1", "shard0"},
			wantPath:   "/backups/%2E%2E/tg1/shard0",
			wantErr:    false,
		},
		{
			name:       "slash in component encoded",
			basePath:   "/backups",
			components: []string{"db/etc", "tg1", "shard0"},
			wantPath:   "/backups/db%2Fetc/tg1/shard0",
			wantErr:    false,
		},
		{
			name:       "backslash in component encoded",
			basePath:   "/backups",
			components: []string{"db\\windows", "tg1", "shard0"},
			wantPath:   "/backups/db%5Cwindows/tg1/shard0",
			wantErr:    false,
		},
		{
			name:       "unicode components",
			basePath:   "/backups",
			components: []string{"データベース", "グループ", "シャード"},
			wantPath:   "/backups/%E3%83%87%E3%83%BC%E3%82%BF%E3%83%99%E3%83%BC%E3%82%B9/%E3%82%B0%E3%83%AB%E3%83%BC%E3%83%97/%E3%82%B7%E3%83%A3%E3%83%BC%E3%83%89",
			wantErr:    false,
		},
		{
			name:       "single component",
			basePath:   "/backups",
			components: []string{"mydb"},
			wantPath:   "/backups/mydb",
			wantErr:    false,
		},
		{
			name:       "no components",
			basePath:   "/backups",
			components: []string{},
			wantPath:   "/backups",
			wantErr:    false,
		},
		{
			name:       "empty component preserved",
			basePath:   "/backups",
			components: []string{"mydb", "", "shard0"},
			wantPath:   "/backups/mydb/shard0",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := Join(tt.basePath, tt.components...)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}
