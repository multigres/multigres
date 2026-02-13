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

func TestKeyToFlagPrefix(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "single underscore",
			key:  "pg2_host",
			want: "--pg2-host=",
		},
		{
			name: "multiple underscores",
			key:  "repo1_s3_key",
			want: "--repo1-s3-key=",
		},
		{
			name: "key with numbers",
			key:  "pg1_path",
			want: "--pg1-path=",
		},
		{
			name: "three underscores",
			key:  "pg2_host_ca_file",
			want: "--pg2-host-ca-file=",
		},
		{
			name: "empty key",
			key:  "",
			want: "",
		},
		{
			name: "already formatted key",
			key:  "--pg2-host",
			want: "",
		},
		{
			name: "no underscores",
			key:  "delta",
			want: "--delta=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := keyToFlagPrefix(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestApplyPgBackRestOverrides(t *testing.T) {
	tests := []struct {
		name      string
		baseArgs  []string
		overrides map[string]string
		want      []string
	}{
		{
			name:      "no overrides returns base args",
			baseArgs:  []string{"--stanza=test", "--type=full"},
			overrides: nil,
			want:      []string{"--stanza=test", "--type=full"},
		},
		{
			name:      "empty overrides returns base args",
			baseArgs:  []string{"--stanza=test", "--type=full"},
			overrides: map[string]string{},
			want:      []string{"--stanza=test", "--type=full"},
		},
		{
			name:     "replace existing pg2_host",
			baseArgs: []string{"--pg2-host=oldhost", "--type=full"},
			overrides: map[string]string{
				"pg2_host": "newhost",
			},
			want: []string{"--pg2-host=newhost", "--type=full"},
		},
		{
			name:     "add new pg2_path",
			baseArgs: []string{"--pg2-host=host", "--type=full"},
			overrides: map[string]string{
				"pg2_path": "/custom/path",
			},
			want: []string{"--pg2-host=host", "--type=full", "--pg2-path=/custom/path"},
		},
		{
			name:     "replace pg2_port and add pg2_path",
			baseArgs: []string{"--pg2-host=host", "--pg2-port=5432"},
			overrides: map[string]string{
				"pg2_port": "9999",
				"pg2_path": "/custom/path",
			},
			want: []string{"--pg2-host=host", "--pg2-port=9999", "--pg2-path=/custom/path"},
		},
		{
			name:     "replace pg2_host_port in TLS mode",
			baseArgs: []string{"--pg2-host=host", "--pg2-host-type=tls", "--pg2-host-port=8432"},
			overrides: map[string]string{
				"pg2_host_port": "9999",
			},
			want: []string{"--pg2-host=host", "--pg2-host-type=tls", "--pg2-host-port=9999"},
		},
		{
			name:     "add arbitrary option compress_type",
			baseArgs: []string{"--stanza=test", "--type=full"},
			overrides: map[string]string{
				"compress_type": "gzip",
			},
			want: []string{"--stanza=test", "--type=full", "--compress-type=gzip"},
		},
		{
			name:     "replace arbitrary option repo1_path",
			baseArgs: []string{"--repo1-path=/old/repo", "--type=full"},
			overrides: map[string]string{
				"repo1_path": "/custom/repo",
			},
			want: []string{"--repo1-path=/custom/repo", "--type=full"},
		},
		{
			name:     "add multi-underscore option repo1_s3_bucket",
			baseArgs: []string{"--stanza=test"},
			overrides: map[string]string{
				"repo1_s3_bucket": "my-bucket",
			},
			want: []string{"--stanza=test", "--repo1-s3-bucket=my-bucket"},
		},
		{
			name:     "empty key is skipped",
			baseArgs: []string{"--stanza=test"},
			overrides: map[string]string{
				"":         "ignored",
				"pg2_host": "newhost",
			},
			want: []string{"--stanza=test", "--pg2-host=newhost"},
		},
		{
			name:     "empty value still creates flag",
			baseArgs: []string{"--stanza=test"},
			overrides: map[string]string{
				"delta": "",
			},
			want: []string{"--stanza=test", "--delta="},
		},
		{
			name:     "multiple arbitrary overrides",
			baseArgs: []string{"--stanza=test", "--compress-type=lz4"},
			overrides: map[string]string{
				"compress_type": "gzip",
				"repo1_path":    "/custom/repo",
				"delta":         "y",
			},
			want: []string{"--stanza=test", "--compress-type=gzip", "--repo1-path=/custom/repo", "--delta=y"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ApplyPgBackRestOverrides(tt.baseArgs, tt.overrides)
			// Use ElementsMatch for tests with multiple new overrides to handle map iteration order
			if tt.name == "multiple arbitrary overrides" {
				assert.ElementsMatch(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
