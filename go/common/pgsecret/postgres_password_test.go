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

package pgsecret

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writePasswordFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestReadPasswordFile_TrimsTrailingNewline(t *testing.T) {
	cases := map[string]string{
		"lf":    "secret\n",
		"crlf":  "secret\r\n",
		"none":  "secret",
		"multi": "secret\n\n",
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			path := writePasswordFile(t, contents)
			got, err := ReadPasswordFile(path)
			require.NoError(t, err)
			assert.Equal(t, "secret", got)
		})
	}
}

func TestReadPasswordFile_MissingFile(t *testing.T) {
	_, err := ReadPasswordFile("/definitely/does/not/exist/password")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read postgres password file")
}
