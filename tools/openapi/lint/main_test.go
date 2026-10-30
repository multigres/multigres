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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		valid       bool
	}{
		{"valid", `openapi: 3.1.0
info:
  title: Test API
  version: v1
paths: {}
`, true},
		{"invalid syntax", `{`, false},
		{"missing title", `openapi: 3.1.0
info:
  version: v1
paths: {}
`, false},
		{"invalid response", `openapi: 3.1.0
info:
  title: Test API
  version: v1
paths:
  /test:
    get:
      responses:
        '200': {}
`, false},
		{"unresolved reference", `openapi: 3.1.0
info:
  title: Test API
  version: v1
paths:
  /test:
    get:
      responses:
        '200':
          $ref: '#/components/responses/Missing'
`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spec.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.input), 0o600))
			err := run([]string{path})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	require.ErrorContains(t, run(nil), "usage:")
	require.Error(t, run([]string{filepath.Join(t.TempDir(), "missing.yaml")}))
}
