// Copyright 2026 Supabase, Inc.
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

package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePgpassFile(t *testing.T) {
	tests := []struct {
		name          string
		setupExisting bool
	}{
		{name: "creates file and parent directory"},
		{name: "replaces existing file with secure mode", setupExisting: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolerDir := t.TempDir()
			wantPath := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.pgpass")
			if tt.setupExisting {
				if err := os.MkdirAll(filepath.Dir(wantPath), 0o755); err != nil {
					t.Fatalf("create pgbackrest directory: %v", err)
				}
				if err := os.WriteFile(wantPath, []byte("stale"), 0o644); err != nil {
					t.Fatalf("create existing pgpass file: %v", err)
				}
				if err := os.Chmod(wantPath, 0o644); err != nil {
					t.Fatalf("set existing pgpass mode: %v", err)
				}
			}

			gotPath, err := WritePgpassFile(poolerDir, "postgres", "secret")
			if err != nil {
				t.Fatalf("WritePgpassFile() error: %v", err)
			}
			if gotPath != wantPath {
				t.Errorf("path = %q, want %q", gotPath, wantPath)
			}

			content, err := os.ReadFile(wantPath)
			if err != nil {
				t.Fatalf("read pgpass file: %v", err)
			}
			if got, want := string(content), "*:*:*:postgres:secret\n"; got != want {
				t.Errorf("content = %q, want %q", got, want)
			}

			info, err := os.Stat(wantPath)
			if err != nil {
				t.Fatalf("stat pgpass file: %v", err)
			}
			if got := info.Mode().Perm(); got != 0o600 {
				t.Errorf("mode = %o, want 600", got)
			}
		})
	}
}
