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

func TestRestorePgpassMode(t *testing.T) {
	tests := []struct {
		name         string
		mode         os.FileMode
		missing      bool
		wantRestored bool
	}{
		{name: "leaves 0600 alone", mode: 0o600},
		{name: "leaves a narrower mode alone", mode: 0o400},
		{name: "restores group rw added by fsGroup", mode: 0o660, wantRestored: true},
		{name: "restores world read", mode: 0o604, wantRestored: true},
		{name: "missing file is not an error", missing: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pgbackrest.pgpass")
			if !tt.missing {
				if err := os.WriteFile(path, []byte("*:*:*:postgres:secret\n"), 0o600); err != nil {
					t.Fatalf("create pgpass file: %v", err)
				}
				if err := os.Chmod(path, tt.mode); err != nil {
					t.Fatalf("set pgpass mode: %v", err)
				}
			}

			restored, err := RestorePgpassMode(path)
			if err != nil {
				t.Fatalf("RestorePgpassMode() error: %v", err)
			}
			if restored != tt.wantRestored {
				t.Errorf("restored = %v, want %v", restored, tt.wantRestored)
			}
			if tt.missing {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("stat after missing-file call: err = %v, want not-exist", err)
				}
				return
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat pgpass file: %v", err)
			}
			want := tt.mode
			if tt.wantRestored {
				want = 0o600
			}
			if got := info.Mode().Perm(); got != want {
				t.Errorf("mode = %o, want %o", got, want)
			}
		})
	}
}
