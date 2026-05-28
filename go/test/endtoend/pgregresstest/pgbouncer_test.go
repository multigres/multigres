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

package pgregresstest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParsePgbouncerVersion(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"PgBouncer 1.24.1\n", "1.24.1"},
		{"pgbouncer version 1.21.0", "1.21.0"},
		{"junk before 2.0.0 trailing", "2.0.0"},
		{"no version here", ""},
	}
	for _, tc := range tests {
		got := parsePgbouncerVersion(tc.in)
		if got != tc.want {
			t.Errorf("parsePgbouncerVersion(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestRenderPgbouncerConfig exercises the template rendering path end-to-end
// without launching pgbouncer. Catches template syntax errors, missing
// substitution fields, and accidental edits to the embedded .tmpl files.
func TestRenderPgbouncerConfig(t *testing.T) {
	for _, mode := range []PgbouncerMode{PgbouncerModeSession, PgbouncerModeTransaction} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			p := &Pgbouncer{
				Mode:        mode,
				ListenAddr:  "127.0.0.1",
				ListenPort:  6432,
				BackendPort: 5432,
				Password:    "secret",
				DataDir:     dir,
				ConfigPath:  filepath.Join(dir, "pgbouncer.ini"),
				LogPath:     filepath.Join(dir, "pgbouncer.log"),
			}
			if err := p.renderConfig(); err != nil {
				t.Fatalf("renderConfig: %v", err)
			}
			data, err := os.ReadFile(p.ConfigPath)
			if err != nil {
				t.Fatalf("read rendered config: %v", err)
			}
			cfg := string(data)
			if !strings.Contains(cfg, "listen_port = 6432") {
				t.Errorf("listen_port not substituted into config:\n%s", cfg)
			}
			if !strings.Contains(cfg, "port=5432") {
				t.Errorf("backend port not substituted:\n%s", cfg)
			}
			if !strings.Contains(cfg, "password=secret") {
				t.Errorf("password not substituted:\n%s", cfg)
			}
			if !strings.Contains(cfg, "logfile = "+p.LogPath) {
				t.Errorf("logfile not substituted:\n%s", cfg)
			}
			wantPoolMode := "pool_mode = " + string(mode)
			if !strings.Contains(cfg, wantPoolMode) {
				t.Errorf("expected %q in config:\n%s", wantPoolMode, cfg)
			}
			if mode == PgbouncerModeTransaction && !strings.Contains(cfg, "max_prepared_statements") {
				t.Errorf("transaction-mode config missing max_prepared_statements:\n%s", cfg)
			}
		})
	}
}
