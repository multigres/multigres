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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/multigres/multigres/config"
)

// ServerConfigOpts holds options for generating pgbackrest-server.conf.
type ServerConfigOpts struct {
	PoolerDir     string // Base directory for pooler data
	CertDir       string // Directory containing TLS certificates
	Port          int    // TLS server port
	Pg1Port       int    // Local PostgreSQL port
	Pg1SocketPath string // Local PostgreSQL socket directory
	Pg1Path       string // Local PostgreSQL data directory
}

// WriteServerConfig generates a minimal pgbackrest-server.conf for the TLS server.
// The server config contains only TLS settings, log path, and pg1 stanza.
// Returns the path to the generated config file.
func WriteServerConfig(opts ServerConfigOpts) (string, error) {
	pgbackrestDir := filepath.Join(opts.PoolerDir, "pgbackrest")
	logPath := filepath.Join(pgbackrestDir, "log")

	// Create directories
	for _, dir := range []string{pgbackrestDir, logPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	tmpl, err := template.New("pgbackrest-server").Parse(config.PgBackRestServerConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgbackrest server config template: %w", err)
	}

	templateData := struct {
		LogPath        string
		ServerCertFile string
		ServerKeyFile  string
		ServerCAFile   string
		ServerPort     int
		Pg1SocketPath  string
		Pg1Port        int
		Pg1Path        string
	}{
		LogPath:        logPath,
		ServerCertFile: filepath.Join(opts.CertDir, "pgbackrest.crt"),
		ServerKeyFile:  filepath.Join(opts.CertDir, "pgbackrest.key"),
		ServerCAFile:   filepath.Join(opts.CertDir, "ca.crt"),
		ServerPort:     opts.Port,
		Pg1SocketPath:  opts.Pg1SocketPath,
		Pg1Port:        opts.Pg1Port,
		Pg1Path:        opts.Pg1Path,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return "", fmt.Errorf("failed to execute pgbackrest server config template: %w", err)
	}

	configPath := filepath.Join(pgbackrestDir, "pgbackrest-server.conf")
	if err := os.WriteFile(configPath, buf.Bytes(), 0o644); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest-server.conf: %w", err)
	}

	return configPath, nil
}
