// Copyright 2025 Supabase, Inc.
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

package local

import (
	"fmt"
	"os"
	"path/filepath"
)

// certDir returns the directory where pgBackRest certificates are stored
func (p *localProvisioner) certDir() string {
	return filepath.Join(p.config.RootWorkingDir, "certs")
}

// PgBackRestCertPaths holds the paths to the generated pgBackRest certificates.
type PgBackRestCertPaths struct {
	CACertFile     string // ca.crt
	ServerCertFile string // pgbackrest.crt
	ServerKeyFile  string // pgbackrest.key
}

// GeneratePgBackRestCerts creates TLS certificates for pgBackRest server in the specified directory.
// This is a public function that can be reused by tests and other components.
// It creates:
//   - ca.crt and ca.key (CA certificate and key)
//   - pgbackrest.crt and pgbackrest.key (server certificate and key)
//
// Returns the paths to the generated certificates that are needed for pgBackRest configuration.
func GeneratePgBackRestCerts(certDir string) (*PgBackRestCertPaths, error) {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create pgBackRest certificate directory: %w", err)
	}

	caCertFile := filepath.Join(certDir, "ca.crt")
	caKeyFile := filepath.Join(certDir, "ca.key")
	if err := generateCA(caCertFile, caKeyFile); err != nil {
		return nil, fmt.Errorf("failed to generate CA for pgBackRest: %w", err)
	}

	certFile := filepath.Join(certDir, "pgbackrest.crt")
	keyFile := filepath.Join(certDir, "pgbackrest.key")
	if err := generateCert(caCertFile, caKeyFile, certFile, keyFile, "pgbackrest", []string{"localhost", "pgbackrest"}); err != nil {
		return nil, fmt.Errorf("failed to generate certificate for pgBackRest: %w", err)
	}

	return &PgBackRestCertPaths{
		CACertFile:     caCertFile,
		ServerCertFile: certFile,
		ServerKeyFile:  keyFile,
	}, nil
}
