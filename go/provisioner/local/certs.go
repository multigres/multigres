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

// PgCertPaths holds the paths to generated certificates.
type PgCertPaths struct {
	CACertFile         string // ca.crt
	PgBackrestCertFile string // pgbackrest.crt
	PgBackrestKeyFile  string // pgbackrest.key
	ServerCertFile     string // server.crt
	ServerKeyFile      string // server.key
	PgCtldCertFile     string // <pgCtldUser>.crt
	PgCtldKeyFile      string // <pgCtldUser>.key
}

// GeneratePgCerts creates TLS certificates in the specified directory.
// This is a public function that can be reused by tests and other components.
// It creates:
//   - ca.crt and ca.key (CA certificate and key)
//   - pgbackrest.crt and pgbackrest.key (server certificate and key)
//   - server.crt / server.key  — PostgreSQL server SSL certificate (SAN=localhost)
//   - <pgCtldUser>.crt / <pgCtldUser>.key — client certificate (CN=pgCtldUser)
//
// The client certificate CN must exactly match the PostgreSQL role name for
// cert authentication (clientcert=verify-full) to succeed.
//
// Returns the paths to the generated certificates that are needed for pgBackRest configuration.
func GeneratePgCerts(certDir, pgCtldUser string) (*PgCertPaths, error) {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create pgBackRest certificate directory: %w", err)
	}

	caCertFile := filepath.Join(certDir, "ca.crt")
	caKeyFile := filepath.Join(certDir, "ca.key")
	if err := generateCA(caCertFile, caKeyFile); err != nil {
		return nil, fmt.Errorf("failed to generate CA for pgBackRest: %w", err)
	}

	backrestCertFile := filepath.Join(certDir, "pgbackrest.crt")
	backrestKeyFile := filepath.Join(certDir, "pgbackrest.key")
	if err := generateCert(caCertFile, caKeyFile, backrestCertFile, backrestKeyFile, "pgbackrest", []string{"localhost", "pgbackrest"}); err != nil {
		return nil, fmt.Errorf("failed to generate certificate for pgBackRest: %w", err)
	}

	// PostgreSQL server SSL certificate
	serverCertFile := filepath.Join(certDir, "server.crt")
	serverKeyFile := filepath.Join(certDir, "server.key")
	if err := generateCert(caCertFile, caKeyFile,
		serverCertFile,
		serverKeyFile,
		"postgres-server", []string{"localhost"}); err != nil {
		return nil, fmt.Errorf("failed to generate PG server cert: %w", err)
	}

	// Client certificate for the controller role (CN must match role name for cert auth)
	ctldCertFile := filepath.Join(certDir, pgCtldUser+".crt")
	ctldKeyFile := filepath.Join(certDir, pgCtldUser+".key")
	if err := generateCert(caCertFile, caKeyFile,
		ctldCertFile,
		ctldKeyFile,
		pgCtldUser, []string{"localhost"}); err != nil {
		return nil, fmt.Errorf("failed to generate %s client cert: %w", pgCtldUser, err)
	}

	return &PgCertPaths{
		CACertFile:         caCertFile,
		PgBackrestCertFile: backrestCertFile,
		PgBackrestKeyFile:  backrestKeyFile,
		ServerCertFile:     serverCertFile,
		ServerKeyFile:      serverKeyFile,
		PgCtldCertFile:     ctldCertFile,
		PgCtldKeyFile:      ctldKeyFile,
	}, nil
}
