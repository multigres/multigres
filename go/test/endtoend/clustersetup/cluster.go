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

package clustersetup

import (
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/provisioner/local"
)

// DefaultTestUser is the PostgreSQL user that tests use when connecting to
// the gateway or pooler as a regular client.
const DefaultTestUser = "postgres"

// Cluster is what the query-serving tests need from a running cluster, whether
// it is a Multigres shard or a single Minigres process. Each topology's harness
// implements it; tests that need features of one topology only (Multiorch,
// replicas, several gateways) reach the concrete type through a helper that
// skips the test on the other topology.
type Cluster interface {
	// ClientPort is the PostgreSQL port clients connect to.
	ClientPort() int
	// ClientTLSCertPaths returns the certificates of the client listener, or
	// nil when it was started without TLS.
	ClientTLSCertPaths() *MultigatewayTLSCertPaths
	// ComparisonTargets returns the raw PostgreSQL target and the proxy target,
	// so a test can check that the proxy behaves like PostgreSQL.
	ComparisonTargets(t *testing.T) []TestTarget
	// SetupTest checks that the cluster is in its clean state before a test and
	// registers a cleanup that restores it afterwards.
	SetupTest(t *testing.T, opts ...SetupTestOption)
	// WaitForQueryServing waits until a write succeeds through ClientPort.
	WaitForQueryServing(t *testing.T)
	// PostgresPort is the port of the PostgreSQL instance that serves writes.
	PostgresPort(t *testing.T) int
	// PostgresSocketDir is the Unix socket directory of that PostgreSQL
	// instance, for tests that connect to it directly as the superuser.
	PostgresSocketDir(t *testing.T) string
	// GatewayLogFile is the log file of the gateway.
	GatewayLogFile() string
	// PoolerLogFile is the log file of the pooler that serves writes.
	PoolerLogFile(t *testing.T) string
	// DumpServiceLogs prints where the service logs are kept.
	DumpServiceLogs()
	// Cleanup stops the cluster. The temporary directory is kept when
	// testsFailed is true.
	Cleanup(testsFailed bool)
}

// TestTarget is a PostgreSQL-protocol endpoint a test runs against. Use
// Cluster.ComparisonTargets to get both raw PostgreSQL and the proxy, so the
// same test logic verifies that the proxy behaves like PostgreSQL.
type TestTarget struct {
	// Name identifies the target: "postgres" or "multigateway".
	Name string
	// Port is the PostgreSQL protocol port to connect to.
	Port int
}

// MultigatewayTLSCertPaths holds the paths to the generated certificates of the
// gateway's PostgreSQL listener.
type MultigatewayTLSCertPaths struct {
	CACertFile     string // CA certificate file (for client verify-ca / verify-full)
	ServerCertFile string // Server certificate file
	ServerKeyFile  string // Server private key file
}

// GenerateMultigatewayTLSCerts creates a CA and a server certificate (CN and
// SAN localhost) for the gateway's PostgreSQL listener under certDir.
func GenerateMultigatewayTLSCerts(t *testing.T, certDir string) *MultigatewayTLSCertPaths {
	t.Helper()

	caCertFile := filepath.Join(certDir, "ca.crt")
	caKeyFile := filepath.Join(certDir, "ca.key")
	if err := local.GenerateCA(caCertFile, caKeyFile); err != nil {
		t.Fatalf("failed to generate CA for multigateway TLS: %v", err)
	}

	certFile := filepath.Join(certDir, "server.crt")
	keyFile := filepath.Join(certDir, "server.key")
	if err := local.GenerateCert(caCertFile, caKeyFile, certFile, keyFile, "localhost", []string{"localhost"}); err != nil {
		t.Fatalf("failed to generate certificate for multigateway TLS: %v", err)
	}

	t.Logf("Generated multigateway TLS certificates in %s", certDir)
	return &MultigatewayTLSCertPaths{
		CACertFile:     caCertFile,
		ServerCertFile: certFile,
		ServerKeyFile:  keyFile,
	}
}

// SetupTestConfig holds configuration for SetupTest.
type SetupTestConfig struct {
	NoReplication    bool     // Don't configure replication
	PauseReplication bool     // Configure replication but pause WAL replay
	GucsToReset      []string // GUCs to save before test and restore after
}

// SetupTestOption is a function that configures SetupTest behavior.
type SetupTestOption func(*SetupTestConfig)

// WithoutReplication returns an option that actively breaks replication.
// Clears primary_conninfo and synchronous_standby_names, so tests can set up replication from scratch.
func WithoutReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.NoReplication = true
	}
}

// WithPausedReplication returns an option that pauses WAL replay on standbys.
// Replication is already configured from bootstrap; this just pauses WAL application.
// Use this for tests that need to test pg_wal_replay_resume().
func WithPausedReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.PauseReplication = true
	}
}

// WithResetGuc returns an option that saves and restores specific GUC settings.
func WithResetGuc(gucNames ...string) SetupTestOption {
	return func(c *SetupTestConfig) {
		c.GucsToReset = append(c.GucsToReset, gucNames...)
	}
}
