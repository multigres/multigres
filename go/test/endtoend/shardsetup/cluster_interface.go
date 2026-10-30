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

package shardsetup

import (
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
)

// ShardSetup is the Multigres topology of the query-serving tests.
var _ clustersetup.Cluster = (*ShardSetup)(nil)

// ClientPort returns the multigateway's PostgreSQL port.
func (s *ShardSetup) ClientPort() int {
	return s.MultigatewayPgPort
}

// ClientTLSCertPaths returns the multigateway's TLS certificates, or nil when
// it was started without TLS.
func (s *ShardSetup) ClientTLSCertPaths() *MultigatewayTLSCertPaths {
	return s.MultigatewayTLSCertPaths
}

// ComparisonTargets returns the primary's PostgreSQL and the multigateway.
func (s *ShardSetup) ComparisonTargets(t *testing.T) []TestTarget {
	t.Helper()
	return s.GetComparisonTargets(t)
}

// WaitForQueryServing waits until the multigateway serves a write.
func (s *ShardSetup) WaitForQueryServing(t *testing.T) {
	t.Helper()
	s.WaitForMultigatewayQueryServing(t)
}

// PostgresPort returns the primary's PostgreSQL port.
func (s *ShardSetup) PostgresPort(t *testing.T) int {
	t.Helper()
	return s.GetPrimary(t).Pgctld.PgPort
}

// PostgresSocketDir returns the primary's PostgreSQL Unix socket directory.
func (s *ShardSetup) PostgresSocketDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(s.GetPrimary(t).Pgctld.PoolerDir, "pg_sockets")
}

// GatewayLogFile returns the multigateway's log file.
func (s *ShardSetup) GatewayLogFile() string {
	return s.Multigateway.LogFile
}

// PoolerLogFile returns the primary multipooler's log file.
func (s *ShardSetup) PoolerLogFile(t *testing.T) string {
	t.Helper()
	return s.PrimaryMultipooler(t).LogFile
}
