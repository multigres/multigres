// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/utils"
)

// StartUnsafeMultigateway starts a SECOND multigateway process configured with
// --unsafe-pooler-mode, sharing this (already-bootstrapped) cluster's topology
// and poolers. It exists so a small set of external extension tests whose
// scaffolding the enforcing gateway (correctly) rejects — e.g. pg_partman's
// pgTAP files that run `DO $$ ... EXECUTE 'DROP TABLE '||to_char(...) $$` — can
// still run through a pooler. The enforcing gateway (s.Multigateway) is left
// untouched, so every other suite keeps exercising the rejections.
//
// Idempotent: repeated calls return the same port. Returns the gateway's
// PostgreSQL protocol port. Tear down with StopUnsafeMultigateway (or leave it
// to Cleanup).
func (s *ShardSetup) StartUnsafeMultigateway(t *testing.T) int {
	t.Helper()
	if s.unsafeMultigateway != nil {
		return s.unsafeMultigatewayPgPort
	}

	pgPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)
	grpcPort := utils.GetFreePort(t)

	inst := &ProcessInstance{
		Name:        "multigateway-unsafe",
		Binary:      "multigateway",
		Cell:        s.CellName,
		ServiceID:   "multigateway-unsafe-" + s.CellName,
		PgPort:      pgPort,
		HttpPort:    httpPort,
		GrpcPort:    grpcPort,
		EtcdAddr:    s.EtcdClientAddr,
		GlobalRoot:  "/multigres/global",
		LogFile:     filepath.Join(s.TempDir, "multigateway-unsafe.log"),
		Environment: os.Environ(),
		// Disable the unsafe-statement rejections for this gateway only.
		ExtraArgs: []string{"--unsafe-pooler-mode"},
	}
	if s.MultigatewayTLSCertPaths != nil {
		inst.TLSCertFile = s.MultigatewayTLSCertPaths.ServerCertFile
		inst.TLSKeyFile = s.MultigatewayTLSCertPaths.ServerKeyFile
	}

	if err := inst.Start(s.runningCtx, t); err != nil {
		t.Fatalf("failed to start unsafe multigateway: %v", err)
	}
	s.unsafeMultigateway = inst
	s.unsafeMultigatewayPgPort = pgPort

	// The cluster is fully bootstrapped by the time this is called (poolers are
	// already in topology), so query serving comes up quickly.
	s.waitForMultigatewayQueryServingOnPort(t, pgPort)
	t.Logf("Started unsafe-pooler-mode multigateway on PG port %d", pgPort)
	return pgPort
}

// StopUnsafeMultigateway gracefully terminates the gateway started by
// StartUnsafeMultigateway, if any. Safe to call when none is running.
func (s *ShardSetup) StopUnsafeMultigateway(t *testing.T) {
	t.Helper()
	if s.unsafeMultigateway == nil {
		return
	}
	s.unsafeMultigateway.TerminateGracefully(func(format string, args ...any) {
		t.Logf(format, args...)
	}, 5*time.Second)
	s.unsafeMultigateway = nil
	s.unsafeMultigatewayPgPort = 0
}
