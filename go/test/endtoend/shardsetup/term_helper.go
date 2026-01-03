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

package shardsetup

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// consensusTermFilename must match the constant in go/multipooler/manager/consensus_state.go
const consensusTermFilename = "consensus_term.json"

// SetTermDirectly writes the consensus term file directly to a pooler's data directory.
// This is a test-only helper that bypasses the gRPC API.
//
// WARNING: This should only be used in tests. In production, use BeginTerm RPC.
func SetTermDirectly(t *testing.T, poolerDataDir string, term *multipoolermanagerdatapb.ConsensusTerm) {
	t.Helper()

	data, err := protojson.Marshal(term)
	if err != nil {
		t.Fatalf("failed to marshal consensus term: %v", err)
	}

	termPath := filepath.Join(poolerDataDir, consensusTermFilename)
	if err := os.WriteFile(termPath, data, 0o644); err != nil {
		t.Fatalf("failed to write consensus term file: %v", err)
	}

	t.Logf("Set consensus term directly: term=%d at %s", term.TermNumber, termPath)
}

// DeleteTermFile removes the consensus term file, resetting the node to uninitialized state.
// This is useful for test cleanup.
func DeleteTermFile(t *testing.T, poolerDataDir string) {
	t.Helper()

	termPath := filepath.Join(poolerDataDir, consensusTermFilename)
	if err := os.Remove(termPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to delete consensus term file: %v", err)
	}
}
