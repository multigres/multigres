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

package manager

import (
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// postgresDataDir returns the PostgreSQL data directory path from PGDATA env var
func postgresDataDir() string {
	return os.Getenv(constants.PgDataDirEnvVar)
}

// multigresDataDir returns the multigres-specific subdirectory within PGDATA
func multigresDataDir() string {
	return filepath.Join(postgresDataDir(), constants.MultigresMarkerDirectory)
}

// consensusTermPath returns the path to the consensus term file
func (cs *ConsensusState) consensusTermPath() string {
	return filepath.Join(cs.poolerDir, constants.ConsensusTermFile)
}

// getConsensusTerm retrieves the current consensus term information from disk
func (cs *ConsensusState) getConsensusTerm() (*multipoolermanagerdatapb.ConsensusTerm, error) {
	termPath := cs.consensusTermPath()

	// Check if consensus term file exists
	if _, err := os.Stat(termPath); os.IsNotExist(err) {
		// Return empty term if file doesn't exist
		return &multipoolermanagerdatapb.ConsensusTerm{}, nil
	}

	// Read the file
	data, err := os.ReadFile(termPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read consensus term file: %w", err)
	}

	// Unmarshal JSON to protobuf
	term := &multipoolermanagerdatapb.ConsensusTerm{}
	if err := protojson.Unmarshal(data, term); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consensus term: %w", err)
	}

	return term, nil
}

// setConsensusTerm saves the consensus term information to disk
func (cs *ConsensusState) setConsensusTerm(term *multipoolermanagerdatapb.ConsensusTerm) error {
	termPath := cs.consensusTermPath()

	// Marshal protobuf to JSON
	data, err := protojson.MarshalOptions{
		Indent: "  ",
	}.Marshal(term)
	if err != nil {
		return fmt.Errorf("failed to marshal consensus term: %w", err)
	}

	// Write to file atomically using a temporary file
	tmpPath := termPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write consensus term file: %w", err)
	}

	// Rename to final path (atomic operation)
	if err := os.Rename(tmpPath, termPath); err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename consensus term file: %w", err)
	}

	return nil
}

// DeleteTermFile removes the consensus term file from disk and resets the
// in-memory state to uninitialized (term 0, no accepted coordinator).
// Called after a pgBackRest restore so the node re-joins consensus from
// scratch; the cluster's current term will be propagated by multiorch on
// first contact via BeginTerm.
// If the file does not exist this is a no-op. Returns an error only if
// the file exists but cannot be removed.
func (cs *ConsensusState) DeleteTermFile() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if err := os.Remove(cs.consensusTermPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete consensus term file after restore: %w", err)
	}

	cs.term = &multipoolermanagerdatapb.ConsensusTerm{}
	return nil
}
