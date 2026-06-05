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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

// getRevocation retrieves the current term revocation from disk.
func (cs *ConsensusState) getRevocation() (*clustermetadatapb.TermRevocation, error) {
	termPath := cs.consensusTermPath()

	// Check if consensus term file exists
	if _, err := os.Stat(termPath); os.IsNotExist(err) {
		// Return empty term if file doesn't exist
		return &clustermetadatapb.TermRevocation{}, nil
	}

	// Read the file
	data, err := os.ReadFile(termPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read consensus term file: %w", err)
	}

	// Unmarshal JSON to protobuf
	revocation := &clustermetadatapb.TermRevocation{}
	if err := protojson.Unmarshal(data, revocation); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consensus term: %w", err)
	}

	return revocation, nil
}

// setRevocation saves the term revocation to disk atomically.
func (cs *ConsensusState) setRevocation(revocation *clustermetadatapb.TermRevocation) error {
	termPath := cs.consensusTermPath()

	// Marshal protobuf to JSON
	data, err := protojson.MarshalOptions{
		Indent: "  ",
	}.Marshal(revocation)
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
