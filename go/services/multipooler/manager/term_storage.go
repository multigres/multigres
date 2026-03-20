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
func consensusTermPath(poolerDir string) string {
	return filepath.Join(poolerDir, "pg_data", "consensus", "consensus_term.json")
}

// getConsensusTerm retrieves the current consensus term information from disk
func getConsensusTerm(poolerDir string) (*multipoolermanagerdatapb.ConsensusTerm, error) {
	termPath := consensusTermPath(poolerDir)

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
func setConsensusTerm(poolerDir string, term *multipoolermanagerdatapb.ConsensusTerm) error {
	termPath := consensusTermPath(poolerDir)
	consensusDir := filepath.Dir(termPath)

	// Ensure consensus directory exists
	if err := os.MkdirAll(consensusDir, 0o755); err != nil {
		return fmt.Errorf("failed to create consensus directory: %w", err)
	}

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
