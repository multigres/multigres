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

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// postgresDataDir returns the PostgreSQL data directory path
func postgresDataDir(poolerDir string) string {
	return filepath.Join(poolerDir, "pg_data")
}

// isDataDirInitialized checks if the PostgreSQL data directory is initialized
func isDataDirInitialized(poolerDir string) bool {
	dataDir := postgresDataDir(poolerDir)
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
}

// consensusTermPath returns the path to the consensus term file
func consensusTermPath(poolerDir string) string {
	dataDir := postgresDataDir(poolerDir)
	return filepath.Join(dataDir, "consensus", "consensus_term.json")
}

// GetConsensusTerm retrieves the current consensus term information from disk
func GetConsensusTerm(poolerDir string) (*multipoolermanagerdatapb.ConsensusTerm, error) {
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

// SetConsensusTerm saves the consensus term information to disk
func SetConsensusTerm(poolerDir string, term *multipoolermanagerdatapb.ConsensusTerm) error {
	// Check if data directory is initialized
	if !isDataDirInitialized(poolerDir) {
		dataDir := postgresDataDir(poolerDir)
		return fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	termPath := consensusTermPath(poolerDir)
	consensusDir := filepath.Dir(termPath)

	// Create consensus directory if it doesn't exist
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
