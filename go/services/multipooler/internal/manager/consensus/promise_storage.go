// Copyright 2026 Supabase, Inc.
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

package consensus

import (
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// promisesFilePath returns the path to the consensus promises file.
func (cs *ConsensusPromises) promisesFilePath() string {
	return filepath.Join(cs.poolerDir, constants.ConsensusPromisesFile)
}

// getPromisesFile retrieves the persisted consensus promises from disk.
// Returns a zero-valued file if it doesn't exist yet.
func (cs *ConsensusPromises) getPromisesFile() (*clustermetadatapb.ConsensusPromises, error) {
	path := cs.promisesFilePath()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return &clustermetadatapb.ConsensusPromises{}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read consensus promises file: %w", err)
	}

	file := &clustermetadatapb.ConsensusPromises{}
	if err := protojson.Unmarshal(data, file); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consensus promises: %w", err)
	}

	return file, nil
}

// setPromisesFile saves the consensus promises to disk atomically.
func (cs *ConsensusPromises) setPromisesFile(file *clustermetadatapb.ConsensusPromises) error {
	path := cs.promisesFilePath()

	data, err := protojson.MarshalOptions{
		Indent: "  ",
	}.Marshal(file)
	if err != nil {
		return fmt.Errorf("failed to marshal consensus promises: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write consensus promises file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename consensus promises file: %w", err)
	}

	return nil
}
