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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// GetCurrentWALPosition retrieves the current WAL position from PostgreSQL
func (pm *MultiPoolerManager) GetCurrentWALPosition(ctx context.Context) (*consensusdatapb.WALPosition, error) {
	if pm.db == nil {
		return nil, fmt.Errorf("database connection not available")
	}

	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	if err != nil {
		return nil, fmt.Errorf("failed to get current WAL LSN: %w", err)
	}

	logIndex := ConvertLSNToIndex(lsn)

	// Get current term from consensus state
	pm.mu.RLock()
	currentTerm := int64(0)
	if pm.consensusTerm != nil {
		currentTerm = pm.consensusTerm.CurrentTerm
	}
	pm.mu.RUnlock()

	return &consensusdatapb.WALPosition{
		Lsn:       lsn,
		LogIndex:  logIndex,
		LogTerm:   currentTerm,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// ConvertLSNToIndex converts a PostgreSQL LSN (e.g., "0/1A2B3C4D") to a monotonic integer
// This is used for easy comparison in Raft-style log matching
func ConvertLSNToIndex(lsn string) int64 {
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return 0
	}

	high, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return 0
	}

	low, err := strconv.ParseInt(parts[1], 16, 64)
	if err != nil {
		return 0
	}

	return (high << 32) | low
}

// IsWALPositionUpToDate implements Raft log matching rules to determine
// if a candidate's WAL position is at least as up-to-date as ours
func IsWALPositionUpToDate(candidateLogIndex, candidateLogTerm int64, ourWAL *consensusdatapb.WALPosition) bool {
	// Implement Raft log matching rules:
	// 1. If terms differ, higher term wins
	if candidateLogTerm > ourWAL.LogTerm {
		return true
	}
	if candidateLogTerm < ourWAL.LogTerm {
		return false
	}

	// 2. If terms are equal, higher log index wins
	return candidateLogIndex >= ourWAL.LogIndex
}

// CompareWALPositions compares two WAL positions
// Returns:
//
//	-1 if pos1 < pos2
//	 0 if pos1 == pos2
//	 1 if pos1 > pos2
func CompareWALPositions(pos1, pos2 *consensusdatapb.WALPosition) int {
	if pos1.LogTerm < pos2.LogTerm {
		return -1
	}
	if pos1.LogTerm > pos2.LogTerm {
		return 1
	}

	// Same term, compare log index
	if pos1.LogIndex < pos2.LogIndex {
		return -1
	}
	if pos1.LogIndex > pos2.LogIndex {
		return 1
	}

	return 0
}
