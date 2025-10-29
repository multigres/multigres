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
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// GetCurrentWALPosition retrieves the current WAL position from PostgreSQL
// For primaries: returns CurrentLsn from pg_current_wal_lsn()
// For standbys: returns LastReceiveLsn and LastReplayLsn from pg_last_wal_receive_lsn() and pg_last_wal_replay_lsn()
func (pm *MultiPoolerManager) GetCurrentWALPosition(ctx context.Context) (*consensusdatapb.WALPosition, error) {
	if pm.db == nil {
		return nil, fmt.Errorf("database connection not available")
	}

	// Check if we're in recovery (standby)
	var inRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return nil, fmt.Errorf("failed to check recovery status: %w", err)
	}

	walPos := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}

	if inRecovery {
		// On standby: get receive and replay positions
		var receiveLsn, replayLsn sql.NullString
		err = pm.db.QueryRowContext(ctx,
			"SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()").
			Scan(&receiveLsn, &replayLsn)
		if err != nil {
			return nil, fmt.Errorf("failed to get standby WAL positions: %w", err)
		}

		if receiveLsn.Valid {
			walPos.LastReceiveLsn = receiveLsn.String
		}
		if replayLsn.Valid {
			walPos.LastReplayLsn = replayLsn.String
		}
	} else {
		// On primary: get current write position
		var currentLsn string
		err = pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&currentLsn)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary WAL position: %w", err)
		}
		walPos.CurrentLsn = currentLsn
	}

	return walPos, nil
}

// ConvertLSNToIndex converts a PostgreSQL LSN (e.g., "0/1A2B3C4D") to a monotonic integer
// Note: we may or may not actually use this because timeline numbers are not reliable in split-brain situations
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
