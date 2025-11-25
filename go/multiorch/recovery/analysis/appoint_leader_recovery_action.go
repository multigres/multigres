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

package analysis

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// AppointLeaderRecoveryAction is a placeholder for leader appointment logic.
// Full implementation will be added later when coordinator integration is ready.
type AppointLeaderRecoveryAction struct {
	logger *slog.Logger
}

// NewAppointLeaderRecoveryAction creates a new leader appointment recovery action.
func NewAppointLeaderRecoveryAction(logger *slog.Logger) *AppointLeaderRecoveryAction {
	return &AppointLeaderRecoveryAction{
		logger: logger,
	}
}

func (a *AppointLeaderRecoveryAction) Execute(ctx context.Context, problem Problem) error {
	a.logger.InfoContext(ctx, "appoint leader recovery action triggered (not yet implemented)",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)
	// TODO: Implement actual leader appointment logic:
	// 1. Fetch cohort from poolerStore
	// 2. Call coordinator.AppointLeader() to elect primary
	return fmt.Errorf("appoint leader action not yet implemented")
}

func (a *AppointLeaderRecoveryAction) RequiresHealthyPrimary() bool {
	return false // leader appointment doesn't need existing primary
}

func (a *AppointLeaderRecoveryAction) RequiresLock() bool {
	return true // leader appointment requires exclusive shard lock
}

func (a *AppointLeaderRecoveryAction) Metadata() RecoveryMetadata {
	return RecoveryMetadata{
		Timeout:   30 * time.Second,
		Retryable: true, // can retry if it fails
	}
}

func (a *AppointLeaderRecoveryAction) Priority() Priority {
	return PriorityShardBootstrap
}
