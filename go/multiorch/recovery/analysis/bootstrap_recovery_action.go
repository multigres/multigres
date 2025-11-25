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

// BootstrapRecoveryAction is a placeholder for bootstrap recovery logic.
// Full implementation will be added later when coordinator integration is ready.
type BootstrapRecoveryAction struct {
	logger *slog.Logger
}

// NewBootstrapRecoveryAction creates a new bootstrap recovery action.
func NewBootstrapRecoveryAction(logger *slog.Logger) *BootstrapRecoveryAction {
	return &BootstrapRecoveryAction{
		logger: logger,
	}
}

func (a *BootstrapRecoveryAction) Execute(ctx context.Context, problem Problem) error {
	a.logger.InfoContext(ctx, "bootstrap recovery action triggered (not yet implemented)",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)
	// TODO: Implement actual bootstrap logic:
	// 1. Fetch cohort from poolerStore
	// 2. Call coordinator.BootstrapShard() with appropriate cohort
	return fmt.Errorf("bootstrap action not yet implemented")
}

func (a *BootstrapRecoveryAction) RequiresHealthyPrimary() bool {
	return false // bootstrap doesn't need a primary
}

func (a *BootstrapRecoveryAction) RequiresLock() bool {
	return true // bootstrap requires exclusive shard lock
}

func (a *BootstrapRecoveryAction) Metadata() RecoveryMetadata {
	return RecoveryMetadata{
		Timeout:   60 * time.Second,
		Retryable: false, // bootstrap should not auto-retry
	}
}

func (a *BootstrapRecoveryAction) Priority() Priority {
	return PriorityShardBootstrap
}
