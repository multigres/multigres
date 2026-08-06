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

package actions

import (
	"context"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// Compile-time assertion that AlertOnlyAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*AlertOnlyAction)(nil)

// AlertOnlyAction is the recovery action for non-actionable problems — ones an
// orchestrator has detected but cannot fix on its own (e.g. ShardStuck,
// ShardAtRisk). It performs no remediation; surfacing the problem is the whole
// point. The alert signal is the detected-problems metric that the recovery
// cycle already records; Execute only logs so the reason is visible in logs.
type AlertOnlyAction struct {
	logger *slog.Logger
}

// NewAlertOnlyAction creates an alert-only recovery action.
func NewAlertOnlyAction(logger *slog.Logger) *AlertOnlyAction {
	return &AlertOnlyAction{logger: logger}
}

// Execute records the problem for human attention and returns nil. It never
// mutates cluster state.
func (a *AlertOnlyAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.WarnContext(ctx, "non-actionable problem detected; human intervention required",
		"problem_code", problem.Code,
		"database", problem.ShardKey.GetDatabase(),
		"tablegroup", problem.ShardKey.GetTableGroup(),
		"shard", problem.ShardKey.GetShard(),
		"description", problem.Description,
	)
	return nil
}

func (a *AlertOnlyAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "AlertOnly",
		Description: "Records a non-actionable problem for alerting; performs no remediation",
		Timeout:     5 * time.Second,
	}
}

// RequiresHealthyLeader is false: alerting never touches the leader.
func (a *AlertOnlyAction) RequiresHealthyLeader() bool {
	return false
}

// GracePeriod is nil: there is nothing to defer for an alert.
func (a *AlertOnlyAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
