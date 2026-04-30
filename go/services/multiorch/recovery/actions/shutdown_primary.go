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

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// Compile-time assertion that ShutdownPrimaryAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ShutdownPrimaryAction)(nil)

// ShutdownPrimaryCallback is called by ShutdownPrimaryAction to execute the switchover.
// It is implemented by Engine.ShutdownPrimary and injected via the factory to avoid a
// circular import between recovery/actions and recovery.
type ShutdownPrimaryCallback func(
	ctx context.Context,
	primaryID *clustermetadatapb.ID,
	database, tableGroup, shard string,
) error

// ShutdownPrimaryAction orchestrates a graceful primary switchover when the primary
// multipooler signals PoolerType_STOPPING (e.g. on SIGTERM). It delegates to the
// Engine.ShutdownPrimary method via an injected callback.
type ShutdownPrimaryAction struct {
	fn     ShutdownPrimaryCallback
	logger *slog.Logger
}

// NewShutdownPrimaryAction creates a ShutdownPrimaryAction backed by the given callback.
// fn may be nil when the factory is constructed; Execute will return an error if called
// before SetShutdownPrimary wires it up.
func NewShutdownPrimaryAction(fn ShutdownPrimaryCallback, logger *slog.Logger) *ShutdownPrimaryAction {
	return &ShutdownPrimaryAction{fn: fn, logger: logger}
}

func (a *ShutdownPrimaryAction) Execute(ctx context.Context, problem types.Problem) error {
	if a.fn == nil {
		return mterrors.New(0, "ShutdownPrimaryAction: shutdown callback not wired up")
	}
	if problem.PoolerID == nil {
		return mterrors.New(0, "ShutdownPrimaryAction: problem.PoolerID is nil")
	}
	a.logger.InfoContext(ctx, "ShutdownPrimaryAction: graceful primary switchover triggered",
		"primary", problem.PoolerID.Name,
		"shard", problem.ShardKey.String())

	return a.fn(ctx, problem.PoolerID, problem.ShardKey.Database, problem.ShardKey.TableGroup, problem.ShardKey.Shard)
}

func (a *ShutdownPrimaryAction) RequiresHealthyPrimary() bool { return false }

func (a *ShutdownPrimaryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:      "ShutdownPrimary",
		Timeout:   90 * time.Second,
		Retryable: false,
	}
}

func (a *ShutdownPrimaryAction) Priority() types.Priority {
	return types.PriorityEmergency
}

func (a *ShutdownPrimaryAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
