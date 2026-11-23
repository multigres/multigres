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

package analysis

import (
	"context"

	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// fakeAction is a named recovery action for analyzer tests. Analyzers only attach
// actions to problems, so it never runs.
type fakeAction struct{ name string }

func (a fakeAction) Execute(context.Context, types.RecheckedProblem) error { return nil }
func (a fakeAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{Name: a.name}
}
func (fakeAction) RequiresHealthyLeader() bool           { return false }
func (fakeAction) GracePeriod() *types.GracePeriodConfig { return nil }

type testActionFactory struct{}

var _ ActionFactory = testActionFactory{}

func newTestActionFactory() ActionFactory { return testActionFactory{} }

func (testActionFactory) NewShardInitAction() types.RecoveryAction { return fakeAction{"ShardInit"} }
func (testActionFactory) NewAppointLeaderAction() types.RecoveryAction {
	return fakeAction{"AppointLeader"}
}

func (testActionFactory) NewFixReplicationAction() types.RecoveryAction {
	return fakeAction{"FixReplication"}
}

func (testActionFactory) NewDemoteStaleLeaderAction() types.RecoveryAction {
	return fakeAction{"DemoteStaleLeader"}
}

func (testActionFactory) NewReconcileCohortAction() types.RecoveryAction {
	return fakeAction{"ReconcileCohort"}
}

func (testActionFactory) NewReconnectRecruitAbandonedAction() types.RecoveryAction {
	return fakeAction{"ReconnectRecruitAbandoned"}
}

// The engine recognizes alert-only problems by this name.
func (testActionFactory) NewAlertOnlyAction() types.RecoveryAction {
	return fakeAction{actions.AlertOnlyActionName}
}
