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

package manager

import "time"

// RecoveryActionType represents types of recovery operations that require
// postgres to remain stopped until the recovery completes
type RecoveryActionType int

const (
	// RecoveryActionTypeNone means postgres can be started normally by the monitor
	RecoveryActionTypeNone RecoveryActionType = 0

	// RecoveryActionTypeNeedsRewind means postgres needs pg_rewind before it can be started.
	// Set by EmergencyDemote when shutting down a diverged primary.
	RecoveryActionTypeNeedsRewind RecoveryActionType = 1

	// Future: RecoveryActionTypeNeedsCrashRecovery, RecoveryActionTypeNeedsPITR, etc.
)

// String returns the string representation of the recovery action type
func (t RecoveryActionType) String() string {
	switch t {
	case RecoveryActionTypeNone:
		return "none"
	case RecoveryActionTypeNeedsRewind:
		return "needs_rewind"
	default:
		return "unknown"
	}
}

// RecoveryAction represents a pending recovery operation that blocks postgres restart.
// This is persisted to disk at $PGDATA/recovery/recovery_action.json
// The postgres monitor checks this file and will NOT auto-start postgres if a recovery
// action is pending. The recovery action must be completed and cleared before monitoring resumes.
type RecoveryAction struct {
	// ActionType is the type of recovery action needed
	ActionType RecoveryActionType `json:"action_type"`

	// Reason is a human-readable explanation of why this recovery action was set
	Reason string `json:"reason"`

	// SetTime is when the recovery action was set
	SetTime time.Time `json:"set_time"`

	// ConsensusTerm is the consensus term at the time the recovery action was set
	ConsensusTerm int64 `json:"consensus_term"`
}
