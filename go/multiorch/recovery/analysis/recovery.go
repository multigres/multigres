// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"context"
	"time"
)

// RecoveryAction is a function that fixes a problem.
type RecoveryAction interface {
	// Execute performs the recovery.
	Execute(ctx context.Context, problem Problem) error

	// Metadata returns info about this recovery.
	Metadata() RecoveryMetadata

	// RequiresLock indicates if tablegroup lock is needed.
	RequiresLock() bool

	// RequiresHealthyPrimary indicates if this recovery requires a healthy primary.
	// If true, the recovery will be skipped when the primary is unhealthy.
	// This provides an extra guardrail to avoid accidental operations on replicas
	// when the cluster is not healthy (e.g., can't fix replica replication if primary is dead).
	RequiresHealthyPrimary() bool

	// Priority returns the priority of this recovery action.
	// Higher priority actions are attempted first.
	Priority() Priority
}

// RecoveryMetadata describes the recovery action.
type RecoveryMetadata struct {
	Name        string
	Description string
	Timeout     time.Duration
	Retryable   bool
}
