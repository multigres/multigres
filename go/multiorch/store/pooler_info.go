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

package store

import (
	"time"

	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerInfo represents runtime state of a MultiPooler instance.
// This is the minimal version that stores:
// - The MultiPooler record from topology
// - Timestamps for staleness detection
// - Computed fields for quick access
type PoolerInfo struct {
	// MultiPooler record from topology service
	MultiPooler *clustermetadata.MultiPooler

	// Timestamps (critical for staleness detection)
	LastCheckAttempted  time.Time
	LastCheckSuccessful time.Time
	LastSeen            time.Time

	// Computed fields (cached)
	IsUpToDate       bool
	IsLastCheckValid bool
}
