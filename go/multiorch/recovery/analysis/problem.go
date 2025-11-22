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
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Problem represents a detected issue.
type Problem struct {
	Code           ProblemCode           // Category of problem
	CheckName      CheckName             // Which check detected it
	PoolerID       *clustermetadatapb.ID // Affected pooler
	Database       string                // Database name
	TableGroup     string                // TableGroup name
	Shard          string                // Shard name
	Description    string                // Human-readable description
	Priority       Priority              // Priority of this problem
	Scope          ProblemScope          // Whether this affects the whole cluster or just one pooler
	DetectedAt     time.Time             // When the problem was detected
	RecoveryAction RecoveryAction        // What to do about it
}
