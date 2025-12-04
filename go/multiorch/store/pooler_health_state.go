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
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
)

// IsInitialized returns true if the pooler has been initialized.
// A pooler is considered initialized based on the IsInitialized field from
// the Status RPC, which is determined by the data directory state (not LSN).
// The node must also be reachable for us to trust this information.
func IsInitialized(p *multiorchdata.PoolerHealthState) bool {
	if !p.IsLastCheckValid {
		return false // unreachable nodes are considered uninitialized
	}

	if p.MultiPooler == nil {
		return false
	}

	// Use the IsInitialized field from Status RPC directly.
	// This is based on data directory state, not LSN.
	return p.IsInitialized
}
