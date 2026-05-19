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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// poolerAddressFor projects a MultiPooler into the contact-info subset the
// consensus RPCs (SetTermPrimary, Propose) take. Returns nil if mp is nil.
func poolerAddressFor(mp *clustermetadatapb.MultiPooler) *clustermetadatapb.PoolerAddress {
	if mp == nil {
		return nil
	}
	return &clustermetadatapb.PoolerAddress{
		Id:           mp.GetId(),
		Host:         mp.GetHostname(),
		PostgresPort: mp.GetPortMap()["postgres"],
	}
}
