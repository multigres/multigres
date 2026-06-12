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

package grpcpoolerservice

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// TestAdmissionKind locks the request classification used by every query
// handler. The scenarios are labelled by the handler input that produces them,
// so this doubles as documentation of each handler's intended mapping:
//
//   - id > 0          → ExistingReserved (ConcludeTransaction, DiscardTempTables,
//     ReleaseReservedConnection, or any handler continuing a reserved conn)
//   - id == 0, reserves → NewReservation (StreamExecute with reservation reasons,
//     PortalStreamExecute with MaxRows > 0 or reasons, CopyBidiExecute which
//     always pins)
//   - id == 0, !reserves → SingleQuery (StreamExecute without reasons,
//     ExecuteQuery/Describe, fetch-all PortalStreamExecute with no reasons)
func TestAdmissionKind(t *testing.T) {
	tests := []struct {
		name     string
		connID   uint64
		reserves bool
		want     poolerserver.RequestKind
	}{
		{"existing reserved ignores reserves=false", 42, false, poolerserver.RequestExistingReserved},
		{"existing reserved ignores reserves=true", 42, true, poolerserver.RequestExistingReserved},
		{"fresh request that reserves → new reservation", 0, true, poolerserver.RequestNewReservation},
		{"fresh request that does not reserve → single query", 0, false, poolerserver.RequestSingleQuery},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, admissionKind(tt.connID, tt.reserves))
		})
	}
}

// TestPortalReserves locks the portal reserve predicate against the executor's
// decision: a portal reserves on MaxRows > 0 OR reservation reasons. The reasons
// case is the regression guard — a fetch-all portal (MaxRows == 0) that folds a
// deferred BEGIN into its first execute carries ReasonTransaction and must NOT
// be admitted as a single query during a graceful drain.
func TestPortalReserves(t *testing.T) {
	tests := []struct {
		name    string
		maxRows uint64
		reasons uint32
		want    bool
	}{
		{"fetch-all, no reasons → single query", 0, 0, false},
		{"suspendable cursor (MaxRows > 0)", 100, 0, true},
		{"fetch-all with deferred BEGIN folded in (reasons set)", 0, protoutil.ReasonTransaction, true},
		{"cursor and reasons", 100, protoutil.ReasonTransaction, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &query.ExecuteOptions{MaxRows: tt.maxRows}
			var ro *query.ReservationOptions // nil when reasons == 0, exercising the nil-safe getter
			if tt.reasons != 0 {
				ro = &query.ReservationOptions{Reasons: tt.reasons}
			}
			assert.Equal(t, tt.want, portalReserves(opts, ro))
		})
	}
}
