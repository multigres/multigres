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

package executor

import (
	"context"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// reservedConnAPI is the subset of *reserved.Conn methods that the executor
// touches when serving a query on an existing reserved connection. Defined as
// an interface so unit tests can substitute a mock without standing up a real
// PostgreSQL connection. *reserved.Conn satisfies this interface.
type reservedConnAPI interface {
	ConnID() int64
	ProcessID() uint32
	RemainingReasons() uint32
	IsInTransaction() bool
	BeginWithQuery(ctx context.Context, beginQuery string) error
	AddReservationReason(reason uint32)
	RemoveReservationReason(reason uint32) bool
	MarkSessionStateUntrusted()
	QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error
	// Query runs a simple query and buffers all results. Used for internal
	// probes (e.g. checking pg_locks to decide whether a session still holds an
	// advisory lock), not for streaming client query results.
	Query(ctx context.Context, sql string) ([]*sqltypes.Result, error)
	// ReserveForPortal pins the named portal/cursor on this reserved
	// connection. Used for DECLARE … WITH HOLD so the cursor survives
	// COMMIT — the multipooler's reservation bitmask keeps ReasonPortal
	// set until every pinned portal is released or the session ends.
	ReserveForPortal(portalName string)
	// ReleasePortal drops the named portal from the pin set. Returns true
	// when the last pin clears AND no other reservation reasons remain —
	// callers should then release the backend to the pool.
	ReleasePortal(portalName string) bool
	// Release returns the backend to the pool (after release finalization for
	// clean reasons) or taints/closes it (reasons indicating uncertain state).
	Release(reason reserved.ReleaseReason)
}

// Compile-time check that *reserved.Conn satisfies reservedConnAPI.
var _ reservedConnAPI = (*reserved.Conn)(nil)
