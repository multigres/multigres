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

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

// reservedConnAPI is the subset of *reserved.Conn methods that the executor
// touches when serving a query on an existing reserved connection. Defined as
// an interface so unit tests can substitute a mock without standing up a real
// PostgreSQL connection. *reserved.Conn satisfies this interface.
type reservedConnAPI interface {
	ConnID() int64
	RemainingReasons() uint32
	IsInTransaction() bool
	BeginWithQuery(ctx context.Context, beginQuery string) error
	AddReservationReason(reason uint32)
	RemoveReservationReason(reason uint32) bool
	QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error
	TxnStatus() protocol.TransactionStatus
}

// Compile-time check that *reserved.Conn satisfies reservedConnAPI.
var _ reservedConnAPI = (*reserved.Conn)(nil)
