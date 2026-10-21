// Copyright 2026 Supabase, Inc.
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

package scatterconn

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// A reserved connection can be lost underneath a session: the pooler reaps it
// while idle, its backend is terminated or crashes, or its pooler fails over.
// The multipooler then answers the next RPC on it with an error and no
// reserved state, and the gateway stops tracking the reservation.
//
// When the reservation held only a transaction, the client sees the error
// (usually 40001) and the transaction is marked failed; a retry rebuilds it.
// When it held session state, a retry cannot bring that state back, and the
// next statement would run on a fresh backend without it: an advisory lock
// another session can now take, a temp table that no longer shadows a
// permanent one. The gateway ends the session instead, so the client
// reconnects knowing its state is gone.

// sessionStateReasons are the reservation reasons that stand for session state
// the client relies on beyond the current transaction. ReasonPortal is decided
// separately, see lostSessionState.
const sessionStateReasons = protoutil.ReasonTempTable |
	protoutil.ReasonLogicalReplication |
	protoutil.ReasonSessionAdvisoryLock |
	protoutil.ReasonSetSeed |
	protoutil.ReasonUnsafeConnection

// lostSessionState returns the session-state reasons among reasons, the
// reservation reasons the gateway tracked for a reservation that was lost.
//
// A portal is session state unless a transaction owns it: the pooler adds
// ReasonPortal to a transaction's reservation whenever a portal suspends, and
// such a portal dies with the transaction anyway. WITH HOLD cursors outlive
// their transaction, so with any of them open the portal reason counts. A
// portal suspended outside a transaction counts too: losing it would re-bind it
// on a new backend and return its first rows again.
func lostSessionState(reasons uint32, state *handler.MultigatewayConnectionState) uint32 {
	lost := reasons & sessionStateReasons
	if protoutil.HasReason(reasons, protoutil.ReasonPortal) &&
		(!protoutil.HasTransactionReason(reasons) || state.HasAnyOpenHoldCursor()) {
		lost |= protoutil.ReasonPortal
	}
	return lost
}

// sessionStateLostError logs a lost reservation that held session state and
// returns the FATAL that ends the client's session.
func (sc *ScatterConn) sessionStateLostError(ctx context.Context, conn *server.Conn, lost uint32, cause error) error {
	lostState := protoutil.ReasonsString(lost)
	// Only the SQLSTATE of the cause: its text can carry query data.
	sc.logger.WarnContext(ctx, "ending session: reserved connection holding session state was lost",
		"connection_id", conn.ConnectionID(),
		"lost_state", lostState,
		"cause_sqlstate", mterrors.ExtractSQLSTATE(cause))
	return mterrors.NewSessionStateLost(lostState)
}
