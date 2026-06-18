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

package protoutil

import (
	"fmt"
	"strings"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// validReasonsMask is the bitmask of all known reservation reasons.
const validReasonsMask = ReasonTransaction | ReasonTempTable | ReasonPortal | ReasonCopy | ReasonListen | ReasonLogicalReplication | ReasonSessionAdvisoryLock

// Reason constants as uint32 for bitmask operations.
// These match the ReservationReason enum values.
const (
	ReasonTransaction = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_TRANSACTION) // 1
	ReasonTempTable   = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_TEMP_TABLE)  // 2
	ReasonPortal      = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_PORTAL)      // 4
	ReasonCopy        = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_COPY)        // 8
	ReasonListen      = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_LISTEN)      // 16

	// ReasonLogicalReplication indicates the connection has logical-replication
	// session state (an owned slot or an active replication-protocol stream)
	// and must stay pinned to its current Postgres backend for the session's
	// lifetime.
	//
	// Today this bit is set only at connection-open time, by the
	// reserved.Pool.NewLogicalReplicationConn factory for connections that
	// requested `replication=database` in the startup parameters.
	//
	// TODO: also set this bit mid-session when the planner observes
	// pg_create_logical_replication_slot(...) on a plain SQL connection
	// (polling / CDC-RLS path). This will mirror how ReasonTempTable is set
	// when CREATE TEMP TABLE is observed (see
	// go/services/multigateway/handler/connection_state.go for the
	// PendingTempTableReservation precedent). Until that lands, polling
	// consumers must cooperate by setting an application_name that
	// multigateway recognizes (also future work).
	ReasonLogicalReplication = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_LOGICAL_REPLICATION) // 32

	// ReasonSessionAdvisoryLock indicates the session holds one or more
	// session-level advisory locks (pg_advisory_lock / pg_advisory_lock_shared
	// and their try-variants). These locks live on a specific Postgres backend
	// and survive transaction boundaries, so the backend stays pinned to the
	// client session until every such lock is released (pg_advisory_unlock /
	// pg_advisory_unlock_all) or the session ends. Transaction-level advisory
	// locks (pg_advisory_xact_lock) are released at transaction end and do NOT
	// set this reason.
	ReasonSessionAdvisoryLock = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_SESSION_ADVISORY_LOCK) // 64
)

// ValidateReasons returns an error if any unknown bits are set in the reasons bitmask.
func ValidateReasons(reasons uint32) error {
	if reasons & ^validReasonsMask != 0 {
		return fmt.Errorf("invalid reservation reasons: %032b", reasons)
	}
	return nil
}

// HasReason returns true if the reasons bitmask contains the specified reason.
func HasReason(reasons uint32, reason uint32) bool {
	return reasons&reason != 0
}

// HasTransactionReason returns true if the reasons bitmask includes transaction.
func HasTransactionReason(reasons uint32) bool {
	return HasReason(reasons, ReasonTransaction)
}

// HasTempTableReason returns true if the reasons bitmask includes temp table.
func HasTempTableReason(reasons uint32) bool {
	return HasReason(reasons, ReasonTempTable)
}

// HasPortalReason returns true if the reasons bitmask includes portal.
func HasPortalReason(reasons uint32) bool {
	return HasReason(reasons, ReasonPortal)
}

// HasCopyReason returns true if the reasons bitmask includes an active COPY operation.
func HasCopyReason(reasons uint32) bool {
	return HasReason(reasons, ReasonCopy)
}

// HasListenReason returns true if the reasons bitmask includes LISTEN/NOTIFY.
func HasListenReason(reasons uint32) bool {
	return HasReason(reasons, ReasonListen)
}

// HasLogicalReplicationReason returns true if the reasons bitmask includes a logical-replication session.
func HasLogicalReplicationReason(reasons uint32) bool {
	return HasReason(reasons, ReasonLogicalReplication)
}

// HasSessionAdvisoryLockReason returns true if the reasons bitmask includes a session-level advisory lock.
func HasSessionAdvisoryLockReason(reasons uint32) bool {
	return HasReason(reasons, ReasonSessionAdvisoryLock)
}

// AddReason adds a reason to the bitmask and returns the new value.
func AddReason(reasons uint32, reason uint32) uint32 {
	return reasons | reason
}

// RemoveReason removes a reason from the bitmask and returns the new value.
func RemoveReason(reasons uint32, reason uint32) uint32 {
	return reasons &^ reason
}

// RequiresBegin returns true if the reasons bitmask includes transaction reason.
// This determines if BEGIN should be executed when reserving.
func RequiresBegin(reasons uint32) bool {
	return HasTransactionReason(reasons)
}

// IsEmpty returns true if no reasons are set (connection can be released).
func IsEmpty(reasons uint32) bool {
	return reasons == 0
}

// NewTransactionReservationOptions creates ReservationOptions for a transaction.
func NewTransactionReservationOptions() *querypb.ReservationOptions {
	return &querypb.ReservationOptions{
		Reasons: ReasonTransaction,
	}
}

// NewTempTableReservationOptions creates ReservationOptions for temporary tables.
func NewTempTableReservationOptions() *querypb.ReservationOptions {
	return &querypb.ReservationOptions{
		Reasons: ReasonTempTable,
	}
}

// NewPortalReservationOptions creates ReservationOptions for portal/cursor operations.
func NewPortalReservationOptions() *querypb.ReservationOptions {
	return &querypb.ReservationOptions{
		Reasons: ReasonPortal,
	}
}

// NewReservationOptions creates ReservationOptions with the given reasons bitmask.
func NewReservationOptions(reasons uint32) *querypb.ReservationOptions {
	return &querypb.ReservationOptions{
		Reasons: reasons,
	}
}

// GetReasons extracts the reasons bitmask from ReservationOptions, returning 0 if nil.
func GetReasons(opts *querypb.ReservationOptions) uint32 {
	if opts == nil {
		return 0
	}
	return opts.Reasons
}

// ReasonsString returns a human-readable string of the reasons bitmask.
func ReasonsString(reasons uint32) string {
	if reasons == 0 {
		return "none"
	}
	var parts []string
	if HasTransactionReason(reasons) {
		parts = append(parts, "transaction")
	}
	if HasTempTableReason(reasons) {
		parts = append(parts, "temp_table")
	}
	if HasPortalReason(reasons) {
		parts = append(parts, "portal")
	}
	if HasCopyReason(reasons) {
		parts = append(parts, "copy")
	}
	if HasListenReason(reasons) {
		parts = append(parts, "listen")
	}
	if HasLogicalReplicationReason(reasons) {
		parts = append(parts, "logical_replication")
	}
	if HasSessionAdvisoryLockReason(reasons) {
		parts = append(parts, "session_advisory_lock")
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, "|")
}
