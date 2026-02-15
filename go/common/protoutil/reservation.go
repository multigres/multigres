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
	"strings"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Reason constants as uint32 for bitmask operations.
// These match the ReservationReason enum values.
const (
	ReasonTransaction = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_TRANSACTION) // 1
	ReasonTempTable   = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_TEMP_TABLE)  // 2
	ReasonPortal      = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_PORTAL)      // 4
	ReasonCopy        = uint32(multipoolerpb.ReservationReason_RESERVATION_REASON_COPY)        // 8
)

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
func NewTransactionReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reasons: ReasonTransaction,
	}
}

// NewTempTableReservationOptions creates ReservationOptions for temporary tables.
func NewTempTableReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reasons: ReasonTempTable,
	}
}

// NewPortalReservationOptions creates ReservationOptions for portal/cursor operations.
func NewPortalReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reasons: ReasonPortal,
	}
}

// NewReservationOptions creates ReservationOptions with the given reasons bitmask.
func NewReservationOptions(reasons uint32) *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reasons: reasons,
	}
}

// GetReasons extracts the reasons bitmask from ReservationOptions, returning 0 if nil.
func GetReasons(opts *multipoolerpb.ReservationOptions) uint32 {
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
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, "|")
}
