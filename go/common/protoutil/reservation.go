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
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// IsTransactionReason returns true if the reservation reason is for a transaction.
func IsTransactionReason(r multipoolerpb.ReservationReason) bool {
	return r == multipoolerpb.ReservationReason_RESERVATION_REASON_TRANSACTION
}

// IsTempTableReason returns true if the reservation reason is for temporary tables.
func IsTempTableReason(r multipoolerpb.ReservationReason) bool {
	return r == multipoolerpb.ReservationReason_RESERVATION_REASON_TEMP_TABLE
}

// IsPortalReason returns true if the reservation reason is for portal/cursor operations.
func IsPortalReason(r multipoolerpb.ReservationReason) bool {
	return r == multipoolerpb.ReservationReason_RESERVATION_REASON_PORTAL
}

// RequiresBegin returns true if the reservation reason requires executing BEGIN.
func RequiresBegin(r multipoolerpb.ReservationReason) bool {
	return r == multipoolerpb.ReservationReason_RESERVATION_REASON_TRANSACTION
}

// NewTransactionReservationOptions creates ReservationOptions for a transaction.
func NewTransactionReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reason: multipoolerpb.ReservationReason_RESERVATION_REASON_TRANSACTION,
	}
}

// NewTempTableReservationOptions creates ReservationOptions for temporary tables.
func NewTempTableReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reason: multipoolerpb.ReservationReason_RESERVATION_REASON_TEMP_TABLE,
	}
}

// NewPortalReservationOptions creates ReservationOptions for portal/cursor operations.
func NewPortalReservationOptions() *multipoolerpb.ReservationOptions {
	return &multipoolerpb.ReservationOptions{
		Reason: multipoolerpb.ReservationReason_RESERVATION_REASON_PORTAL,
	}
}
