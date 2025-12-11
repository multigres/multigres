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

package reserved

import "time"

// ReservationReason indicates why a connection is reserved.
type ReservationReason int

const (
	// ReservationTransaction indicates the connection is reserved for a transaction.
	ReservationTransaction ReservationReason = iota

	// ReservationPortal indicates the connection is reserved for a portal.
	ReservationPortal
)

// String returns a string representation of the reservation reason.
func (r ReservationReason) String() string {
	switch r {
	case ReservationTransaction:
		return "transaction"
	case ReservationPortal:
		return "portal"
	default:
		return "unknown"
	}
}

// ReservationProperties tracks why a connection is reserved.
type ReservationProperties struct {
	// StartTime is when the reservation started.
	StartTime time.Time

	// Reason is why the connection is reserved.
	Reason ReservationReason

	// PortalName is set when reserved for a portal.
	PortalName string
}

// NewReservationProperties creates new reservation properties.
func NewReservationProperties(reason ReservationReason) *ReservationProperties {
	return &ReservationProperties{
		StartTime: time.Now(),
		Reason:    reason,
	}
}

// Duration returns how long the connection has been reserved.
func (p *ReservationProperties) Duration() time.Duration {
	return time.Since(p.StartTime)
}

// IsForPortal returns true if the reservation is for a portal.
func (p *ReservationProperties) IsForPortal() bool {
	return p.Reason == ReservationPortal
}

// IsForTransaction returns true if the reservation is for a transaction.
func (p *ReservationProperties) IsForTransaction() bool {
	return p.Reason == ReservationTransaction
}

// ReleaseReason indicates why a reserved connection is being released.
type ReleaseReason int

const (
	// ReleaseCommit indicates the transaction was committed.
	ReleaseCommit ReleaseReason = iota

	// ReleaseRollback indicates the transaction was rolled back.
	ReleaseRollback

	// ReleasePortalComplete indicates the portal execution completed.
	ReleasePortalComplete

	// ReleaseTimeout indicates the reservation timed out.
	ReleaseTimeout

	// ReleaseKill indicates the connection was killed.
	ReleaseKill

	// ReleaseError indicates an error occurred.
	ReleaseError
)

// String returns a string representation of the release reason.
func (r ReleaseReason) String() string {
	switch r {
	case ReleaseCommit:
		return "commit"
	case ReleaseRollback:
		return "rollback"
	case ReleasePortalComplete:
		return "portal_complete"
	case ReleaseTimeout:
		return "timeout"
	case ReleaseKill:
		return "kill"
	case ReleaseError:
		return "error"
	default:
		return "unknown"
	}
}
