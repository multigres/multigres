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

import (
	"time"

	"github.com/multigres/multigres/go/common/protoutil"
)

// ReservationProperties tracks why a connection is reserved.
// Reasons is a bitmask of protoutil.Reason* constants, allowing
// a connection to be reserved for multiple concurrent reasons
// (e.g., transaction AND portal).
type ReservationProperties struct {
	// StartTime is when the reservation started.
	StartTime time.Time

	// Reasons is a bitmask of reservation reasons (protoutil.ReasonTransaction,
	// protoutil.ReasonTempTable, protoutil.ReasonPortal).
	Reasons uint32

	// Portals tracks suspended portal names when reserved for portals.
	// Multiple portals can be open on the same connection.
	Portals map[string]struct{}
}

// NewReservationProperties creates new reservation properties with the given reasons bitmask.
func NewReservationProperties(reasons uint32) *ReservationProperties {
	return &ReservationProperties{
		StartTime: time.Now(),
		Reasons:   reasons,
	}
}

// AddReason adds a reason to the reservation bitmask.
func (p *ReservationProperties) AddReason(reason uint32) {
	p.Reasons = protoutil.AddReason(p.Reasons, reason)
}

// RemoveReason removes a reason from the reservation bitmask.
func (p *ReservationProperties) RemoveReason(reason uint32) {
	p.Reasons = protoutil.RemoveReason(p.Reasons, reason)
}

// HasReason returns true if the reservation includes the specified reason.
func (p *ReservationProperties) HasReason(reason uint32) bool {
	return protoutil.HasReason(p.Reasons, reason)
}

// IsEmpty returns true if no reservation reasons remain.
func (p *ReservationProperties) IsEmpty() bool {
	return protoutil.IsEmpty(p.Reasons)
}

// ReasonsString returns a human-readable string of the reasons bitmask.
func (p *ReservationProperties) ReasonsString() string {
	return protoutil.ReasonsString(p.Reasons)
}

// Duration returns how long the connection has been reserved.
func (p *ReservationProperties) Duration() time.Duration {
	return time.Since(p.StartTime)
}

// IsForPortal returns true if the reservation includes a portal reason.
func (p *ReservationProperties) IsForPortal() bool {
	return p.HasReason(protoutil.ReasonPortal)
}

// IsForTransaction returns true if the reservation includes a transaction reason.
func (p *ReservationProperties) IsForTransaction() bool {
	return p.HasReason(protoutil.ReasonTransaction)
}

// AddPortal adds a portal to the reservation.
func (p *ReservationProperties) AddPortal(name string) {
	if p.Portals == nil {
		p.Portals = make(map[string]struct{})
	}
	p.Portals[name] = struct{}{}
}

// RemovePortal removes a portal from the reservation.
// Returns true if the portal was present and removed.
func (p *ReservationProperties) RemovePortal(name string) bool {
	if p.Portals == nil {
		return false
	}
	_, ok := p.Portals[name]
	if ok {
		delete(p.Portals, name)
	}
	return ok
}

// HasPortals returns true if there are any portals in the reservation.
func (p *ReservationProperties) HasPortals() bool {
	return len(p.Portals) > 0
}

// HasPortal returns true if the specified portal is in the reservation.
func (p *ReservationProperties) HasPortal(name string) bool {
	if p.Portals == nil {
		return false
	}
	_, ok := p.Portals[name]
	return ok
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
