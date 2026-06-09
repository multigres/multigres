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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/protoutil"
)

func TestReleaseReason_String(t *testing.T) {
	tests := []struct {
		reason   ReleaseReason
		expected string
	}{
		{ReleaseCommit, "commit"},
		{ReleaseRollback, "rollback"},
		{ReleasePortalComplete, "portal_complete"},
		{ReleaseTimeout, "timeout"},
		{ReleaseKill, "kill"},
		{ReleaseError, "error"},
		{ReleaseReason(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.reason.String())
		})
	}
}

func TestReservationProperties_New(t *testing.T) {
	t.Run("transaction reservation", func(t *testing.T) {
		before := time.Now()
		props := NewReservationProperties(protoutil.ReasonTransaction)
		after := time.Now()

		assert.True(t, props.IsForTransaction())
		assert.False(t, props.IsForPortal())
		assert.False(t, props.IsEmpty())
		assert.True(t, props.StartTime.After(before) || props.StartTime.Equal(before))
		assert.True(t, props.StartTime.Before(after) || props.StartTime.Equal(after))
		assert.False(t, props.HasPortals())
	})

	t.Run("portal reservation", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonPortal)

		assert.False(t, props.IsForTransaction())
		assert.True(t, props.IsForPortal())
		assert.False(t, props.IsEmpty())
		assert.False(t, props.HasPortals())
	})
}

func TestReservationProperties_MultipleReasons(t *testing.T) {
	t.Run("add multiple reasons", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonTransaction)
		props.AddReason(protoutil.ReasonPortal)

		assert.True(t, props.IsForTransaction())
		assert.True(t, props.IsForPortal())
		assert.False(t, props.IsEmpty())
	})

	t.Run("remove one reason keeps other", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonTransaction | protoutil.ReasonPortal)
		props.RemoveReason(protoutil.ReasonTransaction)

		assert.False(t, props.IsForTransaction())
		assert.True(t, props.IsForPortal())
		assert.False(t, props.IsEmpty())
	})

	t.Run("remove all reasons", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonTransaction | protoutil.ReasonPortal)
		props.RemoveReason(protoutil.ReasonTransaction)
		props.RemoveReason(protoutil.ReasonPortal)

		assert.False(t, props.IsForTransaction())
		assert.False(t, props.IsForPortal())
		assert.True(t, props.IsEmpty())
	})

	t.Run("has reason", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonTransaction | protoutil.ReasonTempTable)

		assert.True(t, props.HasReason(protoutil.ReasonTransaction))
		assert.True(t, props.HasReason(protoutil.ReasonTempTable))
		assert.False(t, props.HasReason(protoutil.ReasonPortal))
	})

	t.Run("reasons string", func(t *testing.T) {
		props := NewReservationProperties(protoutil.ReasonTransaction | protoutil.ReasonPortal)
		s := props.ReasonsString()
		assert.Contains(t, s, "transaction")
		assert.Contains(t, s, "portal")
	})
}

func TestReservationProperties_Duration(t *testing.T) {
	props := NewReservationProperties(protoutil.ReasonTransaction)

	// Sleep briefly to ensure Duration returns a non-zero value.
	time.Sleep(10 * time.Millisecond)

	duration := props.Duration()
	assert.GreaterOrEqual(t, duration, 10*time.Millisecond)
}

func TestReservationProperties_PortalManagement(t *testing.T) {
	props := NewReservationProperties(protoutil.ReasonPortal)

	t.Run("initial state", func(t *testing.T) {
		assert.False(t, props.HasPortals())
		assert.False(t, props.HasPortal("p1"))
	})

	t.Run("add portal", func(t *testing.T) {
		props.AddPortal("p1")

		assert.True(t, props.HasPortals())
		assert.True(t, props.HasPortal("p1"))
		assert.False(t, props.HasPortal("p2"))
	})

	t.Run("add multiple portals", func(t *testing.T) {
		props.AddPortal("p2")
		props.AddPortal("p3")

		assert.True(t, props.HasPortal("p1"))
		assert.True(t, props.HasPortal("p2"))
		assert.True(t, props.HasPortal("p3"))
	})

	t.Run("remove portal", func(t *testing.T) {
		removed := props.RemovePortal("p2")
		assert.True(t, removed)

		assert.True(t, props.HasPortal("p1"))
		assert.False(t, props.HasPortal("p2"))
		assert.True(t, props.HasPortal("p3"))
	})

	t.Run("remove non-existent portal", func(t *testing.T) {
		removed := props.RemovePortal("nonexistent")
		assert.False(t, removed)
	})

	t.Run("remove last portals", func(t *testing.T) {
		props.RemovePortal("p1")
		props.RemovePortal("p3")

		assert.False(t, props.HasPortals())
	})
}

func TestReservationProperties_HasPortal_NilMap(t *testing.T) {
	props := &ReservationProperties{
		Reasons: protoutil.ReasonPortal,
		Portals: nil,
	}

	assert.False(t, props.HasPortal("any"))
}

func TestReservationProperties_RemovePortal_NilMap(t *testing.T) {
	props := &ReservationProperties{
		Reasons: protoutil.ReasonPortal,
		Portals: nil,
	}

	removed := props.RemovePortal("any")
	assert.False(t, removed)
}

func TestReservationProperties_AddPortal_InitializesMap(t *testing.T) {
	props := &ReservationProperties{
		Reasons: protoutil.ReasonPortal,
		Portals: nil,
	}

	props.AddPortal("p1")

	require.NotNil(t, props.Portals)
	assert.True(t, props.HasPortal("p1"))
}
