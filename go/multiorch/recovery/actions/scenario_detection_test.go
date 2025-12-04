// Copyright 2025 Supabase, Inc.
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

package actions

import (
	"testing"

	"github.com/stretchr/testify/assert"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestDetermineScenario(t *testing.T) {
	tests := []struct {
		name     string
		statuses []*multipoolermanagerdatapb.Status
		expected InitializationScenario
	}{
		{
			name: "all empty - bootstrap",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: false},
				{IsInitialized: false},
				{IsInitialized: false},
			},
			expected: ScenarioBootstrap,
		},
		{
			name: "all initialized - reelect",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: true},
				{IsInitialized: true},
				{IsInitialized: true},
			},
			expected: ScenarioReelect,
		},
		{
			name: "mixed initialized and empty - repair",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: true},
				{IsInitialized: false},
				{IsInitialized: true},
			},
			expected: ScenarioRepair,
		},
		{
			name: "one empty node - bootstrap",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: false},
			},
			expected: ScenarioBootstrap,
		},
		{
			name: "all unavailable - unknown",
			statuses: []*multipoolermanagerdatapb.Status{
				nil,
				nil,
				nil,
			},
			expected: ScenarioUnknown,
		},
		{
			name: "some unavailable with initialized - reelect",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: true},
				nil,
				{IsInitialized: true},
			},
			expected: ScenarioReelect,
		},
		{
			name: "some unavailable with empty - bootstrap",
			statuses: []*multipoolermanagerdatapb.Status{
				{IsInitialized: false},
				nil,
				{IsInitialized: false},
			},
			expected: ScenarioBootstrap,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scenario := DetermineScenario(tt.statuses, nil)
			assert.Equal(t, tt.expected, scenario, "unexpected scenario")
		})
	}
}

func TestInitializationScenarioString(t *testing.T) {
	tests := []struct {
		scenario InitializationScenario
		expected string
	}{
		{ScenarioBootstrap, "Bootstrap"},
		{ScenarioRepair, "Repair"},
		{ScenarioReelect, "Reelect"},
		{ScenarioUnknown, "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.scenario.String())
		})
	}
}
