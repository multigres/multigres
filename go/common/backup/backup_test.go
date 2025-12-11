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

package backup

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateTrackingID(t *testing.T) {
	id := GenerateTrackingID()

	// Should match format: YYYYMMDD-HHMMSS.microseconds
	pattern := regexp.MustCompile(`^\d{8}-\d{6}\.\d{6}$`)
	assert.True(t, pattern.MatchString(id),
		"Tracking ID should match format YYYYMMDD-HHMMSS.microseconds, got: %s", id)
}

func TestGenerateTrackingID_Uniqueness(t *testing.T) {
	ids := make(map[string]bool)

	// Generate many IDs and check for uniqueness
	for range 100 {
		id := GenerateTrackingID()
		require.False(t, ids[id], "Duplicate tracking ID generated: %s", id)
		ids[id] = true
		time.Sleep(time.Microsecond) // Ensure time advances
	}
}

func TestGenerateTrackingIDAt(t *testing.T) {
	// Test with a specific timestamp
	ts := time.Date(2025, 1, 17, 12, 34, 56, 123456000, time.UTC)
	id := GenerateTrackingIDAt(ts)

	assert.Equal(t, "20250117-123456.123456", id)
}
