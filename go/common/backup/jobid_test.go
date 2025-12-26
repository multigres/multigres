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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateJobID(t *testing.T) {
	multipoolerID := "mp-us-east-1"
	jobID := GenerateJobID(multipoolerID)

	// Should contain the multipooler ID
	assert.Contains(t, jobID, multipoolerID)

	// Should be non-empty
	assert.NotEmpty(t, jobID)
}

func TestGenerateJobIDAt(t *testing.T) {
	multipoolerID := "mp-cell-1"
	ts := time.Date(2025, 12, 3, 14, 30, 45, 123456000, time.UTC)

	jobID := GenerateJobIDAt(multipoolerID, ts)

	assert.Equal(t, "20251203-143045.123456_mp-cell-1", jobID)
}

func TestJobIDLexicographicSorting(t *testing.T) {
	multipoolerID := "mp-1"
	earlier := GenerateJobIDAt(multipoolerID, time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	later := GenerateJobIDAt(multipoolerID, time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC))

	// Later job ID should sort after earlier one lexicographically
	assert.True(t, later > earlier, "later job ID should sort after earlier")
}
