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
	"time"
)

// JobIDTimestampFormat is the time format used in job IDs.
// Format: YYYYMMDD-HHMMSS.microseconds (same as TrackingIDFormat for consistency)
const JobIDTimestampFormat = TrackingIDFormat

// JobIDSeparator separates the timestamp from the multipooler ID in job IDs.
const JobIDSeparator = "_"

// GenerateJobID creates a unique job ID that embeds the multipooler ID and
// current timestamp. The format is lexicographically sortable:
// YYYYMMDD-HHMMSS.microseconds_<multipooler_id>
func GenerateJobID(multipoolerID string) string {
	return GenerateJobIDAt(multipoolerID, time.Now())
}

// GenerateJobIDAt creates a job ID for a specific timestamp.
// Useful for testing or when a specific timestamp is required.
func GenerateJobIDAt(multipoolerID string, t time.Time) string {
	return t.Format(JobIDTimestampFormat) + JobIDSeparator + multipoolerID
}
