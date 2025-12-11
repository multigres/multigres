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

// Package backup provides shared utilities for backup operations.
package backup

import (
	"time"
)

// TrackingIDFormat is the time format used for backup tracking IDs.
// Format: YYYYMMDD-HHMMSS.microseconds
const TrackingIDFormat = "20060102-150405.000000"

// GenerateTrackingID generates a unique tracking ID for a backup operation.
// This ID is used as an annotation to pgBackRest to identify backups created
// by this system. The format matches the timestamp format used historically
// in multipooler for consistency.
func GenerateTrackingID() string {
	return GenerateTrackingIDAt(time.Now())
}

// GenerateTrackingIDAt generates a tracking ID for a specific timestamp.
// This is useful for testing or when a specific timestamp is required.
func GenerateTrackingIDAt(t time.Time) string {
	return t.Format(TrackingIDFormat)
}
