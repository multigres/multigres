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

package constants

import "time"

const ( // PrimaryShutdownSignalTimeout is how long we wait for multiorch to call
	// EmergencyDemote after we signal STOPPING. If it doesn't arrive in time we
	// stop postgres ourselves so the process always exits cleanly.
	PrimaryShutdownSignalTimeout = 45 * time.Second

	// PrimaryShutdownPollInterval is how often we check whether postgres has stopped
	// while waiting for EmergencyDemote.
	PrimaryShutdownPollInterval = 500 * time.Millisecond
)
